// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/partitioned-hash-join-node.inline.h"

#include "codegen/impala-ir.h"
#include "exec/hash-table.inline.h"
#include "runtime/row-batch.h"
#include <sstream>

#include "common/names.h"

using namespace impala;

// Wrapper around ExecNode's eval conjuncts with a different function name.
// This lets us distinguish between the join conjuncts vs. non-join conjuncts
// for codegen.
// Note: don't declare this static.  LLVM will pick the fastcc calling convention and
// we will not be able to replace the functions with codegen'd versions.
// TODO: explicitly set the calling convention?
// TODO: investigate using fastcc for all codegen internal functions?
bool IR_NO_INLINE EvalOtherJoinConjuncts(
    ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
  return ExecNode::EvalConjuncts(ctxs, num_ctxs, row);
}

// CreateOutputRow, EvalOtherJoinConjuncts, and EvalConjuncts are replaced by
// codegen.
template<int const JoinOp>
int PartitionedHashJoinNode::ProcessProbeBatch(
    RowBatch* out_batch, HashTableCtx* ht_ctx, Status* status) {
  ExprContext* const* other_join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  const int num_other_join_conjuncts = other_join_conjunct_ctxs_.size();
  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  const int num_conjuncts = conjunct_ctxs_.size();

  DCHECK(!out_batch->AtCapacity());
  DCHECK_GE(probe_batch_pos_, 0);
  TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
  const int max_rows = out_batch->capacity() - out_batch->num_rows();
  int num_rows_added = 0;

  while (true) {
    if (current_probe_row_ != NULL) {
      while (!hash_tbl_iterator_.AtEnd()) {
        TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
        DCHECK(matched_build_row != NULL);

        if ((JoinOp == TJoinOp::RIGHT_SEMI_JOIN || JoinOp == TJoinOp::RIGHT_ANTI_JOIN) &&
            hash_tbl_iterator_.IsMatched()) {
          hash_tbl_iterator_.NextDuplicate();
          continue;
        }

        if (JoinOp == TJoinOp::LEFT_ANTI_JOIN || JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
            JoinOp == TJoinOp::RIGHT_ANTI_JOIN || JoinOp == TJoinOp::RIGHT_SEMI_JOIN ||
            JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
          // Evaluate the non-equi-join conjuncts against a temp row assembled from all
          // build and probe tuples.
          if (num_other_join_conjuncts > 0) {
            CreateOutputRow(semi_join_staging_row_, current_probe_row_,
                matched_build_row);
            if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs,
                     num_other_join_conjuncts, semi_join_staging_row_)) {
              hash_tbl_iterator_.NextDuplicate();
              continue;
            }
          }

          // Create output row assembled from build xor probe tuples.
          if (JoinOp == TJoinOp::LEFT_ANTI_JOIN || JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
              JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            out_batch->CopyRow(current_probe_row_, out_row);
          } else {
            out_batch->CopyRow(matched_build_row, out_row);
          }
        } else {
          // Not a semi join; create an output row with all probe/build tuples and
          // evaluate the non-equi-join conjuncts.
          CreateOutputRow(out_row, current_probe_row_, matched_build_row);
          if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs, num_other_join_conjuncts,
                   out_row)) {
            hash_tbl_iterator_.NextDuplicate();
            continue;
          }
        }

        // At this point the probe is considered matched.
        matched_probe_ = true;
        if (JoinOp == TJoinOp::LEFT_ANTI_JOIN ||
            JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
          // We can safely ignore this probe row for left anti joins.
          hash_tbl_iterator_.SetAtEnd();
          goto next_row;
        }

        // Update hash_tbl_iterator.
        if (JoinOp == TJoinOp::LEFT_SEMI_JOIN) {
          hash_tbl_iterator_.SetAtEnd();
        } else {
          if (JoinOp == TJoinOp::RIGHT_OUTER_JOIN || JoinOp == TJoinOp::RIGHT_ANTI_JOIN ||
              JoinOp == TJoinOp::FULL_OUTER_JOIN || JoinOp == TJoinOp::RIGHT_SEMI_JOIN) {
            // There is a match for this build row. Mark the Bucket or the DuplicateNode
            // as matched for right/full joins.
            hash_tbl_iterator_.SetMatched();
          }
          hash_tbl_iterator_.NextDuplicate();
        }

        if ((JoinOp != TJoinOp::RIGHT_ANTI_JOIN) &&
            ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
          ++num_rows_added;
          out_row = out_row->next_row(out_batch);
          if (num_rows_added == max_rows) goto end;
        }
      }

      if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && !matched_probe_) {
        // Null aware behavior. The probe row did not match in the hash table so we
        // should interpret the hash table probe as "unknown" if there are nulls on the
        // build size. For those rows, we need to process the remaining join
        // predicates later.
        if (null_aware_partition_->build_rows()->num_rows() != 0) {
          if (num_other_join_conjuncts == 0) goto next_row;
          if (UNLIKELY(!AppendRow(null_aware_partition_->probe_rows(),
                                  current_probe_row_, status))) {
            return -1;
          }
          goto next_row;
        }
      }

      if ((JoinOp == TJoinOp::LEFT_OUTER_JOIN || JoinOp == TJoinOp::FULL_OUTER_JOIN) &&
          !matched_probe_) {
        // No match for this row, we need to output it.
        CreateOutputRow(out_row, current_probe_row_, NULL);
        if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
          ++num_rows_added;
          matched_probe_ = true;
          out_row = out_row->next_row(out_batch);
          if (num_rows_added == max_rows) goto end;
        }
      }
      if ((JoinOp == TJoinOp::LEFT_ANTI_JOIN ||
          JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) &&
          !matched_probe_) {
        // No match for this current_probe_row_, we need to output it. No need to
        // evaluate the conjunct_ctxs since semi joins cannot have any.
        out_batch->CopyRow(current_probe_row_, out_row);
        ++num_rows_added;
        matched_probe_ = true;
        out_row = out_row->next_row(out_batch);
        if (num_rows_added == max_rows) goto end;
      }
    }

next_row:
    // Must have reached the end of the hash table iterator for the current row before
    // moving to the row.
    DCHECK(hash_tbl_iterator_.AtEnd());

    if (UNLIKELY(probe_batch_pos_ == probe_batch_->num_rows())) {
      // Finished this batch.
      current_probe_row_ = NULL;
      goto end;
    }

    // Establish current_probe_row_ and find its corresponding partition.
    current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
    matched_probe_ = false;
    uint32_t hash;
    if (!ht_ctx->EvalAndHashProbe(current_probe_row_, &hash)) {
      if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        // For NAAJ, we need to treat NULLs on the probe carefully. The logic is:
        // 1. No build rows -> Return this row.
        // 2. Has build rows & no other join predicates, skip row.
        // 3. Has build rows & other join predicates, we need to evaluate against all
        // build rows. First evaluate it against this partition, and if there is not
        // a match, save it to evaluate against other partitions later. If there
        // is a match, the row is skipped.
        if (!non_empty_build_) continue;
        if (num_other_join_conjuncts == 0) goto next_row;
        if (UNLIKELY(!AppendRow(null_probe_rows_, current_probe_row_, status))) {
          return -1;
        }
        matched_null_probe_.push_back(false);
        goto next_row;
      }
      continue;
    }
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    if (LIKELY(hash_tbls_[partition_idx] != NULL)) {
      hash_tbl_iterator_= hash_tbls_[partition_idx]->FindProbeRow(ht_ctx, hash);
    } else {
      Partition* partition = hash_partitions_[partition_idx];
      if (UNLIKELY(partition->is_closed())) {
        // This partition is closed, meaning the build side for this partition was empty.
        DCHECK(state_ == PROCESSING_PROBE || state_ == REPARTITIONING);
      } else {
        // This partition is not in memory, spill the probe row and move to the next row.
        DCHECK(partition->is_spilled());
        DCHECK(partition->probe_rows() != NULL);
        if (UNLIKELY(!AppendRow(partition->probe_rows(), current_probe_row_, status))) {
          return -1;
        }
        goto next_row;
      }
    }
  }

end:
  DCHECK_LE(num_rows_added, max_rows);
  return num_rows_added;
}

// CreateOutputRow, EvalOtherJoinConjuncts, and EvalConjuncts are replaced by
// codegen.
template<int const JoinOp>
int PartitionedHashJoinNode::ProcessProbeBatchBase(
    RowBatch* out_batch, HashTableCtx* ht_ctx, Status* status) {
  ExprContext* const* other_join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  const int num_other_join_conjuncts = other_join_conjunct_ctxs_.size();
  ExprContext* const* conjunct_ctxs = &conjunct_ctxs_[0];
  const int num_conjuncts = conjunct_ctxs_.size();

  DCHECK(!out_batch->AtCapacity());
  DCHECK_GE(probe_batch_pos_, 0);
  TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
  const int max_rows = out_batch->capacity() - out_batch->num_rows();
  int num_rows_added = 0;

  while (true) {
    if (current_probe_row_ != NULL) {
      while (!hash_tbl_iterator_.AtEnd()) {
        TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
        DCHECK(matched_build_row != NULL);

        if ((JoinOp == TJoinOp::RIGHT_SEMI_JOIN || JoinOp == TJoinOp::RIGHT_ANTI_JOIN) &&
            hash_tbl_iterator_.IsMatched()) {
          hash_tbl_iterator_.NextDuplicate();
          continue;
        }

        if (JoinOp == TJoinOp::LEFT_ANTI_JOIN || JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
            JoinOp == TJoinOp::RIGHT_ANTI_JOIN || JoinOp == TJoinOp::RIGHT_SEMI_JOIN ||
            JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
          // Evaluate the non-equi-join conjuncts against a temp row assembled from all
          // build and probe tuples.
          if (num_other_join_conjuncts > 0) {
            CreateOutputRow(semi_join_staging_row_, current_probe_row_,
                matched_build_row);
            if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs,
                     num_other_join_conjuncts, semi_join_staging_row_)) {
              hash_tbl_iterator_.NextDuplicate();
              continue;
            }
          }

          // Create output row assembled from build xor probe tuples.
          if (JoinOp == TJoinOp::LEFT_ANTI_JOIN || JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
              JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            out_batch->CopyRow(current_probe_row_, out_row);
          } else {
            out_batch->CopyRow(matched_build_row, out_row);
          }
        } else {
          // Not a semi join; create an output row with all probe/build tuples and
          // evaluate the non-equi-join conjuncts.
          CreateOutputRow(out_row, current_probe_row_, matched_build_row);
          if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs, num_other_join_conjuncts,
                   out_row)) {
            hash_tbl_iterator_.NextDuplicate();
            continue;
          }
        }

        // At this point the probe is considered matched.
        matched_probe_ = true;
        if (JoinOp == TJoinOp::LEFT_ANTI_JOIN ||
            JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
          // We can safely ignore this probe row for left anti joins.
          hash_tbl_iterator_.SetAtEnd();
          goto next_row;
        }

        // Update hash_tbl_iterator.
        if (JoinOp == TJoinOp::LEFT_SEMI_JOIN) {
          hash_tbl_iterator_.SetAtEnd();
        } else {
          if (JoinOp == TJoinOp::RIGHT_OUTER_JOIN || JoinOp == TJoinOp::RIGHT_ANTI_JOIN ||
              JoinOp == TJoinOp::FULL_OUTER_JOIN || JoinOp == TJoinOp::RIGHT_SEMI_JOIN) {
            // There is a match for this build row. Mark the Bucket or the DuplicateNode
            // as matched for right/full joins.
            hash_tbl_iterator_.SetMatched();
          }
          hash_tbl_iterator_.NextDuplicate();
        }

        if ((JoinOp != TJoinOp::RIGHT_ANTI_JOIN) &&
            ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
          ++num_rows_added;
          out_row = out_row->next_row(out_batch);
          if (num_rows_added == max_rows) goto end;
        }
      }

      if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && !matched_probe_) {
        // Null aware behavior. The probe row did not match in the hash table so we
        // should interpret the hash table probe as "unknown" if there are nulls on the
        // build size. For those rows, we need to process the remaining join
        // predicates later.
        if (null_aware_partition_->build_rows()->num_rows() != 0) {
          if (num_other_join_conjuncts == 0) goto next_row;
          if (UNLIKELY(!AppendRow(null_aware_partition_->probe_rows(),
                                  current_probe_row_, status))) {
            return -1;
          }
          goto next_row;
        }
      }

      if ((JoinOp == TJoinOp::LEFT_OUTER_JOIN || JoinOp == TJoinOp::FULL_OUTER_JOIN) &&
          !matched_probe_) {
        // No match for this row, we need to output it.
        CreateOutputRow(out_row, current_probe_row_, NULL);
        if (ExecNode::EvalConjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
          ++num_rows_added;
          matched_probe_ = true;
          out_row = out_row->next_row(out_batch);
          if (num_rows_added == max_rows) goto end;
        }
      }
      if ((JoinOp == TJoinOp::LEFT_ANTI_JOIN ||
          JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) &&
          !matched_probe_) {
        // No match for this current_probe_row_, we need to output it. No need to
        // evaluate the conjunct_ctxs since semi joins cannot have any.
        out_batch->CopyRow(current_probe_row_, out_row);
        ++num_rows_added;
        matched_probe_ = true;
        out_row = out_row->next_row(out_batch);
        if (num_rows_added == max_rows) goto end;
      }
    }

next_row:
    // Must have reached the end of the hash table iterator for the current row before
    // moving to the row.
    DCHECK(hash_tbl_iterator_.AtEnd());

    if (UNLIKELY(probe_batch_pos_ == probe_batch_->num_rows())) {
      // Finished this batch.
      current_probe_row_ = NULL;
      goto end;
    }

    // Establish current_probe_row_ and find its corresponding partition.

    current_probe_row_ = probe_batch_->GetRow(probe_batch_pos_++);
    matched_probe_ = false;

    uint32_t hash;
    int32_t probe_value = ht_ctx->GetIntCol(current_probe_row_);
    ht_ctx->HashQuickInt(probe_value, &hash) ;
    if (!true) {
      if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        // For NAAJ, we need to treat NULLs on the probe carefully. The logic is:
        // 1. No build rows -> Return this row.
        // 2. Has build rows & no other join predicates, skip row.
        // 3. Has build rows & other join predicates, we need to evaluate against all
        // build rows. First evaluate it against this partition, and if there is not
        // a match, save it to evaluate against other partitions later. If there
        // is a match, the row is skipped.
        if (!non_empty_build_) continue;
        if (num_other_join_conjuncts == 0) goto next_row;
        if (UNLIKELY(!AppendRow(null_probe_rows_, current_probe_row_, status))) {
          return -1;
        }
        matched_null_probe_.push_back(false);
        goto next_row;
      }
      continue;
    }
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    if (LIKELY(hash_tbls_[partition_idx] != NULL)) {
      hash_tbl_iterator_= hash_tbls_[partition_idx]->FindProbeRow(ht_ctx, hash, probe_value);
    } else {
      Partition* partition = hash_partitions_[partition_idx];
      if (UNLIKELY(partition->is_closed())) {
        // This partition is closed, meaning the build side for this partition was empty.
        DCHECK(state_ == PROCESSING_PROBE || state_ == REPARTITIONING);
      } else {
        // This partition is not in memory, spill the probe row and move to the next row.
        DCHECK(partition->is_spilled());
        DCHECK(partition->probe_rows() != NULL);
        if (UNLIKELY(!AppendRow(partition->probe_rows(), current_probe_row_, status))) {
          return -1;
        }
        goto next_row;
      }
    }
  }

end:
  DCHECK_LE(num_rows_added, max_rows);
  return num_rows_added;
}


template<int const JoinOp>
int PartitionedHashJoinNode::ProcessProbeBatchBucketed(
	RowBatch* out_batch, HashTableCtx* ht_ctx, Status* status) {

  vector<ProbeTuple> batched_probe_rows[PARTITION_FANOUT];

  DCHECK(!out_batch->AtCapacity());
  TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
  const int max_rows = out_batch->capacity() - out_batch->num_rows();
  int num_probe_rows = std::min((probe_batch_->num_rows() - probe_batch_pos_),max_rows);
  int num_rows_added = 0;

  if (UNLIKELY(probe_batch_pos_ >= probe_batch_->num_rows())) {
	// Finished this batch.
	current_probe_row_ = NULL;
	return num_rows_added;
  }

  if (probe_batch_pos_ >= 0) {
	HashTable::Iterator *hash_table_iterators = new HashTable::Iterator[num_probe_rows];

	for (int i =0; i < num_probe_rows; i++) {
	  uint32_t hash_value;
	  int32_t probe_value = ht_ctx->GetIntCol(probe_batch_->GetRow(i));
	  ht_ctx->HashQuickInt(probe_value, &hash_value);
	  const uint32_t partition_idx = hash_value >> (32 - NUM_PARTITIONING_BITS);
	  //hash_partitions_probe_count[partition_idx]++;
	  ProbeTuple tuple;
	  tuple.hash_value = hash_value;
	  tuple.probe_value = probe_value;
	  uint32_t partition_id =  hash_value >> (32 - NUM_PARTITIONING_BITS);
	  hash_tbls_[partition_id]->Prefetch(hash_value);
	  //tuple.row = probe_batch_->GetRow(i);
	  batched_probe_rows[partition_idx].push_back(tuple);
	}

	int probe_count = 0;
	for (int i = 0; i < PARTITION_FANOUT; ++i) {
	  int partition_row_count = batched_probe_rows[i].size();
	  for (int j = 0; j < partition_row_count; j++) {
		uint32 hash_value = batched_probe_rows[i].at(j).hash_value;
		uint32_t partition_id =  hash_value >> (32 - NUM_PARTITIONING_BITS);
		hash_tbls_[partition_id]->Prefetch(hash_value);
	  }
	}

	probe_count = 0;
	for (int i = 0; i < PARTITION_FANOUT; ++i) {
	  int partition_row_count = batched_probe_rows[i].size();
	  for (int j = 0; j < partition_row_count; j++) {
		uint32 hash_value = batched_probe_rows[i].at(j).hash_value;
		uint32_t partition_id =  hash_value >> (32 - NUM_PARTITIONING_BITS);
		hash_tbls_[partition_id]->PrefetchBucketData(hash_value);
	  }
	}

	probe_count = 0;
	for (int i = 0; i < PARTITION_FANOUT; ++i) {
	  int partition_row_count = batched_probe_rows[i].size();
	  for (int j = 0; j < partition_row_count; j++) {
		uint32 hash_value = batched_probe_rows[i].at(j).hash_value;
		int32_t probe_value = batched_probe_rows[i].at(j).probe_value;
		uint32 target_partition = hash_value  >> (32 - NUM_PARTITIONING_BITS);
		DCHECK(i == target_partition);
		hash_table_iterators[probe_count] = hash_tbls_[i]->FindProbeRow(ht_ctx,
			hash_value,
			probe_value);

		probe_count++;
	  }
	}

	for (int i =0; i < num_probe_rows; i++) {
	  while (!hash_table_iterators[i].AtEnd()) {
		TupleRow* matched_build_row = hash_table_iterators[i].GetRow();
		DCHECK(matched_build_row != NULL);

		// Not a semi join; create an output row with all probe/build tuples and
		// evaluate the non-equi-join conjuncts.
		CreateOutputRow(out_row, probe_batch_->GetRow(i), matched_build_row);
		// At this point the probe is considered matched.
		hash_table_iterators[i].NextDuplicate();
		++num_rows_added;
		out_row = out_row->next_row(out_batch);
	  }
	}

	delete hash_table_iterators;
	probe_batch_pos_ += num_probe_rows;
  }

  DCHECK_LE(num_rows_added, max_rows);
  return num_rows_added;

}

// CreateOutputRow, EvalOtherJoinConjuncts, and EvalConjuncts are replaced by
// codegen.
template<bool const prefetch>
int PartitionedHashJoinNode::ProcessProbeBatchFast(
		RowBatch* out_batch, HashTableCtx* ht_ctx, Status* status) {
  DCHECK(!out_batch->AtCapacity());
  TupleRow* out_row = out_batch->GetRow(out_batch->AddRow());
  int const max_rows = out_batch->capacity() - out_batch->num_rows();
  int rows_to_add = max_rows;
  int const probe_limit = probe_batch_->num_rows();
  int num_probe_rows = std::min((probe_limit - probe_batch_pos_),max_rows);
  int num_rows_added = 0;

  if (UNLIKELY(probe_batch_pos_ >= probe_limit)) {
	// Finished this batch.
	current_probe_row_ = NULL;
	return num_rows_added;
  }

  HashTable::Iterator *hash_table_iterators = new HashTable::Iterator[num_probe_rows];
  int32_t *probe_values = new int[num_probe_rows];
  uint32_t *hash_values = new uint32_t[num_probe_rows];
  uint32_t *partition_ids = new uint32_t[num_probe_rows];

  while ((probe_batch_pos_ < probe_limit)) {
	if (probe_batch_pos_ >= 0) {
	  rows_to_add = max_rows - num_rows_added;
	  num_probe_rows = std::min((probe_limit - probe_batch_pos_), rows_to_add);
	  int unrolled_num_probe_rows = num_probe_rows & -4;

	  int i =0;
	  for (; i < unrolled_num_probe_rows; i+=4) {
		probe_values[i] = ht_ctx->GetIntCol(probe_batch_->GetRow(i));
		probe_values[i+1] = ht_ctx->GetIntCol(probe_batch_->GetRow(i+1));
		probe_values[i+2] = ht_ctx->GetIntCol(probe_batch_->GetRow(i+2));
		probe_values[i+3] = ht_ctx->GetIntCol(probe_batch_->GetRow(i+3));

		ht_ctx->HashQuickInt(probe_values[i], &hash_values[i]);
		ht_ctx->HashQuickInt(probe_values[i+1], &hash_values[i+1]);
		ht_ctx->HashQuickInt(probe_values[i+2], &hash_values[i+2]);
		ht_ctx->HashQuickInt(probe_values[i+3], &hash_values[i+3]);

		partition_ids[i] = hash_values[i] >> (32 - NUM_PARTITIONING_BITS);
		partition_ids[i+1] = hash_values[i+1] >> (32 - NUM_PARTITIONING_BITS);
		partition_ids[i+2] = hash_values[i+2] >> (32 - NUM_PARTITIONING_BITS);
		partition_ids[i+3] = hash_values[i+3] >> (32 - NUM_PARTITIONING_BITS);

		// Prefetch the buckets
		if(prefetch) {
		  hash_tbls_[partition_ids[i]]->Prefetch(hash_values[i]);
		  hash_tbls_[partition_ids[i+1]]->Prefetch(hash_values[i+1]);
		  hash_tbls_[partition_ids[i+2]]->Prefetch(hash_values[i+2]);
		  hash_tbls_[partition_ids[i+3]]->Prefetch(hash_values[i+3]);
		}
	  }

	  for (; i < num_probe_rows; i++) {
		probe_values[i] = ht_ctx->GetIntCol(probe_batch_->GetRow(i));
		ht_ctx->HashQuickInt(probe_values[i], &hash_values[i]);
		partition_ids[i] = hash_values[i] >> (32 - NUM_PARTITIONING_BITS);
		if(prefetch) {
		  hash_tbls_[partition_ids[i]]->Prefetch(hash_values[i]);
		}
	  }

	  // Prefetch the bucket data
	  if(prefetch) {
		int i =0;
		for (; i < unrolled_num_probe_rows; i+=4) {
		  hash_tbls_[partition_ids[i]]->PrefetchBucketData(hash_values[i]);
		  hash_tbls_[partition_ids[i+1]]->PrefetchBucketData(hash_values[i+1]);
		  hash_tbls_[partition_ids[i+2]]->PrefetchBucketData(hash_values[i+2]);
		  hash_tbls_[partition_ids[i+3]]->PrefetchBucketData(hash_values[i+3]);
		}
		for (; i < num_probe_rows; i++) {
		  hash_tbls_[partition_ids[i]]->PrefetchBucketData(hash_values[i]);
		}
	  }

	  i =0;
	  for (; i < unrolled_num_probe_rows; i+=4) {
		hash_table_iterators[i] = hash_tbls_[partition_ids[i]]->FindProbeRow(
			ht_ctx, hash_values[i], probe_values[i]);

		hash_table_iterators[i+1] = hash_tbls_[partition_ids[i+1]]->FindProbeRow(
			ht_ctx, hash_values[i+1], probe_values[i+1]);

		hash_table_iterators[i+2] = hash_tbls_[partition_ids[i+2]]->FindProbeRow(
			ht_ctx, hash_values[i+2], probe_values[i+2]);

		hash_table_iterators[i+3] = hash_tbls_[partition_ids[i+3]]->FindProbeRow(
			ht_ctx, hash_values[i+3], probe_values[i+3]);
		probe_batch_pos_+=4;
	  }

	  for (; i < num_probe_rows; i++) {
		hash_table_iterators[i] = hash_tbls_[partition_ids[i]]->FindProbeRow(
			ht_ctx, hash_values[i], probe_values[i]);
		probe_batch_pos_++;
	  }

	  i =0;
	  for (; i < unrolled_num_probe_rows; i+=4) {
	    int num_rows_added_1 = 0;
	    int num_rows_added_2 = 0;
	    int num_rows_added_3 = 0;
	    int num_rows_added_4 = 0;

	    while (!hash_table_iterators[i].AtEnd()) {
	      TupleRow* matched_build_row = hash_table_iterators[i].GetRow();
	      DCHECK(matched_build_row != NULL);
	      CreateOutputRow(out_row, probe_batch_->GetRow(i), matched_build_row);
	      hash_table_iterators[i].NextDuplicate();
	      ++num_rows_added_1;
	      out_row = out_row->next_row(out_batch);
	    }

	    while (!hash_table_iterators[i+1].AtEnd()) {
	      TupleRow* matched_build_row = hash_table_iterators[i+1].GetRow();
	      DCHECK(matched_build_row != NULL);
	      CreateOutputRow(out_row, probe_batch_->GetRow(i+1), matched_build_row);
	      hash_table_iterators[i+1].NextDuplicate();
	      ++num_rows_added_2;
	      out_row = out_row->next_row(out_batch);
	    }

	    while (!hash_table_iterators[i+2].AtEnd()) {
	      TupleRow* matched_build_row = hash_table_iterators[i+2].GetRow();
	      DCHECK(matched_build_row != NULL);
	      CreateOutputRow(out_row, probe_batch_->GetRow(i+2), matched_build_row);
	      hash_table_iterators[i+2].NextDuplicate();
	      ++num_rows_added_3;
	      out_row = out_row->next_row(out_batch);
	    }

	    while (!hash_table_iterators[i+3].AtEnd()) {
	      TupleRow* matched_build_row = hash_table_iterators[i+3].GetRow();
	      DCHECK(matched_build_row != NULL);
	      CreateOutputRow(out_row, probe_batch_->GetRow(i+3), matched_build_row);
	      hash_table_iterators[i+3].NextDuplicate();
	      ++num_rows_added_4;
	      out_row = out_row->next_row(out_batch);
	    }
        num_rows_added += num_rows_added_1 + num_rows_added_2 + num_rows_added_3 + num_rows_added_4;
	  }

	  for (; i < num_probe_rows; i++) {
	    while (!hash_table_iterators[i].AtEnd()) {
	      TupleRow* matched_build_row = hash_table_iterators[i].GetRow();
	      DCHECK(matched_build_row != NULL);
	      CreateOutputRow(out_row, probe_batch_->GetRow(i), matched_build_row);
	      hash_table_iterators[i].NextDuplicate();
	      ++num_rows_added;
	      out_row = out_row->next_row(out_batch);
	    }
	  }

	  if(num_rows_added >= max_rows)
		break;
	}

	if ((probe_batch_pos_ >= probe_limit) || (num_rows_added >= max_rows))
	  break;
  }
  delete hash_table_iterators;
  delete probe_values;
  delete hash_values;
  delete partition_ids;

  DCHECK_LE(num_rows_added, max_rows);
  return num_rows_added;

}
int PartitionedHashJoinNode::ProcessProbeBatch(
    const TJoinOp::type join_op, RowBatch* out_batch, HashTableCtx* ht_ctx,
    Status* status) {
 switch (join_op) {
    case TJoinOp::INNER_JOIN:
        if (use_batched_join == 0) {
            return ProcessProbeBatchBase<TJoinOp::INNER_JOIN>(out_batch, ht_ctx, status);
        }
        else if (use_batched_join == 1) {
          if (enable_join_prefetch) {
            return ProcessProbeBatchFast<true>(out_batch, ht_ctx, status);
          } else {
             return ProcessProbeBatchFast<false>(out_batch, ht_ctx, status);
          }
        } else if (use_batched_join == 2) {
                return ProcessProbeBatchBucketed<TJoinOp::INNER_JOIN>(out_batch, ht_ctx, status);
        } else {
                return ProcessProbeBatch<TJoinOp::INNER_JOIN>(out_batch, ht_ctx, status);
        }
    case TJoinOp::LEFT_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_OUTER_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::LEFT_SEMI_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_SEMI_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::LEFT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_ANTI_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>(out_batch, ht_ctx,
          status);
    case TJoinOp::RIGHT_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_OUTER_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::RIGHT_SEMI_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_SEMI_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::RIGHT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_ANTI_JOIN>(out_batch, ht_ctx, status);
    case TJoinOp::FULL_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::FULL_OUTER_JOIN>(out_batch, ht_ctx, status);
    default:
      DCHECK(false) << "Unknown join type";
      return -1;
  }
}

Status PartitionedHashJoinNode::ProcessBuildBatch(RowBatch* build_batch) {
  for (int i = 0; i < build_batch->num_rows(); ++i) {
    DCHECK(buildStatus_.ok());
    TupleRow* build_row = build_batch->GetRow(i);
    uint32_t hash;
    if (!ht_ctx_->EvalAndHashBuild(build_row, &hash)) {
      if (null_aware_partition_ != NULL) {
        // TODO: remove with codegen/template
        // If we are NULL aware and this build row has NULL in the eq join slot,
        // append it to the null_aware partition. We will need it later.
        if (UNLIKELY(!AppendRow(null_aware_partition_->build_rows(),
                                build_row, &buildStatus_))) {
          return buildStatus_;
        }
      }
      continue;
    }
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    Partition* partition = hash_partitions_[partition_idx];
    const bool result = AppendRow(partition->build_rows(), build_row, &buildStatus_);
    if (UNLIKELY(!result)) return buildStatus_;
  }
  return Status::OK();
}
