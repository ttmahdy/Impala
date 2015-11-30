// Copyright 2016 Cloudera Inc.
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


#ifndef IMPALA_RUNTIME_RUNTIME_FILTER_H
#define IMPALA_RUNTIME_RUNTIME_FILTER_H

#include "runtime/raw-value.h"
#include "runtime/types.h"
#include "util/bitmap.h"
#include "util/spinlock.h"

#include <boost/thread.hpp>

namespace impala {

/// RuntimeFilters represent predicates that are computed during query execution (rather
/// than during planning). They can then be sent to other operators to improve their
/// selectivity. For example, a RuntimeFilter might compute a predicate corresponding to
/// set membership, where the members of that set can only be computed at runtime (for
/// example, the distinct values of the build side of a hash table). Other plan nodes can
/// use that predicate by testing for membership of that set to filter rows early on in
/// the plan tree (e.g. the scan that feeds the probe side of that join node could
/// eliminate rows from consideration for join matching).
class RuntimeFilter {
 public:
  RuntimeFilter(const TRuntimeFilter& filter) : bitmap_(NULL), filter_desc_(filter) { }

  /// Returns NULL if no calls to UpdateBitmap() have been made yet.
  const Bitmap* GetBitmap() const { return bitmap_; }

  const TRuntimeFilter& filter_desc() const { return filter_desc_; }

  /// Sets the internal filter bitmap to 'bitmap', and in doing so acquires the memory
  /// associated with 'bitmap'. Can only legally be called once per filter.
  void UpdateBitmap(Bitmap* bitmap) {
    DCHECK(bitmap_ == NULL);
    // TODO: Barrier required here to ensure compiler does not both inline and re-order
    // this assignment. Not an issue for correctness (as assignment is atomic), but
    // potentially confusing.
    bitmap_ = bitmap;
  }

  ~RuntimeFilter() { if (bitmap_ != NULL) delete bitmap_; }

  /// Returns false iff the bitmap filter has been set via UpdateBitmap() and hash[val] is
  /// not in that bitmap. Otherwise returns true. Is safe to call concurrently with
  /// UpdateBitmap().
  ///
  /// Templatized in preparation for templatized hashes.
  template<typename T>
  inline bool Eval(T val, const ColumnType& col_type) const {
    // Safe to read bitmap_ concurrently with any ongoing UpdateBitmap() thanks to a) the
    // atomicity of / pointer assignments and b) the x86 TSO memory model.
    if (bitmap_ == NULL) return true;
    uint32_t h = RawValue::GetHashValue(reinterpret_cast<void*>(val), col_type,
        Bitmap::DefaultHashSeed());
    return bitmap_->Get<true>(h);
  }

 private:
  /// Membership bitmap populated by filter producing-operator.
  Bitmap* bitmap_;

  /// Descriptor of the filter.
  TRuntimeFilter filter_desc_;
};

/// Runtime filters are produced and consumed by plan nodes at run time to propagate
/// predicates across the plan tree dynamically. Each fragment instance manages its
/// filters with a RuntimeFilterBank which provides low-synchronization access to filter
/// objects and data structures.
///
/// A RuntimeFilterBank manages both production and consumption of filters. In the case
/// where a given filter is both consumed and produced by the same fragment, the
/// RuntimeFilterBank treats each filter independently.
///
/// All filters must be registered with the filter bank via RegisterFilter(). Filters that
/// are produced by the local plan fragment have their bitmaps set by calling
/// PublishBitmap(). The bitmap that is passed into PublishBitmap() must have been
/// allocated by AllocateScratchBitmap(); this allows RuntimeFilterBank to manage all
/// memory associated with filters.
///
/// PublishBitmap() may only be called once per filter_id; afterwards the filter is
/// consider to be 'published' and may be read by other fragments (even those on remote
/// machines). This restriction will be relaxed when multiple producers of a given filter
/// ID may exist in one fragment (e.g. multi-threaded execution).
///
/// Bitmaps are made available to consumers by calling DeliverBitmap() - all calls to
/// PublishBitmap() will result in a consequent call to DeliverBitmap() (although perhaps
/// in some other filter bank). After DeliverBitmap() has been called (and again, it may
/// only be called once per filter_id), the RuntimeFilter object associated with filter_id
/// will have a valid bitmap, and may be used for filter evaluation. This operation occurs
/// without synchronisation, and neither the thread that calls DeliverBitmap() nor the
/// thread that may call RuntimeFilter::Eval() need to coordinate in any way.
class RuntimeFilterBank {
 public:
  RuntimeFilterBank(const TQueryCtx& query_ctx) : query_ctx_(query_ctx) { }

  /// Registers a filter that will either be produced (is_producer == false) or consumed
  /// (is_producer == true) by fragments that share this RuntimeState. The filter bitmap
  /// itself is unallocated until the first call to PublishBitmap().
  void RegisterFilter(const TRuntimeFilter& filter_desc, bool is_producer);

  /// Updates a filter's bitmap with 'bitmap' which has been produced by some operator in
  /// the local fragment instance.
  void PublishBitmap(uint32_t filter_id, Bitmap* bitmap);

  /// Makes a bitmap available for consumption by operators that wish to use it for
  /// filtering. After this call the memory allocated to 'bitmap' is owned by this filter
  /// bank.
  void DeliverBitmap(uint32_t filter_id, Bitmap* bitmap);

  /// Size to use when building bitmap filters.
  uint32_t filter_bitmap_size() const { return 32768; }

  /// Returns a RuntimeFilter with the given filter id. This is safe to call after all
  /// calls to RegisterFilter() have finished, and not before. Filters may be cached by
  /// clients and subsequently accessed without synchronization. Concurrent calls to
  /// DeliverBitmap() will update a filter's bitmap atomically, without the need for
  /// client synchronization.
  const RuntimeFilter* GetRuntimeFilter(uint32_t filter_id) {
    boost::lock_guard<SpinLock> l(runtime_filter_lock_);
    RuntimeFilterMap::iterator it = consumed_filters_.find(filter_id);
    if (it == consumed_filters_.end()) return NULL;
    return it->second;
  }

  /// Returns a bitmap that can be used by an operator to produce an update for a
  /// filter. Filters are updated (by logical OR'ing) with the contents of a scratch
  /// bitmap by calling PublishBitmap(id, bitmap). The memory returned is owned by the
  /// RuntimeFilterBank (which may transfer it to a RuntimeFilter subsequently), and
  /// should not be deleted by the caller.
  Bitmap* AllocateScratchBitmap(uint32_t size) {
    boost::lock_guard<SpinLock> l(runtime_filter_lock_);
    Bitmap* bitmap = new Bitmap(size);
    producer_bitmaps_.insert(bitmap);
    return bitmap;
  }

  ~RuntimeFilterBank();

 private:
  const TQueryCtx query_ctx_;

  /// Lock protecting produced_filters_ and consumed_filters_.
  SpinLock runtime_filter_lock_;

  /// Map from filter id to a RuntimeFilter.
  typedef boost::unordered_map<uint32_t, RuntimeFilter*> RuntimeFilterMap;
  /// All filters expected to be produced by the local plan fragment instance.
  RuntimeFilterMap produced_filters_;

  /// All filters expected to be consumed by the local plan fragment instance.
  RuntimeFilterMap consumed_filters_;

  /// All 'scratch' bitmaps handed out to producer plan nodes.
  std::set<Bitmap*> producer_bitmaps_;
};

}

#endif
