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


#ifndef IMPALA_EXEC_HDFS_PARQUET_SCANNER_H
#define IMPALA_EXEC_HDFS_PARQUET_SCANNER_H

#include "exec/hdfs-scanner.h"
#include "exec/parquet-common.h"

namespace impala {

struct HdfsFileDesc;

/// This scanner parses Parquet files located in HDFS, and writes the
/// content as tuples in the Impala in-memory representation of data, e.g.
/// (tuples, rows, row batches).
/// For the file format spec, see: github.com/Parquet/parquet-format
//
/// Parquet (and other columnar formats) use scanner ranges differently than
/// other formats.  Each materialized column maps to a single ScanRange.  For
/// streaming reads, all the columns need to be read in parallel. This is done
/// by issuing one ScanRange (in IssueInitialRanges()) for the file footer as
/// the other scanners do. This footer range is processed in ProcessSplit().
/// ProcessSplit() then computes the column ranges and submits them to the IoMgr
/// for immediate scheduling (so they don't surface in DiskIoMgr::GetNextRange()).
/// Scheduling them immediately also guarantees they are all read at once.
//
/// Like the other scanners, each parquet scanner object is one to one with a
/// ScannerContext. Unlike the other scanners though, the context will have multiple
/// streams, one for each column.
class HdfsParquetScanner : public HdfsScanner {
 public:
  HdfsParquetScanner(HdfsScanNode* scan_node, RuntimeState* state);

  virtual ~HdfsParquetScanner();
  virtual Status Prepare(ScannerContext* context);
  virtual void Close();
  virtual Status ProcessSplit();

  /// Issue just the footer range for each file.  We'll then parse the footer and pick
  /// out the columns we want.
  static Status IssueInitialRanges(HdfsScanNode* scan_node,
                                   const std::vector<HdfsFileDesc*>& files);

  struct FileVersion {
    /// Application that wrote the file. e.g. "IMPALA"
    std::string application;

    /// Version of the application that wrote the file, expressed in three parts
    /// (<major>.<minor>.<patch>). Unspecified parts default to 0, and extra parts are
    /// ignored. e.g.:
    /// "1.2.3"    => {1, 2, 3}
    /// "1.2"      => {1, 2, 0}
    /// "1.2-cdh5" => {1, 2, 0}
    struct {
      int major;
      int minor;
      int patch;
    } version;

    /// If true, this file was generated by an Impala internal release
    bool is_impala_internal;

    FileVersion() : is_impala_internal(false) { }

    /// Parses the version from the created_by string
    FileVersion(const std::string& created_by);

    /// Returns true if version is strictly less than <major>.<minor>.<patch>
    bool VersionLt(int major, int minor = 0, int patch = 0) const;

    /// Returns true if version is equal to <major>.<minor>.<patch>
    bool VersionEq(int major, int minor, int patch) const;
  };

 private:
  /// Internal representation of a column schema (including nested-type columns).
  struct SchemaNode {
    /// The corresponding schema element defined in the file metadata
    const parquet::SchemaElement* element;

    /// The index into the RowGroup::columns list if this column is materialized in the
    /// file (i.e. it's a scalar type). -1 for nested types.
    int col_idx;

    /// The maximum definition level of this column, i.e., the definition level that
    /// corresponds to a non-NULL value. Valid values are >= 0.
    int max_def_level;

    // The maximum repetition level of this column. Valid values are >= 0.
    int max_rep_level;

    /// Any nested schema nodes. Empty for non-nested types.
    std::vector<SchemaNode> children;

    SlotDescriptor* slot_desc;

    SchemaNode() : col_idx(-1), max_def_level(-1), slot_desc(NULL) { }
    std::string DebugString(int indent = 0) const;
  };

  /// Size of the file footer.  This is a guess.  If this value is too little, we will
  /// need to issue another read.
  static const int FOOTER_SIZE = 100 * 1024;

  /// Per column reader.
  class BaseColumnReader;
  friend class BaseColumnReader;

  template<typename T> class ColumnReader;
  template<typename T> friend class ColumnReader;
  class BoolColumnReader;
  friend class BoolColumnReader;

  /// Column reader for each materialized columns for this file.
  std::vector<BaseColumnReader*> column_readers_;

  /// File metadata thrift object
  parquet::FileMetaData file_metadata_;

  /// Version of the application that wrote this file.
  FileVersion file_version_;

  /// The root schema node for this file
  SchemaNode schema_;

  /// Scan range for the metadata.
  const DiskIoMgr::ScanRange* metadata_range_;

  /// Returned in ProcessSplit
  Status parse_status_;

  /// Pool to copy dictionary page buffer into. This pool is shared across all the
  /// pages in a column chunk.
  std::unique_ptr<MemPool> dictionary_pool_;

  /// Timer for materializing rows.  This ignores time getting the next buffer.
  ScopedTimer<MonotonicStopWatch> assemble_rows_timer_;

  /// Number of cols that need to be read.
  RuntimeProfile::Counter* num_cols_counter_;

  /// Reads data from all the columns (in parallel) and assembles rows into the context
  /// object. Returns when the entire row group is complete or an error occurred.
  Status AssembleRows(int row_group_idx);

  /// Process the file footer and parse file_metadata_.  This should be called with the
  /// last FOOTER_SIZE bytes in context_.
  /// *eosr is a return value.  If true, the scan range is complete (e.g. select count(*))
  Status ProcessFooter(bool* eosr);

  /// Populates column_readers_ from the file schema. Schema resolution is handled in
  /// this function as well.
  /// We allow additional columns at the end in either the table or file schema.
  /// If there are extra columns in the file schema, it is simply ignored. If there
  /// are extra in the table schema, we return NULLs for those columns.
  Status CreateColumnReaders();

  /// Creates a reader for node. node must refer to a non-nested column and node.slot_desc
  /// must be non-NULL. The reader is added to the runtime state's object pool.
  BaseColumnReader* CreateReader(const SchemaNode& node);

  /// Walks file_metadata_ and initiates reading the materialized columns.  This
  /// initializes column_readers_ and issues the reads for the columns.
  Status InitColumns(int row_group_idx);

  /// Validates the file metadata
  Status ValidateFileMetadata();

  /// Validates the column metadata to make sure this column is supported (e.g. encoding,
  /// type, etc) and matches the type of col_reader's slot desc.
  Status ValidateColumn(const BaseColumnReader& col_reader, int row_group_idx);

  /// Part of the HdfsScanner interface, not used in Parquet.
  Status InitNewRange() { return Status::OK(); };

  /// Unflattens the schema metadata from a Parquet file metadata and converts it to our
  /// SchemaNode representation. Returns the result in 'n' unless an error status is
  /// returned. Does not set the slot_desc field of any SchemaNode.
  Status CreateSchemaTree(const std::vector<parquet::SchemaElement>& schema,
      SchemaNode* node) const;

  /// Recursive implementation used internally by the above CreateSchemaTree() function.
  Status CreateSchemaTree(const std::vector<parquet::SchemaElement>& schema,
      int max_def_level, int max_rep_level, int* idx, int* col_idx, SchemaNode* node)
      const;
};

} // namespace impala

#endif
