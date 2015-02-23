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

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "CatalogObjects.thrift"
include "Types.thrift"
include "Exprs.thrift"

// Descriptors are the data structures used to send query metadata (for example, slot,
// tuple and table information) to backends from the coordinator.

struct THdfsPartitionDescriptor {
  1: required byte lineDelim
  2: required byte fieldDelim
  3: required byte collectionDelim
  4: required byte mapKeyDelim
  5: required byte escapeChar
  6: required CatalogObjects.THdfsFileFormat fileFormat
  7: list<Exprs.TExpr> partitionKeyExprs
  8: required i32 blockSize
  9: optional string location

  // If true, the location field is relative to the base HDFS table directory. If false,
  // the location is an absolute path (for external partitions).
  10: optional bool location_is_relative

  // Unique (in this table) id of this partition. If -1, the partition does not currently
  // exist.
  11: optional i64 id
}

struct THdfsTableDescriptor {
  1: required string hdfsBaseDir

  // The string used to represent NULL partition keys.
  2: required string nullPartitionKeyValue

  // String to indicate a NULL column value in text files
  3: required string nullColumnValue

  // Set to the table's Avro schema if this is an Avro table
  4: optional string avroSchema

  // map from partition id to partition metadata
  5: required map<i64, THdfsPartitionDescriptor> partitions

  // Indicates that this table's partitions reside on more than one filesystem.
  // TODO: remove once INSERT across filesystems is supported.
  6: optional bool multiple_filesystems
}

struct THBaseTableDescriptor {
  1: required string tableName
  2: required list<string> families
  3: required list<string> qualifiers

  // Column i is binary encoded if binary_encoded[i] is true. Otherwise, column i is
  // text encoded.
  4: optional list<bool> binary_encoded
}

struct TDataSourceTableDescriptor {
  // The data source that will scan this table.
  1: required CatalogObjects.TDataSource data_source

  // Init string for the table passed to the data source. May be an empty string.
  2: required string init_string
}

// "Union" of all table types.
struct TTableDescriptor {
  1: required Types.TTableId id
  2: required CatalogObjects.TTableType tableType
  3: required i32 numCols
  4: required i32 numClusteringCols
  // Names of the columns. Clustering columns come first.
  10: optional list<string> colNames;
  5: optional THdfsTableDescriptor hdfsTable
  6: optional THBaseTableDescriptor hbaseTable
  9: optional TDataSourceTableDescriptor dataSourceTable

  // Unqualified name of table
  7: required string tableName;

  // Name of the database that the table belongs to
  8: required string dbName;
}

struct TTupleDescriptor {
  1: required Types.TTupleId id
  2: required i32 byteSize
  3: required i32 numNullBytes
  4: optional Types.TTableId tableId

  // Absolute path into the table schema pointing to the collection whose fields
  // are materialized into this tuple. Non-empty if this tuple belongs to a
  // nested collection, empty otherwise.
  5: optional list<i32> tuplePath
}

struct TSlotDescriptor {
  1: required Types.TSlotId id
  2: required Types.TTupleId parent
  3: required Types.TColumnType slotType

  // Absolute path into the table schema pointing to the column/field materialized into
  // this slot. Contains only a single element for slots that do not materialize a
  // table column/field, e.g., slots materializing an aggregation result.
  // columnPath[i] is the the ordinal position of the column/field of the table schema
  // at level i. For example, columnPos[0] is an ordinal into the list of table columns,
  // columnPos[1] is an ordinal into the list of fields of the complex-typed column at
  // position columnPos[0], etc.
  4: required list<i32> columnPath

  5: required i32 byteOffset  // into tuple
  6: required i32 nullIndicatorByte
  7: required i32 nullIndicatorBit
  8: required i32 slotIdx
  9: required bool isMaterialized
}

struct TDescriptorTable {
  1: optional list<TSlotDescriptor> slotDescriptors;
  2: required list<TTupleDescriptor> tupleDescriptors;

  // all table descriptors referenced by tupleDescriptors
  3: optional list<TTableDescriptor> tableDescriptors;
}
