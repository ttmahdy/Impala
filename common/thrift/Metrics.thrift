// Copyright 2015 Cloudera Inc.
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

// Metric and counter data types.
enum TUnit {
  // A dimensionless numerical quantity
  UNIT,
  // Rate of a dimensionless numerical quantity
  UNIT_PER_SECOND,
  CPU_TICKS,
  BYTES
  BYTES_PER_SECOND,
  TIME_NS,
  DOUBLE_VALUE,
  // No units at all, may not be a numerical quantity
  NONE,
  TIME_MS,
  TIME_S
}

// The kind of value that a metric represents.
enum TMetricKind {
  // May go up or down over time
  GAUGE,
  // A strictly increasing value
  COUNTER,
  // Fixed; will never change
  PROPERTY,
  STATS,
  SET,
  HISTOGRAM,
  TIMER
}

union TSimpleMetric {
  1: i64 int64
  2: double dbl
  3: string str
  4: bool boolean
}

union TMetricInstance {
  1: TSimpleMetric simple
  // 1: TGaugeMetric gauge;
  // 2: TCounterMetric counter;
  // 3: TPropertyMetric property;
  // 4: TStatsMetric stats;
  // //5: TSetMetric set;
  // 6: THistogramMetric histogram;
  // 7: TTimerMetric timer;
}

struct TMetric {
  1: optional string key;
  2: optional TMetricInstance metric;
}

struct TMetricGroup {
  1: optional list<TMetric> metrics;
  2: optional i32 num_children;
  3: optional string name;
}

struct TMetricTree {
  1: optional list<TMetricGroup> groups;
}

// TODO: Flattened metric groups
