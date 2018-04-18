// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_RUNTIME_TIMESTAMP_VALUE_INLINE_H
#define IMPALA_RUNTIME_TIMESTAMP_VALUE_INLINE_H

#include "runtime/timestamp-value.h"

#include <boost/date_time/compiler_config.hpp>
#include <boost/date_time/posix_time/conversion.hpp>
#include <chrono>

#include "cctz/civil_time.h"
#include "exprs/timezone_db.h"
#include "gutil/walltime.h"

namespace impala {

inline TimestampValue TimestampValue::UtcFromUnixTimeMicros(int64_t unix_time_micros) {
  int64_t ts_seconds = unix_time_micros / MICROS_PER_SEC;
  int64_t micros_part = unix_time_micros - (ts_seconds * MICROS_PER_SEC);
  boost::posix_time::ptime temp = UnixTimeToUtcPtime(ts_seconds);
  temp += boost::posix_time::microseconds(micros_part);
  return TimestampValue(temp);
}

inline TimestampValue TimestampValue::FromUnixTimeMicros(int64_t unix_time_micros,
    const cctz::time_zone* local_tz) {
  DCHECK(local_tz != nullptr);
  int64_t ts_seconds = unix_time_micros / MICROS_PER_SEC;
  int64_t micros_part = unix_time_micros - (ts_seconds * MICROS_PER_SEC);
  boost::posix_time::ptime temp = UnixTimeToLocalPtime(ts_seconds, local_tz);
  temp += boost::posix_time::microseconds(micros_part);
  return TimestampValue(temp);
}

/// Interpret 'this' as a timestamp in UTC and convert to unix time.
/// Returns false if the conversion failed ('unix_time' will be undefined), otherwise
/// true.
inline bool TimestampValue::UtcToUnixTime(time_t* unix_time) const {
  DCHECK(unix_time != nullptr);
  if (UNLIKELY(!HasDateAndTime())) return false;

  static const boost::gregorian::date epoch(1970,1,1);
  *unix_time = (date_ - epoch).days() * 24 * 60 * 60 + time_.total_seconds();
  return true;
}

/// Interpret 'this' as a timestamp in UTC and convert to unix time in microseconds.
/// Nanoseconds are rounded to the nearest microsecond supported by Impala. Returns
/// false if the conversion failed ('unix_time_micros' will be undefined), otherwise
/// true.
inline bool TimestampValue::UtcToUnixTimeMicros(int64_t* unix_time_micros) const {
  const static int64_t MAX_UNIXTIME = 253402300799; // 9999-12-31 23:59:59
  const static int64_t MAX_UNIXTIME_MICROS =
      MAX_UNIXTIME * MICROS_PER_SEC + (MICROS_PER_SEC - 1);

  DCHECK(unix_time_micros != nullptr);
  time_t unixtime_seconds;
  if (UNLIKELY(!UtcToUnixTime(&unixtime_seconds))) return false;

  *unix_time_micros =
    (static_cast<int64_t>(unixtime_seconds) * MICROS_PER_SEC) +
    ((time_.fractional_seconds() + (NANOS_PER_MICRO / 2)) / NANOS_PER_MICRO);

  // Rounding may result in the timestamp being MAX_UNIXTIME_MICROS+1 and should be
  // truncated.
  DCHECK_LE(*unix_time_micros, MAX_UNIXTIME_MICROS + 1);
  *unix_time_micros = std::min(MAX_UNIXTIME_MICROS, *unix_time_micros);
  return true;
}

/// Converts to Unix time (seconds since the Unix epoch) representation. The time
/// zone interpretation of the TimestampValue instance is determined by
/// FLAGS_use_local_tz_for_unix_timestamp_conversions. If the flag is true, the
/// instance is interpreted as a local value. If the flag is false, UTC is assumed.
/// Returns false if the conversion failed (unix_time will be undefined), otherwise
/// true.
inline bool TimestampValue::ToUnixTime(const cctz::time_zone* local_tz,
    time_t* unix_time) const {
  DCHECK(local_tz != nullptr);
  DCHECK(unix_time != nullptr);
  if (UNLIKELY(!HasDateAndTime())) return false;
  cctz::civil_second cs(date_.year(), date_.month(), date_.day(), time_.hours(),
      time_.minutes(), time_.seconds());
  auto tp = cctz::convert(cs,
      FLAGS_use_local_tz_for_unix_timestamp_conversions ? *local_tz :
      TimezoneDatabase::GetUtcTimezone());
  auto seconds = tp.time_since_epoch();
  *unix_time = seconds.count();
  return true;
}

/// Converts to Unix time with fractional seconds.
/// Returns false if the conversion failed (unix_time will be undefined), otherwise
/// true.
inline bool TimestampValue::ToSubsecondUnixTime(const cctz::time_zone* local_tz,
    double* unix_time) const {
  DCHECK(local_tz != nullptr);
  DCHECK(unix_time != nullptr);
  time_t temp;
  if (UNLIKELY(!ToUnixTime(local_tz, &temp))) return false;
  *unix_time = static_cast<double>(temp);
  DCHECK(HasTime());
  *unix_time += time_.fractional_seconds() * ONE_BILLIONTH;
  return true;
}

}

#endif
