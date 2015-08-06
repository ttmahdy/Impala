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


#ifndef IMPALA_UTIL_RUNTIME_PROFILE2_H
#define IMPALA_UTIL_RUNTIME_PROFILE2_H

#include "util/metrics.h"

#include <vector>
#include <gutil/strings/substitute.h>

namespace impala {

class RuntimeProfile2 {
 public:
  struct PlanFragmentProfile {
    MetricGroup* summary_;
    std::vector<MetricGroup*> instances_;
    TPlanFragment fragment_;
  };

  PlanFragmentProfile* root_;

  boost::scoped_ptr<MetricGroup> query_metrics_;

  RuntimeProfile2(const TUniqueId& query_id) {
    query_metrics_.reset(
        new MetricGroup(strings::Substitute("query_id=$0", PrintId(query_id))));
  }

  void InitFromPlan(const std::vector<TPlanFragment>& fragments) { }


  // ToJson(Document*, Value*);
};

}

#endif
