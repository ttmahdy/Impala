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
#include <map>
#include <gutil/strings/substitute.h>

namespace impala {

class RuntimeProfile2 {
 public:
  struct PlanFragmentProfile {
    MetricGroup* summary_;
    std::map<std::string, MetricGroup*> instances_;
    TPlanFragment fragment_;
    int32_t num_children_;
  };

  std::vector<PlanFragmentProfile> fragment_profiles_;
  std::map<std::string, int32_t> fragment_id_to_idx_;

  boost::scoped_ptr<MetricGroup> query_metrics_;

  RuntimeProfile2(const TUniqueId& query_id) {
    query_metrics_.reset(
        new MetricGroup(strings::Substitute("query_id=$0", PrintId(query_id))));
  }

  void InitFromPlan(const TQueryExecRequest& request);

  typedef std::map<int32_t, std::vector<int32_t> > SenderMap;

  void InitFromPlanHelper(const TQueryExecRequest& request,
      const SenderMap& sender_map, int idx);

  // ToJson(Document*, Value*);
};

}

#endif
