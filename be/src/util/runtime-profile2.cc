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

#include "util/runtime-profile2.h"

using namespace impala;
using namespace std;

void RuntimeProfile2::InitFromPlanHelper(const TQueryExecRequest& request,
    const SenderMap& sender_map, int idx) {
  const TPlanFragment& fragment = request.fragments[idx];
  MetricGroup* summary =
      query_metrics_->GetChildGroup("fragment_summaries")->GetChildGroup(
          fragment.display_name);

  fragment_profiles_.push_back(PlanFragmentProfile());
  PlanFragmentProfile* profile = &fragment_profiles_.back();
  profile->fragment_ = fragment;
  profile->summary_ = summary;
  profile->num_children_ = 0;
  fragment_id_to_idx_[fragment.display_name] = fragment_profiles_.size() - 1;

  SenderMap::const_iterator it = sender_map.find(idx);
  if (it == sender_map.end()) return;
  profile->num_children_ = it->second.size();
  BOOST_FOREACH(int32_t i, it->second) {
    InitFromPlanHelper(request, sender_map, i);
  }
}

void RuntimeProfile2::InitFromPlan(const TQueryExecRequest& request) {
  // Initialise both PlanFragmentProfile and query_metrics_

  // Build a map from receiver to list of senders
  map<int32_t, vector<int32_t> > sender_map;
  for (int i = 0; i < request.dest_fragment_idx.size(); ++i) {
    sender_map[request.dest_fragment_idx[i]].push_back(i + 1);
  }

  BOOST_FOREACH(const TPlanFragment& fragment, request.fragments) {
    MetricGroup* summary =
        query_metrics_->GetChildGroup("fragment_summaries")->GetChildGroup(
            fragment.display_name);

    fragment_profiles_.push_back(PlanFragmentProfile());
    PlanFragmentProfile* profile = &fragment_profiles_.back();
    profile->fragment_ = fragment;
    profile->summary_ = summary;
  }
}
