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

#include "runtime/runtime-filter.h"

#include "common/names.h"
#include "runtime/client-cache.h"
#include "runtime/exec-env.h"
#include "service/impala-server.h"

using namespace impala;
using namespace boost;

void RuntimeFilterBank::RegisterFilter(const TRuntimeFilter& filter_desc,
    bool is_producer) {
  lock_guard<SpinLock> l(runtime_filter_lock_);
  if (is_producer) {
    DCHECK(produced_filters_.find(filter_desc.filter_id) == produced_filters_.end());
    produced_filters_[filter_desc.filter_id] = new RuntimeFilter(filter_desc);
  } else {
    DCHECK(consumed_filters_.find(filter_desc.filter_id) == consumed_filters_.end());
    consumed_filters_[filter_desc.filter_id] = new RuntimeFilter(filter_desc);
  }
}

namespace {

/// Sends a filter to the coordinator. Executed asynchronously in the context of
/// ExecEnv::rpc_pool().
void SendFilterToCoordinator(TNetworkAddress address, TPublishFilterParams params,
    ImpalaInternalServiceClientCache* client_cache) {
  Status status;
  ImpalaInternalServiceConnection coord(client_cache, address, &status);
  if (!status.ok()) {
    // Failing to send a filter is not a query-wide error - the remote fragment will
    // continue regardless.
    // TODO: Retry.
    LOG(INFO) << "Couldn't send filter to coordinator: " << status.msg().msg();
    return;
  }
  TPublishFilterResult res;
  status = coord.DoRpc(&ImpalaInternalServiceClient::PublishFilter, params, &res);
}

}

void RuntimeFilterBank::PublishBitmap(uint32_t filter_id, Bitmap* bitmap) {
  TPublishFilterParams params;
  {
    lock_guard<SpinLock> l(runtime_filter_lock_);
    RuntimeFilterMap::iterator it = produced_filters_.find(filter_id);
    DCHECK(it != produced_filters_.end()) << "Tried to update unregistered filter: "
                                           << filter_id;
    it->second->UpdateBitmap(bitmap);
    it->second->GetBitmap()->ToThrift(&params.bitmap);
    Bitmap tmp(params.bitmap);

    size_t removed = producer_bitmaps_.erase(bitmap);
    DCHECK(removed == 1) << "Tried to remove bitmap that didn't exist!";
  }

  params.filter_id = filter_id;
  params.query_id = query_ctx_.query_id;

  ExecEnv::GetInstance()->rpc_pool()->Offer(bind<void>(
          SendFilterToCoordinator, query_ctx_.coord_address, params,
          ExecEnv::GetInstance()->impalad_client_cache()));

  // TODO: If this is a broadcast bitmap, and this fragment can consume it, do a
  // short-circuit DeliverBitmap() here.
}

void RuntimeFilterBank::DeliverBitmap(uint32_t filter_id, Bitmap* bitmap) {
  {
    lock_guard<SpinLock> l(runtime_filter_lock_);
    RuntimeFilterMap::iterator it = consumed_filters_.find(filter_id);
    DCHECK(it != consumed_filters_.end()) << "Tried to update unregistered filter: "
                                           << filter_id;
    it->second->UpdateBitmap(bitmap);
  }
}

RuntimeFilterBank::~RuntimeFilterBank() {
  BOOST_FOREACH(Bitmap* b, producer_bitmaps_) {
    delete b;
  }

  BOOST_FOREACH(RuntimeFilterMap::value_type v, produced_filters_) {
    delete v.second;
  }

  BOOST_FOREACH(RuntimeFilterMap::value_type v, consumed_filters_) {
    delete v.second;
  }
}
