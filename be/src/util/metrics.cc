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

#include "util/metrics.h"

#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/math/special_functions/fpclassify.hpp>
#include <gutil/strings/substitute.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

#include "common/logging.h"
#include "util/impalad-metrics.h"

#include "common/names.h"

using namespace impala;
using namespace rapidjson;
using namespace strings;

namespace impala {

template <>
TSimpleMetric ToTSimpleMetric(const double& val) {
  TSimpleMetric ret;
  ret.__set_dbl(val);
  return ret;
}

template <>
TSimpleMetric ToTSimpleMetric(const int64_t& val) {
  TSimpleMetric ret;
  ret.__set_int64(val);
  return ret;
}

template <>
TSimpleMetric ToTSimpleMetric(const std::string& val) {
  TSimpleMetric ret;
  ret.__set_str(val);
  return ret;
}

template <>
TSimpleMetric ToTSimpleMetric(const bool& val) {
  TSimpleMetric ret;
  ret.__set_bool_val(val);
  return ret;
}

template <>
TSimpleMetric ToTSimpleMetric(const uint64_t& val) {
  TSimpleMetric ret;
  ret.__set_int64(val);
  return ret;
}

template <>
int64_t TSimpleMetricToValue<int64_t>(const TSimpleMetric& simple) {
  return simple.int64;
}

template <>
double TSimpleMetricToValue<double>(const TSimpleMetric& simple) {
  return simple.dbl;
}

template <>
uint64_t TSimpleMetricToValue<uint64_t>(const TSimpleMetric& simple) {
  return simple.int64;
}

template <>
string TSimpleMetricToValue<string>(const TSimpleMetric& simple) {
  return simple.str;
}

template <>
bool TSimpleMetricToValue<bool>(const TSimpleMetric& simple) {
  return simple.bool_val;
}

template <>
void ToJsonValue<string>(const string& value, const TUnit::type unit,
    Document* document, Value* out_val) {
  Value val(value.c_str(), document->GetAllocator());
  *out_val = val;
}

}

void Metric::AddStandardFields(Document* document, Value* val) {
  Value name(key_.c_str(), document->GetAllocator());
  val->AddMember("name", name, document->GetAllocator());
  Value desc(description_.c_str(), document->GetAllocator());
  val->AddMember("description", desc, document->GetAllocator());
  Value metric_value(ToHumanReadable().c_str(), document->GetAllocator());
  val->AddMember("human_readable", metric_value, document->GetAllocator());
}

MetricDefs* MetricDefs::GetInstance() {
  // Note that this is not thread-safe in C++03 (but will be in C++11 see
  // http://stackoverflow.com/a/19907903/132034). We don't bother with the double-check
  // locking pattern because it introduces complexity whereas a race is very unlikely
  // and it doesn't matter if we construct two instances since MetricDefsConstants is
  // just a constant map.
  static MetricDefs instance;
  return &instance;
}

TMetricDef MetricDefs::Get(const string& key, const string& arg) {
  MetricDefs* inst = GetInstance();
  map<string, TMetricDef>::iterator it = inst->metric_defs_.TMetricDefs.find(key);
  if (it == inst->metric_defs_.TMetricDefs.end()) {
    DCHECK(false) << "Could not find metric definition for key=" << key << " arg=" << arg;
    return TMetricDef();
  }
  TMetricDef md = it->second;
  md.__set_key(Substitute(md.key, arg));
  md.__set_description(Substitute(md.description, arg));
  return md;
}

MetricGroup::MetricGroup(const string& name)
    : obj_pool_(new ObjectPool()), name_(name) { }

Status MetricGroup::Init(Webserver* webserver) {
  if (webserver != NULL) {
    Webserver::UrlCallback default_callback =
        bind<void>(mem_fn(&MetricGroup::CMCompatibleCallback), this, _1, _2);
    webserver->RegisterUrlCallback("/jsonmetrics", "legacy-metrics.tmpl",
        default_callback, false);

    Webserver::UrlCallback json_callback =
        bind<void>(mem_fn(&MetricGroup::TemplateCallback), this, _1, _2);
    webserver->RegisterUrlCallback("/metrics", "metrics.tmpl", json_callback);
  }

  return Status::OK();
}

void MetricGroup::CMCompatibleCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  // If the request has a 'metric' argument, search all top-level metrics for that metric
  // only. Otherwise, return document with list of all metrics at the top level.
  Webserver::ArgumentMap::const_iterator metric_name = args.find("metric");

  lock_guard<SpinLock> l(lock_);
  if (metric_name != args.end()) {
    MetricMap::const_iterator metric = metric_map_.find(metric_name->second);
    if (metric != metric_map_.end()) {
      metric->second->ToLegacyJson(document);
    }
    return;
  }

  stack<MetricGroup*> groups;
  groups.push(this);
  do {
    // Depth-first traversal of children to flatten all metrics, which is what was
    // expected by CM before we introduced metric groups.
    MetricGroup* group = groups.top();
    groups.pop();
    BOOST_FOREACH(const ChildGroupMap::value_type& child, group->children_) {
      groups.push(child.second);
    }
    BOOST_FOREACH(const MetricMap::value_type& m, group->metric_map_) {
      m.second->ToLegacyJson(document);
    }
  } while (!groups.empty());
}

void MetricGroup::TemplateCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  Webserver::ArgumentMap::const_iterator metric_group = args.find("metric_group");

  lock_guard<SpinLock> l(lock_);
  // If no particular metric group is requested, render this metric group (and all its
  // children).
  if (metric_group == args.end()) {
    Value container;
    ToJson(true, document, &container);
    document->AddMember("metric_group", container, document->GetAllocator());
    return;
  }

  // Search all metric groups to find the one we're looking for. In the future, we'll
  // change this to support path-based resolution of metric groups.
  MetricGroup* found_group = NULL;
  stack<MetricGroup*> groups;
  groups.push(this);
  while (!groups.empty() && found_group == NULL) {
    // Depth-first traversal of children to flatten all metrics, which is what was
    // expected by CM before we introduced metric groups.
    MetricGroup* group = groups.top();
    groups.pop();
    BOOST_FOREACH(const ChildGroupMap::value_type& child, group->children_) {
      if (child.first == metric_group->second) {
        found_group = child.second;
        break;
      }
      groups.push(child.second);
    }
  }
  if (found_group != NULL) {
    Value container;
    found_group->ToJson(false, document, &container);
    document->AddMember("metric_group", container, document->GetAllocator());
  } else {
    Value error(Substitute("Metric group $0 not found", metric_group->second).c_str(),
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}

void MetricGroup::ToJson(bool include_children, Document* document, Value* out_val) {
  Value metric_list(kArrayType);
  BOOST_FOREACH(const MetricMap::value_type& m, metric_map_) {
    Value metric_value;
    m.second->ToJson(document, &metric_value);
    metric_list.PushBack(metric_value, document->GetAllocator());
  }

  Value container(kObjectType);
  container.AddMember("metrics", metric_list, document->GetAllocator());
  container.AddMember("name", name_.c_str(), document->GetAllocator());
  if (include_children) {
    Value child_groups(kArrayType);
    BOOST_FOREACH(const ChildGroupMap::value_type& child, children_) {
      Value child_value;
      child.second->ToJson(true, document, &child_value);
      child_groups.PushBack(child_value, document->GetAllocator());
    }
    container.AddMember("child_groups", child_groups, document->GetAllocator());
  }

  *out_val = container;
}

MetricGroup* MetricGroup::GetChildGroup(const string& name) {
  lock_guard<SpinLock> l(lock_);
  ChildGroupMap::iterator it = children_.find(name);
  if (it != children_.end()) return it->second;
  MetricGroup* group = obj_pool_->Add(new MetricGroup(name));
  children_[name] = group;
  return group;
}

string MetricGroup::DebugString() {
  Webserver::ArgumentMap empty_map;
  Document document;
  document.SetObject();
  TemplateCallback(empty_map, &document);
  StringBuffer strbuf;
  PrettyWriter<StringBuffer> writer(strbuf);
  document.Accept(writer);
  return strbuf.GetString();
}

TMetricTree MetricGroup::ToThrift() {
  TMetricTree ret;
  ret.__set_groups(vector<TMetricGroup>());
  ToTMetricGroup(&ret.groups);
  return ret;
}

void MetricGroup::ToTMetricGroup(vector<TMetricGroup>* groups) {
  TMetricGroup group;
  lock_guard<SpinLock> l(lock_);
  group.__set_num_children(children_.size());
  group.__set_name(name_);
  group.__set_metrics(vector<TMetric>());
  BOOST_FOREACH(MetricMap::value_type& metric, metric_map_) {
    const TMetric& m = metric.second->ToThrift();
    group.metrics.push_back(m);
  }
  groups->push_back(group);
  BOOST_FOREACH(const ChildGroupMap::value_type& child, children_) {
    child.second->ToTMetricGroup(groups);
  }
}

template <TMetricKind::type V>
Metric* SimpleMetricFactory(const TMetricDef& def, const TSimpleMetric& metric) {
  if (metric.__isset.int64) {
    return new SimpleMetric<int64_t, V>(def, metric.int64);
  } else if (metric.__isset.dbl) {
    return new SimpleMetric<double, V>(def, metric.dbl);
  } else if (metric.__isset.str) {
    return new SimpleMetric<string, V>(def, metric.str);
  } else if (metric.__isset.bool_val) {
    return new SimpleMetric<bool, V>(def, metric.bool_val);
  } else {
    return NULL;
  }
}

Metric* MetricFactory(const TMetric& metric) {
  if (!metric.__isset.metric_def) return NULL;
  const TMetricDef& def =
      metric.__isset.metric_def ? metric.metric_def : MetricDefs::Get(metric.key);

  if (metric.metric.__isset.simple) {
    switch (def.kind) {
      case TMetricKind::GAUGE:
        return SimpleMetricFactory<TMetricKind::GAUGE>(def, metric.metric.simple);
      case TMetricKind::PROPERTY:
        return SimpleMetricFactory<TMetricKind::PROPERTY>(def, metric.metric.simple);
      case TMetricKind::COUNTER:
        return SimpleMetricFactory<TMetricKind::COUNTER>(def, metric.metric.simple);
      default:
        return NULL;
    }
  } else {
    // DCHECK(false);
    return NULL;
  }
}

void MetricGroup::FromThriftHelper(vector<TMetricGroup>::const_iterator it,
    MetricGroup* cur) {
  // cur is *it
  BOOST_FOREACH(const TMetric& metric, it->metrics) {
    MetricMap::const_iterator it = cur->metric_map_.find(metric.key);
    if (it == cur->metric_map_.end()) {
      Metric* m = MetricFactory(metric);
      if (m == NULL) continue;
      cur->RegisterMetric(m);
    } else {
      it->second->MergeFromThrift(metric);
    }
  }
  int32_t num_children = it->num_children;
  for (int32_t i = 0; i < num_children; ++i) {
    ++it;
    MetricGroup* child = cur->GetChildGroup(it->name);
    FromThriftHelper(it, child);
  }
}

void MetricGroup::FromThrift(const TMetricTree& thrift, MetricGroup* root) {
  FromThriftHelper(thrift.groups.begin(), root);
}

TMetricDef impala::MakeMetricDef(const std::string& key, TMetricKind::type kind,
    TUnit::type units) {
  TMetricDef ret;
  ret.__set_key(key);
  ret.__set_kind(kind);
  ret.__set_units(units);
  return ret;
}

TMetricDef impala::MakePropertyDef(const string& key) {
  return MakeMetricDef(key, TMetricKind::PROPERTY, TUnit::NONE);
}
