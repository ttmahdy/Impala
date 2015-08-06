#ifndef TIMER_METRIC_H
#define TIMER_METRIC_H

#include "util/metrics.h"

#include "util/stopwatch.h"

namespace impala {

class TimerMetric : public Metric {
 public:
  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* val) {
    rapidjson::Value container(rapidjson::kObjectType);
    AddStandardFields(document, &container);

    rapidjson::Value metric_value;
    ToJsonValue(stopwatch_.ElapsedTime(), TUnit::NONE, document, &metric_value);
    container.AddMember("value", metric_value, document->GetAllocator());

    // rapidjson::Value type_value(PrintTMetricKind(kind()).c_str(),
    //     document->GetAllocator());
    // container.AddMember("kind", type_value, document->GetAllocator());
    rapidjson::Value units(PrintTUnit(TUnit::TIME_NS).c_str(), document->GetAllocator());
    container.AddMember("units", units, document->GetAllocator());
    *val = container;
  }
  virtual void ToLegacyJson(rapidjson::Document* document) { }

  virtual std::string ToHumanReadable() {
    return PrettyPrinter::Print(stopwatch_.ElapsedTime(), TUnit::TIME_NS);
  }

  virtual const TMetric ToThrift() {
    TMetric ret;
    ret.__set_key(key());
    ret.__set_metric(TMetricInstance());
    ret.__set_metric_def(MakeMetricDef(key(), TMetricKind::COUNTER, TUnit::TIME_NS));
    ret.metric.__set_simple(ToTSimpleMetric(stopwatch_.ElapsedTime()));
    return ret;
  }

  TimerMetric(const TMetricDef& def) : Metric(def) { }

  void Start() { stopwatch_.Start(); }
  void Stop() { stopwatch_.Stop(); }

 private:
  MonotonicStopWatch stopwatch_;
};

struct ScopedTimerMetric {
  ScopedTimerMetric(TimerMetric* m) : metric(m) { metric->Start(); }
  ~ScopedTimerMetric() { metric->Stop(); }
  TimerMetric* metric;
};

}

#endif
