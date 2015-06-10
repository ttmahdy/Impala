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


#ifndef IMPALA_UTIL_CONTAINER_UTIL_H
#define IMPALA_UTIL_CONTAINER_UTIL_H

#include <map>
#include <unordered_map>
#include <boost/functional/hash.hpp>

#include "util/hash-util.h"
#include "gen-cpp/Types_types.h"

namespace std {

/// Hash function for TNetworkAddress. This function must be called hash_value to be
/// picked up properly by boost.
template<>
struct hash<impala::TNetworkAddress> {
 public:
  inline std::size_t operator() (const impala::TNetworkAddress& host_port) const {
    uint32_t hash =
        impala::HashUtil::Hash(host_port.hostname.c_str(), host_port.hostname.length(), 0);
    return impala::HashUtil::Hash(&host_port.port, sizeof(host_port.port), hash);
  }
};

}


namespace impala {

template<typename K, typename V>
struct PairHash {
  inline std::size_t operator() (const std::pair<K,V>& p) const {
    size_t seed = 0;
    boost::hash_combine(seed, std::hash<K>()(p.first));
    boost::hash_combine(seed, std::hash<V>()(p.second));
    return seed;
  }
};

template<typename T>
struct VectorHash {
  inline size_t operator() (const std::vector<T>& to_hash) const {
    size_t hash_value = 0;
    for (const T& s: to_hash) {
      boost::hash_combine(hash_value, s);
    }
    return hash_value;
  }
};


struct HashTNetworkAddressPtr : public std::unary_function<TNetworkAddress*, size_t> {
  size_t operator()(const TNetworkAddress* const& p) const {
    return std::hash<TNetworkAddress>()(*p);
  }
};

struct TNetworkAddressPtrEquals : public std::unary_function<TNetworkAddress*, bool> {
  bool operator()(const TNetworkAddress* const& p1,
                  const TNetworkAddress* const& p2) const {
    return p1->hostname == p2->hostname && p1->port == p2->port;
  }
};


/// FindOrInsert(): if the key is present, return the value; if the key is not present,
/// create a new entry (key, default_val) and return default_val.

template <typename K, typename V>
V* FindOrInsert(std::map<K,V>* m, const K& key, const V& default_val) {
  typename std::map<K,V>::iterator it = m->find(key);
  if (it == m->end()) {
    it = m->insert(std::make_pair(key, default_val)).first;
  }
  return &it->second;
}

template <typename K, typename V>
V* FindOrInsert(std::unordered_map<K,V>* m, const K& key, const V& default_val) {
  typename std::unordered_map<K,V>::iterator it = m->find(key);
  if (it == m->end()) {
    it = m->insert(std::make_pair(key, default_val)).first;
  }
  return &it->second;
}


/// FindWithDefault: if the key is present, return the corresponding value; if the key
/// is not present, return the supplied default value

template <typename K, typename V>
const V& FindWithDefault(const std::map<K, V>& m, const K& key, const V& default_val) {
  typename std::map<K,V>::const_iterator it = m.find(key);
  if (it == m.end()) return default_val;
  return it->second;
}

template <typename K, typename V>
const V& FindWithDefault(const std::unordered_map<K, V>& m, const K& key,
                         const V& default_val) {
  typename std::unordered_map<K,V>::const_iterator it = m.find(key);
  if (it == m.end()) return default_val;
  return it->second;
}

/// Merges (by summing) the values from two maps of values. The values must be
/// native types or support operator +=.
template<typename K, typename V>
void MergeMapValues(const std::map<K, V>& src, std::map<K, V>* dst) {
  for (typename std::map<K, V>::const_iterator src_it = src.begin();
      src_it != src.end(); ++src_it) {
    typename std::map<K, V>::iterator dst_it = dst->find(src_it->first);
    if (dst_it == dst->end()) {
      (*dst)[src_it->first] = src_it->second;
    } else {
      dst_it->second += src_it->second;
    }
  }
}

}

#endif
