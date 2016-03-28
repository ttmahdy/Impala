#include <stdio.h>
#include <stdbool.h>
#include <iostream>
#include <unistd.h>
#include "immintrin.h"
#include <avxintrin.h>


int hash32shift(int key)
{
  key = ~key + (key << 15); // key = (key << 15) - key - 1;
  key = key ^ (key >> 12);
  key = key + (key << 2);
  key = key ^ (key >> 4);
  key = key * 2057; // key = (key + (key << 3)) + (key << 11);
  key = key ^ (key >> 16);
  return key;
}


static __inline__ unsigned long GetCC(void)
{
  unsigned a, d;
  asm volatile("rdtsc" : "=a" (a), "=d" (d));
  return ((unsigned long)a) | (((unsigned long)d) << 32);
}

struct Tuple {
    int value;
};

struct Bucket {
  /// Cache of the hash for data.
  /// TODO: Do we even have to cache the hash value?
  int hash;

  /// Whether this bucket contains a vaild entry, or it is empty.
  bool filled = false;

  /// Used for full outer and right {outer, anti, semi} joins. Indicates whether the
  /// row in the bucket has been matched.
  /// From an abstraction point of view, this is an awkward place to store this
  /// information but it is efficient. This space is otherwise unused.
  bool matched;

  /// Used in case of duplicates. If true, then the bucketData union should be used as
  /// 'duplicates'.
  bool hasDuplicates;

  /// Either the data for this bucket or the linked list of duplicates.
  Tuple bucketData;
};

struct Bucket16 {
  long hash;
  long value;

};

struct Bucket24 {
  long hash;
  long value;
  long value2;
};

static void load_hash_table(Bucket *hash_table, int hash_table_size,
                          int key_count, int* key_values) {

    unsigned long cycles_before = 0;
    unsigned long cycles_after = 0;
    unsigned long cycles_consumed = 0;
    cycles_before = GetCC();

    for (int i = 0; i < key_count; i++) {
        int buckdt_id = hash32shift(key_values[i]) & (hash_table_size -1);
        hash_table[buckdt_id].filled=true;
        hash_table[buckdt_id].hasDuplicates=false;
        hash_table[buckdt_id].bucketData.value = key_values[i];
        hash_table[buckdt_id].hash = buckdt_id;
    }
    cycles_after = GetCC();
    cycles_consumed = cycles_after - cycles_before;
    std::cout << "Load keys consumed   " << cycles_consumed << " cycles, " << cycles_consumed/key_count << " per iteration"<< std::endl;
}

static void probe_hash_table(Bucket *hash_table, int hash_table_size,
                             int key_count, int* key_values, int probe_multiplyer) {

    unsigned long cycles_before = 0;
    unsigned long cycles_after = 0;
    unsigned long cycles_consumed = 0;
    cycles_before = GetCC();
    long match_count = 0;

    for (int j = 0; j < probe_multiplyer; j ++) {
        for (int i = 0; i < key_count; i++) {
            int buckdt_id = hash32shift(key_values[i]) & (hash_table_size -1);
            Bucket current_bucket = hash_table[buckdt_id];
            if(current_bucket.filled == true) {
                if (current_bucket.hash == buckdt_id) {
                    if (current_bucket.bucketData.value == key_values[i]) {
                        match_count +=1;
                    }
                }
            }
        }
    }
    match_count /=probe_multiplyer;
    std::cout << "Match count " << match_count << " out of " << key_count << std::endl;

    cycles_after = GetCC();
    cycles_consumed = cycles_after - cycles_before;
    std::cout << "Probe keys consumed   " << cycles_consumed << " cycles, " << cycles_consumed/(key_count * probe_multiplyer) << " per iteration"<< std::endl;
}


static void batched_probe_hash_table(Bucket *hash_table, int hash_table_size,
                             int key_count, int* key_values, int probe_multiplyer) {

    unsigned long cycles_before = 0;
    unsigned long cycles_after = 0;
    unsigned long cycles_consumed = 0;
    cycles_before = GetCC();
    long match_count = 0;
    int match_1 = 0 ,match_2 = 0 ,match_3 = 0 ,match_4 = 0;

    for (int j = 0; j < probe_multiplyer; j ++) {
        for (int i = 0; i < key_count; i+=4) {

            int buckdt_id_1 = hash32shift(key_values[i]) & (hash_table_size -1);
            int buckdt_id_2 = hash32shift(key_values[i+1]) & (hash_table_size -1);
            int buckdt_id_3 = hash32shift(key_values[i+2]) & (hash_table_size -1);
            int buckdt_id_4 = hash32shift(key_values[i+3]) & (hash_table_size -1);

             __builtin_prefetch (&hash_table[buckdt_id_1], 0, 3);
             __builtin_prefetch (&hash_table[buckdt_id_2], 0, 3);
             __builtin_prefetch (&hash_table[buckdt_id_3], 0, 3);
             __builtin_prefetch (&hash_table[buckdt_id_4], 0, 3);

            Bucket current_bucket_1 = hash_table[buckdt_id_1];

            /*std::cout<<"Batched keys "<<(long)&current_bucket_1<<" ";
            std::cout<<current_bucket_1.bucketData.value<<" "
                    <<current_bucket_1.filled<<" "
                    <<current_bucket_1.hasDuplicates<<" "
                    <<current_bucket_1.hash<<std::endl;*/

            if(current_bucket_1.filled == true) {
                if (current_bucket_1.hash == buckdt_id_1) {
                    if (current_bucket_1.bucketData.value == key_values[i]) {
                        match_1 +=1;
                    }
                }
            }
            Bucket current_bucket_2 = hash_table[buckdt_id_2];

            if(current_bucket_2.filled == true) {
                if (current_bucket_2.hash == buckdt_id_2) {
                    if (current_bucket_2.bucketData.value == key_values[i+1]) {
                        match_2 +=1;
                    }
                }
            }
            Bucket current_bucket_3 = hash_table[buckdt_id_3];

            if(current_bucket_3.filled == true) {
                if (current_bucket_3.hash == buckdt_id_3) {
                    if (current_bucket_3.bucketData.value == key_values[i+2]) {
                        match_3 +=1;
                    }
                }
            }
            Bucket current_bucket_4 = hash_table[buckdt_id_4];

            if(current_bucket_4.filled == true) {
                if (current_bucket_4.hash == buckdt_id_4) {
                    if (current_bucket_4.bucketData.value == key_values[i+3]) {
                        match_4 +=1;
                    }
                }
            }

            match_count = match_1 + match_2 + match_3 + match_4;
        }
    }
    match_count /=probe_multiplyer;
    std::cout << "Match count " << match_count << " out of " << key_count << std::endl;

    cycles_after = GetCC();
    cycles_consumed = cycles_after - cycles_before;
    std::cout << "Batched Probe keys consumed   " << cycles_consumed << " cycles, " << cycles_consumed/(key_count * probe_multiplyer) << " per iteration"<< std::endl;
}

__attribute__((target("avx2")))
static void simd_probe_hash_table(Bucket *hash_table, int hash_table_size,
                             int key_count, int* key_values, int probe_multiplyer) {

    unsigned long cycles_before = 0;
    unsigned long cycles_after = 0;
    unsigned long cycles_consumed = 0;
    cycles_before = GetCC();
    long match_count = 0;
    int match_1 = 0 ,match_2 = 0 ,match_3 = 0 ,match_4 = 0;
    Bucket *gathered_buckets = new Bucket[4];

    //long * mypointer;
    //*mypointer = 10;
    //std::cout << "mypointer is " << (long)mypointer << '\n';


    for (int j = 0; j < probe_multiplyer; j ++) {
        for (int i = 0; i < key_count; i+=4) {

            int buckdt_id_1 = hash32shift(key_values[i]) & (hash_table_size -1);
            int buckdt_id_2 = hash32shift(key_values[i+1]) & (hash_table_size -1);
            int buckdt_id_3 = hash32shift(key_values[i+2]) & (hash_table_size -1);
            int buckdt_id_4 = hash32shift(key_values[i+3]) & (hash_table_size -1);
            Bucket current_bucket_base_1 = hash_table[buckdt_id_1];
            Bucket current_bucket_base_2 = hash_table[buckdt_id_2];

            __m128i hash_indexes = _mm_set_epi32(buckdt_id_1 * 12/8,buckdt_id_2* 12/8,
                                                 buckdt_id_3* 12/8,buckdt_id_4* 12/8);

            void *p_gathered_buckets = (void *) gathered_buckets;
            long *p_gathered_pointers = (long *)gathered_buckets;
            /*std::cout <<"Gathered buckets before"<< std::endl;
            for (int i =0; i < 4; i++) {
                 std::cout<<p_gathered_pointers[i]<< std::endl;
            }*/

            __m256i gathered_pointers = _mm256_i32gather_epi64(
                         reinterpret_cast<long long const *>(&hash_table[0])
                    , hash_indexes, 8);

            _mm256_storeu_si256((__m256i*)p_gathered_buckets, gathered_pointers);

            /*std::cout <<"Gathered buckets after"<< std::endl;
            for (int i =0; i < 4; i++) {
                 std::cout<<p_gathered_pointers[i]<< std::endl;
            }*/

            //long base_address = ((long) gathered_buckets) - 32;
            Bucket current_bucket_1 = gathered_buckets[0];
            Bucket current_bucket_2 = gathered_buckets[1];
            Bucket current_bucket_3 = gathered_buckets[2];
            Bucket current_bucket_4 = gathered_buckets[3];

            /*Bucket* bytePtr = reinterpret_cast<Bucket*>(&gathered_buckets[0]);
            bytePtr -= 10000000;
            Bucket n_bucket = *bytePtr;
            */
            /*std::cout<<"SIMD keys base 1 "<<(long)&current_bucket_base_1<<" ";
            std::cout<<current_bucket_base_1.bucketData.value<<" "
                    <<current_bucket_base_1.filled<<" "
                    <<current_bucket_base_1.hasDuplicates<<" "
                    <<current_bucket_base_1.hash<<std::endl;
            std::cout<<"SIMD keys base 2 "<<(long)&current_bucket_base_2<<" ";
            std::cout<<current_bucket_base_2.bucketData.value<<" "
                    <<current_bucket_base_2.filled<<" "
                    <<current_bucket_base_2.hasDuplicates<<" "
                    <<current_bucket_base_2.hash<<std::endl;

            std::cout<<"SIMD keys avx a "<<(long)&n_bucket<<" ";
            std::cout<<n_bucket.bucketData.value<<" "
                    <<n_bucket.filled<<" "
                    <<n_bucket.hasDuplicates<<" "
                    <<n_bucket.hash<<std::endl;

            std::cout<<"SIMD keys avx original address "<<(long) gathered_buckets<<std::endl;
            std::cout<<"SIMD keys avx base address "<<base_address<<std::endl;
            std::cout<<"SIMD keys avx 1 "<<(long)&current_bucket_1<<" ";
            std::cout<<current_bucket_1.bucketData.value<<" "
                    <<current_bucket_1.filled<<" "
                    <<current_bucket_1.hasDuplicates<<" "
                    <<current_bucket_1.hash<<std::endl;

            std::cout<<"SIMD keys avx 2 "<<(long)&current_bucket_2<<" ";
            std::cout<<current_bucket_2.bucketData.value<<" "
                    <<current_bucket_2.filled<<" "
                    <<current_bucket_2.hasDuplicates<<" "
                    <<current_bucket_2.hash<<std::endl;
            std::cout<<"SIMD keys avx 3 "<<(long)&current_bucket_3<<" ";
            std::cout<<current_bucket_3.bucketData.value<<" "
                    <<current_bucket_3.filled<<" "
                    <<current_bucket_3.hasDuplicates<<" "
                    <<current_bucket_3.hash<<std::endl;
            std::cout<<"SIMD keys avx 4 "<<(long)&current_bucket_4<<" ";
            std::cout<<current_bucket_4.bucketData.value<<" "
                    <<current_bucket_4.filled<<" "
                    <<current_bucket_4.hasDuplicates<<" "
                    <<current_bucket_4.hash<<std::endl;*/

            if(current_bucket_1.filled == true) {
                if (current_bucket_1.hash == buckdt_id_1) {
                    if (current_bucket_1.bucketData.value == key_values[i]) {
                        match_1 +=1;
                    }
                }
            }

            if(current_bucket_2.filled == true) {
                if (current_bucket_2.hash == buckdt_id_2) {
                    if (current_bucket_2.bucketData.value == key_values[i+1]) {
                        match_2 +=1;
                    }
                }
            }

            if(current_bucket_3.filled == true) {
                if (current_bucket_3.hash == buckdt_id_3) {
                    if (current_bucket_3.bucketData.value == key_values[i+2]) {
                        match_3 +=1;
                    }
                }
            }

            if(current_bucket_4.filled == true) {
                if (current_bucket_4.hash == buckdt_id_4) {
                    if (current_bucket_4.bucketData.value == key_values[i+3]) {
                        match_4 +=1;
                    }
                }
            }

            match_count = match_1 + match_2 + match_3 + match_4;
        }
    }
    match_count /=probe_multiplyer;
    std::cout << "Match count SIMD " << match_count << " out of " << key_count << std::endl;

    cycles_after = GetCC();
    cycles_consumed = cycles_after - cycles_before;
    std::cout << "SIMD Probe keys consumed   " << cycles_consumed << " cycles, " << cycles_consumed/(key_count * probe_multiplyer) << " per iteration"<< std::endl;
}

static void linear_access_hash_table(Bucket *hash_table, int hash_table_size,
                             int key_count, int* key_values, int probe_multiplyer) {

    unsigned long cycles_before = 0;
    unsigned long cycles_after = 0;
    unsigned long cycles_consumed = 0;
    cycles_before = GetCC();
    long match_count = 0;

    for (int j = 0; j < probe_multiplyer; j ++) {
        for (int i = 0; i < hash_table_size; i++) {
            //int buckdt_id = hash32shift(key_values[i]) & (hash_table_size -1);
            Bucket current_bucket = hash_table[i];
            if(current_bucket.filled == true) {
                if (current_bucket.hash > key_values[i & (key_count-1)]) {
                    match_count +=1;
                }
            }
        }
    }
    match_count /=probe_multiplyer;
    std::cout << "Linear match count " << match_count << " out of " << key_count << std::endl;

    cycles_after = GetCC();
    cycles_consumed = cycles_after - cycles_before;
    std::cout << "Linear access consumed   " << cycles_consumed << " cycles, " << cycles_consumed/(key_count * probe_multiplyer) << " per iteration"<< std::endl;
}


static void check_fill(Bucket *hash_table, int hash_table_size) {
    unsigned long cycles_before = 0;
    unsigned long cycles_after = 0;
    unsigned long cycles_consumed = 0;
    cycles_before = GetCC();

    long fill_count = 0;
    for (int i = 0; i< hash_table_size; i++) {
        if (hash_table[i].filled) {
            fill_count++;
        }
    }

    std::cout << "Fill count " << fill_count << " out of " << hash_table_size << std::endl;

    cycles_after = GetCC();
    cycles_consumed = cycles_after - cycles_before;
    std::cout << "Check keys consumed   " << cycles_consumed << " cycles, " << cycles_consumed/hash_table_size << " per iteration"<< std::endl;
}

static void init_keys(int key_count,int *key_values) {
    unsigned long cycles_before = 0;
    unsigned long cycles_after = 0;
    unsigned long cycles_consumed = 0;
    cycles_before = GetCC();

    for (int i = 0; i < key_count; i++) {
        key_values[i] = rand() ;
    }
    cycles_after = GetCC();
    cycles_consumed = cycles_after - cycles_before;
    std::cout << "Gen keys consumed   " << cycles_consumed << " cycles, " << cycles_consumed/key_count << " per iteration"<< std::endl;
}

__attribute__((target("avx2")))
static void simd_gather_test() {
    int values[16];
    int results[16];
    int gather_results[16];
    void *p = (void *) results;
    void *p_gather_results = (void *) gather_results;
    for (int i =0; i< 16;i++) {
        values[i] = i;
    }

    __m256i hash_indexes = _mm256_setr_epi32(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7]);
     _mm256_storeu_si256((__m256i*)p, hash_indexes);
     for (int i =0; i< 8;i++) {
        // std::cout <<"Direct store test " << results[i] << std::endl;
     }

     __m256i gather_indexes = _mm256_setr_epi32(0,0,1,1,7,7,3,3);

     __m256i gathered_values = _mm256_i32gather_epi32(
             reinterpret_cast<const int*>(&values[0]), gather_indexes, 4);

     _mm256_storeu_si256((__m256i*)p_gather_results, gathered_values);
     for (int i =0; i< 8;i++) {
         std::cout <<"Gather test " << gather_results[i] << std::endl;
     }

     long long_values[16];
     long long_gather_results[16];
     void *p_long_gather_results = (void *) long_gather_results;
     for (long i =0; i< 16;i++) {
         long_values[i] = i;
     }

     __m128i gather_indexes_long = _mm_set_epi32(7,7,3,3);
     gathered_values = _mm256_i32gather_epi64(
                  reinterpret_cast<long long const *>(&long_values[0])
             , gather_indexes_long, 8);

     //__int64 const*

    _mm256_storeu_si256((__m256i*)p_long_gather_results, gathered_values);
    for (int i =0; i< 4;i++) {
        std::cout <<"Gather long test " << long_gather_results[i] << std::endl;
    }

}


__attribute__((target("avx2")))
static void simd_gather_test_16() {

    int buckets_size = 256;
    int target = 2;
    int bucket_16_target = target * sizeof(Bucket16)/sizeof(long);
    int bucket_24_target = target * sizeof(Bucket24)/sizeof(long);

    std::cout <<"bucket_16_target value for " << target <<" "<<bucket_16_target<< std::endl;
    std::cout <<"bucket_16_target value for " << target <<" "<<bucket_24_target<< std::endl;

    Bucket16 *buckets_16 = new Bucket16[buckets_size];
    Bucket24 *buckets_24 = new Bucket24[buckets_size];
    Bucket16 *buckets_16_returned;
    Bucket24 *buckets_24_returned;

    long *bucket_results_16 = new long[buckets_size];
    void *p_gather_results_16 = (void *) bucket_results_16;

    long *bucket_results_24 = new long[buckets_size];
    void *p_gather_results_24 = (void *) bucket_results_24;

    buckets_16_returned = reinterpret_cast<Bucket16  *>(&bucket_results_16[0]);
    buckets_24_returned = reinterpret_cast<Bucket24  *>(&bucket_results_24[0]);

    int values[16];
    int results[16];
    int gather_results[16];
    void *p = (void *) results;

    for (long i =0; i< buckets_size;i++) {
        buckets_16[i].hash = i;
        buckets_16[i].value = i+100;

        buckets_24[i].hash = i;
        buckets_24[i].value = i+200;
        buckets_24[i].value2 = i+300;
        //std::cout <<"buckets_16 value for " << i <<" "<<buckets_16[i].value<< std::endl;
        //std::cout <<"buckets_24 value for " << i <<" "<<buckets_24[i].value<< std::endl;
    }

     __m128i gather_indexes = _mm_setr_epi32(bucket_16_target,bucket_16_target,
                                             bucket_16_target,bucket_16_target);
     __m256i gathered_values = _mm256_i32gather_epi64(
                  reinterpret_cast<long long const *>(&buckets_16[0])
             , gather_indexes, 8);

     _mm256_storeu_si256((__m256i*)p_gather_results_16, gathered_values);
     for (long i =0; i< 4;i++) {
         std::cout <<"Gather 16 hash "<< i <<" " << buckets_16_returned[i].hash<<
                     " " << buckets_16_returned[i].value<< std::endl;
     }

     gather_indexes = _mm_setr_epi32(bucket_24_target,bucket_24_target,
                                     bucket_24_target,bucket_24_target);
     gathered_values = _mm256_i32gather_epi64(
                  reinterpret_cast<long long const *>(&buckets_24[0])
             , gather_indexes, 8);

     _mm256_storeu_si256((__m256i*)p_gather_results_24, gathered_values);
     for (long i =0; i< 4;i++) {
         std::cout <<"Gather 24 hash "<< i <<" " << buckets_24_returned[i].hash
                  <<" " << buckets_24_returned[i].value<< std::endl;
     }

     std::cout <<"Base 24 address 0: " << (long)(&buckets_24[target])<<" " <<buckets_24[target].hash<<" " <<buckets_24[target].value<< std::endl;

     std::cout <<"Gather 24 address 0: " << (long)(&buckets_24_returned[0])<<" " <<buckets_24_returned[0].hash<<" " <<buckets_24_returned[0].value<< std::endl;
     std::cout <<"Gather 24 address 1: " << (long)(&buckets_24_returned[1])<<" " <<buckets_24_returned[1].hash<<" " <<buckets_24_returned[1].value<< std::endl;
     std::cout <<"Gather 24 address 2: " << (long)(&buckets_24_returned[2])<<" " <<buckets_24_returned[2].hash<<" " <<buckets_24_returned[2].value<< std::endl;
     std::cout <<"Gather 24 address 3: " << (long)(&buckets_24_returned[3])<<" " <<buckets_24_returned[3].hash<<" " <<buckets_24_returned[3].value<< std::endl;

     /*
     long long_values[16];
     long long_gather_results[16];
     void *p_long_gather_results = (void *) long_gather_results;
     for (long i =0; i< 16;i++) {
         long_values[i] = i;
     }

     __m128i gather_indexes_long = _mm_set_epi32(3,3,7,7);
     gathered_values = _mm256_i32gather_epi64(
                  reinterpret_cast<long long const *>(&long_values[0])
             , gather_indexes_long, 8);

     //__int64 const*

    _mm256_storeu_si256((__m256i*)p_long_gather_results, gathered_values);
    for (int i =0; i< 4;i++) {
        std::cout <<"Gather long test " << long_gather_results[i] << std::endl;
    }*/
}


int main(int argc, char *argv[])
{
    int size = atoi(argv[1]);
    int sleep_time =atoi(argv[2]);
    usleep(sleep_time);
    int hash_table_size = size * 1024 * 1024;
    int key_count = 0.7 * hash_table_size ;
    int probe_multiplyer = 4;
    Bucket *bp;

    simd_gather_test_16();
    simd_gather_test();
    if (1 == 10) {
        return 1;
    }

    std::cout << "Bucket size "<<sizeof(Bucket)<< std::endl;
    std::cout << "Bucket pointer size "<<sizeof(bp)<< std::endl;
    std::cout << "Hash table buckets " << hash_table_size << std::endl;
    std::cout << "Key count " << key_count << std::endl;

    int* key_values = new int[key_count];
    //Bucket *hash_table = (Bucket *)aligned_alloc(1024, hash_table_size*sizeof(Bucket));
    Bucket *hash_table = new Bucket[hash_table_size];
    int keys_size = sizeof(int) * key_count / 1024/1024;
    int hash_table_size_mb = sizeof(Bucket) * hash_table_size / 1024 /1024;

    std::cout << "Hash table size is " << hash_table_size_mb<<" MB"<< std::endl;
    std::cout << "Keys size is " << keys_size<<" MB"<< std::endl;

    init_keys(key_count, key_values);
    load_hash_table(hash_table, hash_table_size, key_count, key_values);
    check_fill(hash_table, hash_table_size);
    probe_hash_table(hash_table, hash_table_size,key_count, key_values, probe_multiplyer);
    batched_probe_hash_table(hash_table, hash_table_size,key_count, key_values, probe_multiplyer);
    linear_access_hash_table(hash_table, hash_table_size,key_count, key_values, probe_multiplyer);
    simd_probe_hash_table(hash_table, hash_table_size,key_count, key_values, probe_multiplyer);
    check_fill(hash_table, hash_table_size);

    //simd_gather_test();
    delete hash_table;
    delete key_values;
    return 1;
}


