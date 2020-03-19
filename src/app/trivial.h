#pragma once

#include "dataframe.h"
#include "application.h"
#include "string.h"
#include "array.h"

class Trivial : public Application {
public:

  Trivial(size_t idx) : Application(idx) {}

  void run() {
    size_t SZ = 1000*1000;
    IntArray* vals = new IntArray();

    int sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
      vals->push_back(i);
      sum += vals->get(i);
    }

    Schema s("");
    DataFrame d(s);

    String* key = new String("triv");
    DataFrame* df = d.from_array(key, &kv, SZ, vals);

    assert(df->get_int(0,1) == 1);

    DataFrame* df2 = kv.get(key);

    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_int(0, i);

    assert(sum == 0);

    delete vals;
    delete key;
    delete df; 
  }
};