#pragma once

#include "dataframe.h"
#include "application.h"
#include "string.h"
#include "array.h"

class Trivial : public Application {
public:

  String* key_int;
  String* key_double;

  Trivial(size_t idx) : Application(idx) {
    key_int = new String("int-triv");
    key_double = new String("double-triv");
    run();
  }

  void run() {
    run_int();
    printf("trivial example for IntArray complete and successful.\n\n");

    run_double();
    printf("trivial example for DoubleArray complete and successful.\n\n");
  }

  void run_int() {

    size_t SZ = 1000*1000;
    IntArray* vals = new IntArray();

    int sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
      vals->push_back(i);
      sum += vals->get(i);
    }

    Schema s("");
    DataFrame d(s);

    DataFrame* df = d.from_array(key_int, &kv, SZ, vals);

    assert(df->get_int(0,1) == 1);

    DataFrame* df2 = kv.get(key_int);
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_int(0, i);

    assert(sum == 0);

    delete vals;
    delete df;
  }

  void run_double() {
    size_t SZ = 1000*1000;
    DoubleArray* vals = new DoubleArray();

    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
      vals->push_back(i);
      sum += vals->get(i);
    }

    Schema s("");
    DataFrame d(s);

    DataFrame* df = d.from_array(key_double, &kv, SZ, vals);

    assert(df->get_double(0,1) == 1);

    DataFrame* df2 = kv.get(key_double);
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0, i);

    assert(sum == 0);

    delete vals;
    delete df;
  }
};
