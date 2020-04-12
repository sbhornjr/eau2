#pragma once

#include "dataframe.h"
#include "application.h"
#include "string.h"
#include "array.h"

class Trivial : public Application {
public:

  Key* key_int;
  Key* key_double;
  DataFrame* d;

  Trivial(size_t idx, KVStore* kv) : Application(idx, kv) {
    cout << "a" << endl;
    String* s1 = new String("int-triv");
    String* s2 = new String("double-triv");
    key_int = new Key(s1, 0);
    key_double = new Key(s2, 0);
    Schema s("");
    d = new DataFrame(s, kv_);
    cout << "b" << endl;
    run_();
  }

  ~Trivial() {
    delete key_int;
    delete key_double;
    delete d;
  }

  void run_() {
    cout << "c" << endl;
    run_int();
    printf("Trivial example for IntArray complete and successful.\n");
    cout << "d" << endl;
    run_double();
    printf("Trivial example for DoubleArray complete and successful.\n\n");
  }

  void run_int() {
    size_t SZ = 1000*1000;
    IntArray* vals = new IntArray();

    int sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
      vals->push_back(i);
      sum += vals->get(i);
    }

    cout << "e" << endl;

    DataFrame* df = d->from_array(key_int, getKVStore(), SZ, vals);

    cout << "f" << endl;

    assert(df->get_int(0,1) == 1);

    cout << "g" << endl;

    DataFrame* df2 = d->get_dataframe(getKVStore()->get(key_int));
    cout << "j" << endl;
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_int(0, i);

    cout << "h" << endl;

    assert(sum == 0);

    delete vals;
    delete df;
    delete df2;
    cout << "i" << endl;
  }

  void run_double() {
    size_t SZ = 1000*1000;
    DoubleArray* vals = new DoubleArray();

    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
      vals->push_back(i);
      sum += vals->get(i);
    }

    DataFrame* df = d->from_array(key_double, getKVStore(), SZ, vals);

    assert(df->get_double(0,1) == 1);

    DataFrame* df2 = d->get_dataframe(getKVStore()->get(key_double));
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0, i);

    assert(sum == 0);

    delete vals;
    delete df;
    delete df2;
  }
};
