#pragma once

#include "dataframe.h"
#include "application.h"
#include "string.h"
#include "array.h"
#include "key.h"

class Demo : public Application {
public:

  DataFrame* df;
  DataFrame* df2;
  DataFrame* df3;
  DoubleArray* vals;

  String* m = new String("main");
  String* v = new String("verif");
  String* c = new String("ck");
  Key* main = new Key(m,0);
  Key* verify = new Key(v,0);
  Key* check = new Key(c,0);

  Demo(size_t idx, KDFMap* kv): Application(idx, kv) {
    run_();
  }

  ~Demo() {
    delete m;
    delete v;
    delete c;
    delete main;
    delete verify;
    delete check;
  }

  void run_() override {
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }

  void producer() {
    size_t SZ = 100*1000;
    vals = new DoubleArray();

    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
      vals->push_back(i);
      sum += vals->get(i);
    }

    Schema s("");
    DataFrame d(s);
    df = d.from_array(main, getKVStore(), SZ, vals);
    df2 = d.from_scalar(check, getKVStore(), sum);
  }

  void counter() {
    size_t SZ = 100*1000;
    DataFrame* v = getKVStore()->getAndWait(main);

    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
      sum += v->get_double(0,i);
    }
    p("The sum is  ").pln(sum);

    Schema s("");
    DataFrame d(s);
    df3 = d.from_scalar(verify, getKVStore(), sum);
  }

  void summarizer() {
    DataFrame* result = getKVStore()->getAndWait(verify);
    DataFrame* expected = getKVStore()->getAndWait(check);
    pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
  }
};

/** A DemoThread wraps a Thread and contains a Demo.
 *  author: armani.a@husky.neu.edu, horn.s@husky.neu.edu */
class DemoThread : public Thread {
public:

  std::thread thread_;
  Demo* d;
  size_t threadId_;

  DemoThread(size_t index, size_t cur_thread, KDFMap* kv) {
    threadId_ = cur_thread;
    d = new Demo(index, kv);
  }

  ~DemoThread() {
    delete d;
  }

};
