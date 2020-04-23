#pragma once

#include "object.h"
#include "kvstore.h"

/**
  * Extendable class that represents the application as a whole.
  * Each "Application" has an index which says what kind of node it is.
  * Example applications can be seen in demo.h and trivial.h files.
  *
  * @authors: armani.a@husky.neu.edu, horn.s@husky.neu.edu
  */
class Application : public Object {
public:
    KVStore* kv_;
    size_t idx_;

    Application(size_t i, KVStore* kv) {
      idx_ = i;
      kv_ = kv;
    }

    virtual void run_() {}

    /** which node is this */
    size_t this_node() {
        return idx_;
    }

    /** getter for the KV store that this application is using **/
    KVStore* getKVStore() {
      return kv_;
    }
};
