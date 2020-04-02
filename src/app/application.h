#pragma once

#include "object.h"
#include "map.h"

/**
  * Extendable class that represents the application as a whole.
  * Each "Application" has an index which says what kind of node it is.
  * Example applications can be seen in demo.h and trivial.h files.
  *
  * @authors: armani.a@husky.neu.edu, horn.s@husky.neu.edu
  */
class Application : public Object {
public:
    KSMap* kv_;
    KChunkMap* kc_;
    size_t idx;

    Application(size_t i, KSMap* kv, KChunkMap* kc) {
      idx = i;
      kv_ = kv;
      kc_ = kc;
    }

    virtual void run_() {}

    /** which node is this */
    size_t this_node() {
        return idx;
    }

    /** getter for the KV store that this application is using **/
    KSMap* getKVStore() {
      return kv_;
    }
};
