#pragma once

#include "object.h"
#include "map.h"

class Application : public Object {
public:
    SDFMap kv;
    size_t idx;

    /** constructor taking in the idx */
    Application(size_t i) {
        idx = i;
    }

    virtual void run() {}

    /** which node is this */
    size_t this_node() {
        return idx;
    }
};