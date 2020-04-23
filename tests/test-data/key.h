#pragma once

#include "object.h"
#include "string.h"
#include "helper.h"
#include <cstdio>

/**
  * Authors: armani.a@husky.neu.edu, horn.s@husky.neu.edu
  * Key in a key-value store.
  * Has a string value and a home node.
  * Does NOT take ownership of strings, just keeps their pointer.
  */
class Key : public Object {
public:

  String* name_;
  int homeNode_;
  size_t creator_id_;

  Key(String* name) {
    name_ = name;
    homeNode_ = -1;
    creator_id_ = 0;
  }

  Key(String* name, int homeNode) {
    name_ = name;
    homeNode_ = homeNode;
    creator_id_ = 0;
  }

  Key(String* name, size_t creator_id) {
    name_ = name;
    homeNode_ = -1;
    creator_id_ = creator_id;
  }

  ~Key() {
    delete name_;
  }

  // Returns the name of this key.
  String* getName() {
    return name_;
  }

  // Returns the home node of this key.
  int getHomeNode() {
    return homeNode_;
  }

  // Returns the creator of this key.
  size_t getCreatorID() {
    return creator_id_;
  }

  // Sets the name of this key to a new string.
  void setName(String* s) {
    assert(s != nullptr);
    name_ = s;
  }

  // Sets the home node of this key, if it needs to be changed for some reason.
  void setHomeNode(size_t n) {
    homeNode_ = n;
  }

  // Sets the creator id of this key, if it needs to be changed for some reason.
  void setCreatorID(size_t n) {
    creator_id_ = n;
  }

 // Compute the hash value of this key.
  size_t hash_me() {
        size_t hash = 0;
        hash = getName()->hash() + getHomeNode();
        return hash;
  }

  /** Compare two keys for their name and home node. */
  bool equals(Object* other) {

      if (other == this) return true;

      Key* x = dynamic_cast<Key*>(other);
      if (x == nullptr) return false;
      if (!name_->equals(x->getName())) return false;
      if (homeNode_ != x->getHomeNode()) return false;

      return true;
  }

};
