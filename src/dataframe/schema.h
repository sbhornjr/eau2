// lang::CwC

#pragma once
#include "string.h"
#include "column.h"
#include <string.h>

/** @designers: vitekj@me.com, course staff */

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'D'.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Schema : public Object {
 public:

  size_t numrows_;          // the num of rows in the df
  size_t numcols_;          // the num of cols in the schema
  String* types_;           // String representing the types of the schema

  /** Copying constructor.
   *  Rows are not carried over.
   */
  Schema(Schema& from) {
    // initialize values
    numrows_ = 0;
    numcols_ = from.numcols_;
    types_ = new String(*from.types_);
  }

  /** Create an empty schema **/
  Schema() {
    numrows_ = 0;
    numcols_ = 0;
    types_ = new String("");
  }

  /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
  Schema(const char* types) {
    // make sure every type is int, bool, double, or string
    for (size_t i = 0; types[i] != 0; ++i) {
      assert(types[i] == 'I' || types[i] == 'B'
      || types[i] == 'D' || types[i] == 'S');
    }
    // initialize values
    numrows_ = 0;
    types_ = new String(types);
    numcols_ = types_->size();
  }

  // destructor - delete types and name columns
  ~Schema() {
    delete types_;
  }

  /**
   * tests equality on an object.
   * Schemas are equal if they have the same types.
   * @param other: object to test equality against
   * @returns true if objects are equal
   */
  bool equals(Object* other) {
    if (other == nullptr) return false;
    Schema* tgt = dynamic_cast<Schema*>(other);
    if (tgt == nullptr) return false;
    return types_->equals(types_);
  }

  /**
   * hash function. hash is determined by types.
   * @returns hash value of this schema
   */
  size_t hash() {
    size_t hash = 0;
    hash += numcols_;
    hash += types_->hash_me();
    return hash;
  }

  /** Add a column of the given type */
  void add_column(char typ) {
    // make sure the type is valid
    assert(typ == 'I' || typ == 'B' || typ == 'D' || typ == 'S');
    ++numcols_;

    String* newtypes;
    if (types_->size() == 0) {
      // Empty schemas must be treated differently.
      // Replace empty string rather than append to avoid memory issues.
      char oneChar[1];
      oneChar[0] = typ;
      newtypes = new String(oneChar, 1);
    } else {
      // Append the typ onto our types string.
      size_t len = types_->size();
      char* types = types_->c_str();
      types[len++] = typ;
      types[len] = '\0';
      newtypes = new String(types);
    }
    delete types_;
    types_ = newtypes;
  }

  /** Add a row */
  void add_row() {
    ++numrows_;
  }

  /** Return type of column at idx. An idx >= width is undefined. */
  char col_type(size_t idx) {
    return types_->at(idx);
  }

  /** The number of columns */
  size_t width() {
    return numcols_;
  }

  /** The number of rows */
  size_t length() {
    return numrows_;
  }
};
