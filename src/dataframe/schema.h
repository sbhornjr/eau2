// lang::CwC

#pragma once
#include "string.h"
#include "column.h"
#include <string.h>

/** @designers: vitekj@me.com, course staff */

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Schema : public Object {
 public:

  size_t numrows_;          // the num of rows in the df
  size_t numcols_;          // the num of cols in the schema
  String* types_;           // String representing the types of the schema
  StringColumn* colnames_;  // the names of the cols (1:1 idx mapping)
  StringColumn* rownames_;  // the names or the rows (1:1 idx mapping)
 
  /** Copying constructor.
   *  Rows are not carried over.
   */
  Schema(Schema& from) {
    // initialize values
    numrows_ = 0;
    numcols_ = from.numcols_;
    types_ = new String(*from.types_);
    colnames_ = new StringColumn();
    
    // populate colnames so that column names are carried over
    for (size_t i = 0; i < numcols_; ++i) {
      colnames_->push_back(from.colnames_->get(i));
    }
    rownames_ = new StringColumn();
  }
 
  /** Create an empty schema **/
  Schema() {
    numrows_ = 0;
    numcols_ = 0;
    types_ = new String("");
    colnames_ = new StringColumn();
    rownames_ = new StringColumn();
  }
 
  /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
  Schema(const char* types) {
    // make sure every type is int, bool, float, or string
    for (size_t i = 0; types[i] != 0; ++i) {
      assert(types[i] == 'I' || types[i] == 'B' 
      || types[i] == 'F' || types[i] == 'S');
    }
    // initialize values
    numrows_ = 0;
    types_ = new String(types);
    numcols_ = types_->size();
    colnames_ = new StringColumn();

    // initialize colnames with empty strings to ensure 1:1 mapping
    for (size_t i = 0; i < numcols_; ++i) {
      colnames_->push_back(new String(""));
    }
    rownames_ = new StringColumn();
  }

  // destructor - delete types and name columns
  ~Schema() {
    delete types_;
    delete colnames_;
    delete rownames_;
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
 
  /** Add a column of the given type and name (can be nullptr), name
    * is external. */
  void add_column(char typ, String* name) {
    // make sure the type is valid
    assert(typ == 'I' || typ == 'B' || typ == 'F' || typ == 'S');
    colnames_->push_back(name);
    ++numcols_;
    
    // append the typ onto our types string
    size_t len = types_->size();
    char* types = types_->c_str();
    types[len++] = typ;
    types[len] = '\0';
    String* newtypes = new String(types);
    delete types_;
    types_ = newtypes;
  }
 
  /** Add a row with a name (possibly nullptr), name is external. */
  void add_row(String* name) {
    ++numrows_;
    rownames_->push_back(name);
  }
 
  /** Return name of row at idx; nullptr indicates no name. An idx >= width
    * is undefined. */
  String* row_name(size_t idx) {
    return rownames_->get(idx);
  }
 
  /** Return name of column at idx; nullptr indicates no name given.
    *  An idx >= width is undefined.*/
  String* col_name(size_t idx) {
    return colnames_->get(idx);
  }
 
  /** Return type of column at idx. An idx >= width is undefined. */
  char col_type(size_t idx) {
    return types_->at(idx);
  }

  /** Set the name of the column at idx. */
  void set_col_name(size_t idx, String* name) {
    colnames_->set(idx, name);
  }

  /** Set the name of the row at idx. */
  void set_row_name(size_t idx, String* name) {
    rownames_->set(idx, name);
  }
 
  /** Given a column name return its index, or -1. */
  int col_idx(const char* name) {
    for (size_t i = 0; i < numcols_; ++i) {
      String* n = colnames_->get(i);
      if (strcmp(name, n->c_str()) == 0) {
        return i;
      }
    }
    return -1;
  }
 
  /** Given a row name return its index, or -1. */
  int row_idx(const char* name) {
    for (size_t i = 0; i < numrows_; ++i) {
      String* n = rownames_->get(i);
      if (strcmp(name, n->c_str()) == 0) {
        return i;
      }
    }
    return -1;
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