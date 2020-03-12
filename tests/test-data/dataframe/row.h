// lang::CwC

#pragma once
#include "schema.h"

/** @designers: vitekj@me.com, course staff */

/*****************************************************************************
 * Fielder::
 * A field visitor invoked by Row.
 * all methods are pure virtual.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Fielder : public Object {
public:

  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) = 0;

  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) = 0;

  virtual void accept(float f) = 0;

  virtual void accept(int i) = 0;

  virtual void accept(String* s) = 0;

  /** Called when all fields have been seen. */
  virtual void done() = 0;
};

/**
 * Fielder used for printing the dataframe as a sorer file.
 * Prints each individual value as a sorer field.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class SorerFielder : public Fielder {
public:

  size_t idx_;
  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) {
    idx_ = r;
  }

  /** Called for fields of the argument's type with the value of the field.
   * print the given value as a sorer field (<value>). */
  virtual void accept(bool b) {
    printf("<%i> ", b);
  }
  virtual void accept(float f) {
    printf("<%f> ", f);
  }
  virtual void accept(int i) {
    printf("<%i> ", i);
  }
  virtual void accept(String* s) {
    printf("<%s> ", s->c_str());
  }

  /** Called when all fields have been seen. */
  virtual void done() {
    printf("\n");
  };
};

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Row : public Object {
public:

  // the values of this array as a list of columns.
  // each column is of size 1, idx 0 contains the row's val at that col.
  Column** columns_;

  Schema* schema_;    // the schema this row conforms to
  size_t size_;       // the number of fields in the row
  size_t curIdx_;     // the idx of the DF this row represents

  /** Build a row following a schema. */
  Row(Schema& scm) {
    // initialize values
    schema_ = new Schema(scm);
    size_ = schema_->width();
    curIdx_ = 0;
    columns_ = new Column*[schema_->width()];

    // for loop to populate columns_ with columns of correct types
    for (size_t i = 0; i < schema_->width(); ++i) {
      char type = schema_->col_type(i);
      Column* c;
      if (type == 'I') {
        c = new IntColumn();
      } else if (type == 'B') {
        c = new BoolColumn();
      } else if (type == 'F') {
        c = new FloatColumn();
      } else if (type == 'S') {
        c = new StringColumn();
      } else {
        assert(false);
      }
      columns_[i] = c;
    }
  }

  // destructor - delete schema, columns_, and cols in columns_
  ~Row() {
    delete schema_;
    for (size_t i = 0; i < size_; ++i) {
      delete columns_[i];
    }
    delete[] columns_;
  }

  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined.
    * Make sure the value type matches up with the given col,
    * if we already have columns of size 1 then replace the 0th idx,
    * else push back the given value.
    */
  void set(size_t col, int val) {
    assert(schema_->col_type(col) == 'I');
    IntColumn* c = dynamic_cast<IntColumn*>(columns_[col]);
    if (c->size() == 0) {
      c->push_back(val);
    } else {
      c->set(0, val);
    }
  }

  void set(size_t col, float val) {
    assert(schema_->col_type(col) == 'F');
    FloatColumn* c = dynamic_cast<FloatColumn*>(columns_[col]);
    if (c->size() == 0) {
      c->push_back(val);
    } else {
      c->set(0, val);
    }
  };

  void set(size_t col, bool val) {
    assert(schema_->col_type(col) == 'B');
    BoolColumn* c = dynamic_cast<BoolColumn*>(columns_[col]);
    if (c->size() == 0) {
      c->push_back(val);
    } else {
      c->set(0, val);
    }
  }
  /** Acquire ownership of the string. */
  void set(size_t col, String* val) {
    assert(schema_->col_type(col) == 'S');
    StringColumn* c = dynamic_cast<StringColumn*>(columns_[col]);
    if (c->size() == 0) {
      c->push_back(val);
    } else {
      c->set(0, val);
    }
  }

  /** Set/get the index of this row (ie. its position in the dataframe. This is
   *  only used for informational purposes, unused otherwise */
  void set_idx(size_t idx) {
    curIdx_ = idx;
  }

  size_t get_idx() {
    return curIdx_;
  }

  /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined.
    * Make sure column is of given type,
    * then return the value from that column in this row.
    */
  int get_int(size_t col){
    assert(schema_->col_type(col) == 'I');
    IntColumn* ic = columns_[col]->as_int();
    return ic->get(0);
  }
  bool get_bool(size_t col){
    assert(schema_->col_type(col) == 'B');
    BoolColumn* bc = columns_[col]->as_bool();
    return bc->get(0);
  }
  float get_float(size_t col){
    assert(schema_->col_type(col) == 'F');
    FloatColumn* fc = columns_[col]->as_float();
    return fc->get(0);
  }
  String* get_string(size_t col){
    assert(schema_->col_type(col) == 'S');
    StringColumn* sc = columns_[col]->as_string();
    return sc->get(0);
  }

  /** Number of fields in the row. */
  size_t width() {
    return schema_->width();
  };

  /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx) {
    assert(idx < size_);
    return schema_->col_type(idx);
  };

  /** Given a Fielder, visit every field of this row.
    * Calling this method before the row's fields have been set is undefined. */
  void visit(size_t idx, Fielder& f) {
    assert(columns_ != nullptr);

    // for loop - for each value, call the given fielder's accept method
    for (size_t i = 0; i < size_; ++i) {
      char type = schema_->col_type(i);
      if (type == 'I') {
        IntColumn* ic = columns_[i]->as_int();
        f.accept(ic->get(0));
      } else if (type == 'B') {
        BoolColumn* bc = columns_[i]->as_bool();
        f.accept(bc->get(0));
      } else if (type == 'F') {
        FloatColumn* fc = columns_[i]->as_float();
        f.accept(fc->get(0));
      } else if (type == 'S') {
        StringColumn* sc = columns_[i]->as_string();
        f.accept(sc->get(0));
      } else {
        assert(false);
      }
    }
  }
};

/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 * methods are pure virtual.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Rower : public Object {
 public:
  /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. The return value is used in filters to indicate that a row
      should be kept. */
  virtual bool accept(Row& r) = 0;

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  virtual void join_delete(Rower* other) = 0;

  /** Returns a clone of this rower */
  virtual Rower* clone () = 0;

};

/**
 * Rower used to print each value of the row in sorer notation.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Print : public Rower {
public:

  // simply call visit on the row with a sorer fielder
  // return 0 because no values were changed
  bool accept(Row& r) {
    SorerFielder f;
    f.start(0);
    r.visit(0, f);
    f.done();
    return 1;
  }

  /**
    * Simply deletes other rower.
    * No fields to save or update.
    */
  void join_delete(Rower* other) {
    delete other;
  }

  /** Returns a clone of this rower */
  Rower* clone () {
    return new Print(*this);
  }
};

/**
 * TEST ROWER
 * Rower used to filter a DF where the second column is a boolean,
 * to keep each row in which that column's bool is true.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class BoolRower : public Rower {
public:

  // make sure the second col is a bool, return true if the bool is true
  bool accept(Row& r) {
    // second col of df is bool; we want all rows where that is true
    assert(r.schema_->col_type(1) == 'B');
    return r.columns_[1]->as_bool()->get(0);
  }


  /**
    * Simply deletes other rower.
    * No fields to save or update.
    */
  void join_delete(Rower* other) {
    delete other;
  }

  /** Returns a clone of this rower */
  Rower* clone () {
    return new BoolRower(*this);
  }
};

/**
 * TEST ROWER
 * Rower used to add 1 to every int and float in a DF.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class PlusOneRower : public Rower {
public:

  // for each value in the row, if it is a int/float, add 1 to it.
  // return true because values were changed.
  bool accept(Row& r) {
    for (size_t i = 0; i < r.width(); ++i) {
      if (r.col_type(i) == 'I') r.set(i, r.get_int(i) + 1);
      else if (r.col_type(i) == 'F') r.set(i, (float)(r.get_float(i) + 1.0));
    }
    return true;
  }

  void join_delete(Rower* other) {
    delete other;
  }


  /**
  * Simply deletes other rower.
  * No fields to save or update.
  */
  Rower* clone() {
    return new PlusOneRower(*this);
  }
};


/** Sums the integer values in the rows.
  * @authors armani.a@husky.neu.edu, horn.s@husky.neu.edu
  */
class SumRower: public Rower {
public:

  int sum_;

  SumRower() {
    sum_ = 0;
  }

  /** Returns the sum value */
  int getSum() {
    return sum_;
  }

  bool accept(Row& r) {
    for (size_t i = 0; i < r.width(); ++i) {
      if (r.col_type(i) == 'I') {
        sum_ += r.get_int(i);
      }
    }
    return true;
  }

  /** Adds the sum of the other rower to this sum and deletes it. */
  void join_delete(Rower* other) {
    SumRower* s = dynamic_cast<SumRower*>(other);
    sum_ += s->getSum();
    delete s;
  }

  /** Returns a clone of this rower */
  Rower* clone () {
    return new SumRower(*this);
  }

};

/** Reverses the string values.
  * "ABC" -> "CBA"
  * @authors armani.a@husky.neu.edu, horn.s@husky.neu.edu
  */
class ReverseRower: public Rower {
public:

  ReverseRower() {}

  /** Reverse any strings in the dataframe and set to new values. */
  bool accept(Row& r) {
    for (size_t i = 0; i < r.width(); ++i) {
      if (r.col_type(i) == 'S') {
        String* old = r.get_string(i);
        String* newString = old->reverse();
        r.set(i, newString);
        delete newString;
      }
    }
    return true;
  }

  /** No fields to manage, so deletes other rower. */
  void join_delete(Rower* other) {
    delete other;
  }

  /** Returns a clone of this rower */
  Rower* clone () {
    return new ReverseRower(*this);
  }

};
