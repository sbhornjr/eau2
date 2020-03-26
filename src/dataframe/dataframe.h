// lang::CwC

#pragma once
#include "column.h"
#include "row.h"
#include "schema.h"
#include "helper.h"

#include <cstdlib>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <sstream>
#include <atomic>
#include "object.h"
#include "string.h"
#include "array.h"
#include "map.h"

/** A Thread wraps the thread operations in the standard library.
 *  author: vitekj@me.com */
class Thread : public Object {
public:
    std::thread thread_;

    /** Starts running the thread, invoked the run() method. */
    void start() { thread_ = std::thread([this]{ this->run(); }); }

    /** Wait on this thread to terminate. */
    void join() { thread_.join(); }

    /** Yield execution to another thread. */
    static void yield() { std::this_thread::yield(); }

    /** Sleep for millis milliseconds. */
    static void sleep(size_t millis) {
       std::this_thread::sleep_for(std::chrono::milliseconds(millis));
    }

    /** Subclass responsibility, the body of the run method */
    virtual void run() { assert(false); }

    // there's a better way to get an CwC value out of a threadid, but this'll do for now
    /** Return the id of the current thread */
    static String * thread_id() {
        std::stringstream buf;
        buf << std::this_thread::get_id();
        std::string buffer(buf.str());
        return new String(buffer.c_str(), buffer.size());
    }
};

/** A convenient lock and condition variable wrapper. */
class Lock : public Object {
public:
    std::mutex mtx_;
    std::condition_variable_any cv_;

    /** Request ownership of this lock.
     *
     *  Note: This operation will block the current thread until the lock can
     *  be acquired.
     */
    void lock() { mtx_.lock(); }

    /** Release this lock (relinquish ownership). */
    void unlock() { mtx_.unlock(); }

    /** Sleep and wait for a notification on this lock.
     *
     *  Note: After waking up, the lock is owned by the current thread and
     *  needs released by an explicit invocation of unlock().
     */
    void wait() { cv_.wait(mtx_); }

    // Notify all threads waiting on this lock
    void notify_all() { cv_.notify_all(); }
};

/** A simple thread-safe counter. */
class Counter : public Object {
public:
    std::atomic<size_t> next_;

    Counter() { next_ = 0; }

    size_t next() {
        size_t r = next_++;
        return r;
    }
    size_t prev() {
        size_t r = next_--;
        return r;
    }

    size_t current() { return next_;  }
};

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public Object {
 public:

  Column** cols_;   // column array representing the values of the DF
  Schema* schema_;  // the schema this DF conforms to

  /** Create a data frame with the same columns as the give df but no rows */
  DataFrame(DataFrame& df): DataFrame(*df.schema_) {}

  /** Create a data frame from a schema and columns. Results are undefined if
    * the columns do not match the schema. */
  DataFrame(Schema& schema) {
    schema_ = new Schema(schema);
    cols_ = new Column*[schema_->width()];

    // populates cols_ with empty columns
    for (size_t i = 0; i < schema_->width(); ++i) {
      Column* c = get_new_col_(schema_->col_type(i));
      cols_[i] = c;
    }
  }

  // destructor - delete schema, cols_, and its cols
  ~DataFrame() {
    for (size_t i = 0; i < schema_->width(); ++i) {
      cols_[i]->delete_all();
      delete cols_[i];
    }
    delete[] cols_;
    delete schema_;
  }

  /**
   *  create and return a df of 1 col with the values in from of size sz,
   *  and make it the value of the given key in the kvstore */
  DataFrame* from_array(Key* key, KDFMap* kv, size_t sz, Array* from) {
    Schema scm("");
    DataFrame* df = new DataFrame(scm);
    Column* c = get_new_col_(from->get_type());
    if (from->as_int() != nullptr) {
      IntArray* ia = from->as_int();
      IntColumn* ic = c->as_int();
      for (size_t i = 0; i < sz; ++i) {
        ic->push_back(ia->get(i));
      }
      df->schema_->add_column('I');
    } else if (from->as_bool() != nullptr) {
      BoolArray* ba = from->as_bool();
      BoolColumn* bc = c->as_bool();
      for (size_t i = 0; i < sz; ++i) {
        bc->push_back(ba->get(i));
      }
      df->schema_->add_column('B');
    } else if (from->as_double() != nullptr) {
      DoubleArray* da = from->as_double();
      DoubleColumn* dc = c->as_double();
      for (size_t i = 0; i < sz; ++i) {
        dc->push_back(da->get(i));
      }
      df->schema_->add_column('D');
    } else if (from->as_string() != nullptr) {
      StringArray* sa = from->as_string();
      StringColumn* sc = c->as_string();
      for (size_t i = 0; i < sz; ++i) {
        sc->push_back(sa->get(i));
      }
      df->schema_->add_column('S');
    } else {
      printf("creating DF from array: unknown array type, not creating DF");
      return nullptr;
    }

    df->schema_->numrows_ = sz;

    Column** cols = new Column*[1];
    cols[0] = c;
    delete[] df->cols_;
    df->cols_ = cols;

    kv->put(key, df);
    return df;
  }

  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() {
    return *schema_;
  }

  /** Adds a column this dataframe, updates the schema, the new column
    * is external, and appears as the last column of the dataframe.
    * A nullptr column is undefined. */
  void add_column(Column* col) {
    assert(col != nullptr);
    // for however many rows there are that the schema doesn't have,
    // add rows to the schema.
    for (size_t i = schema_->length(); i < col->size(); ++i) {
      schema_->add_row();
    }
    // create new column of given column's type to copy over values
    char type = col->get_type();
    Column* c = get_new_col_(type);

    // for loop to copy values from col into new column
    for (size_t i = 0; i < col->size(); ++i) {
      if (type == 'I') {
        IntColumn* ic = col->as_int();
        c->push_back(ic->get(i));
      } else if (type == 'B') {
        BoolColumn* bc = col->as_bool();
        c->push_back(bc->get(i));
      } else if (type == 'D') {
        DoubleColumn* fc = col->as_double();
        c->push_back(fc->get(i));
      } else if (type == 'S') {
        StringColumn* sc = col->as_string();
        c->push_back(sc->get(i));
      }
    }
    // add col to schema
    schema_->add_column(type);

    // add the new column into cols_ by copying values into a new column**
    Column** tmp = cols_;
    cols_ = new Column*[schema_->width()];
    for (size_t i = 0; i < schema_->width() - 1; ++i) {
      cols_[i] = tmp[i];
    }
    cols_[schema_->width()] = c;
    delete[] tmp;
  }

  // gets a new column of the given type
  Column* get_new_col_(char type) {
      Column* c;
      if (type == 'I') {
        c = new IntColumn();
      } else if (type == 'B') {
        c = new BoolColumn();
      } else if (type == 'D') {
        c = new DoubleColumn();
      } else if (type == 'S') {
        c = new StringColumn();
      } else {
        std::cout << type << std::endl;
        assert(false);
      }
      return c;
  }

  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row) {
    IntColumn* ic = cols_[col]->as_int();
    assert(ic != nullptr);
    return ic->get(row);
  }

  bool get_bool(size_t col, size_t row) {
    BoolColumn* bc = cols_[col]->as_bool();
    assert(bc != nullptr);
    return bc->get(row);
  }

  double get_double(size_t col, size_t row) {
    DoubleColumn* dc = cols_[col]->as_double();
    assert(dc != nullptr);
    return dc->get(row);
  }

  String* get_string(size_t col, size_t row) {
    StringColumn* sc = cols_[col]->as_string();
    assert(sc != nullptr);
    return sc->get(row);
  }

  /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. */
  void set(size_t col, size_t row, int val) {
    IntColumn* ic = cols_[col]->as_int();
    assert(ic != nullptr);
    ic->set(row, val);
  }

  void set(size_t col, size_t row, bool val) {
    BoolColumn* bc = cols_[col]->as_bool();
    assert(bc != nullptr);
    bc->set(row, val);
  }

  void set(size_t col, size_t row, double val) {
    DoubleColumn* dc = cols_[col]->as_double();
    assert(dc != nullptr);
    dc->set(row, val);
  }

  void set(size_t col, size_t row, String* val) {
    StringColumn* sc = cols_[col]->as_string();
    assert(sc != nullptr);
    sc->set(row, val);
  }

  /** Set the fields of the given row object with values from the columns at
    * the given offset.  If the row is not form the same schema as the
    * dataframe, results are undefined.
    */
  void fill_row(size_t idx, Row& row) {
    assert(row.schema_->equals(schema_));
    for (size_t i = 0; i < schema_->width(); ++i) {
      char typ = schema_->col_type(i);
      if (typ == 'I') {
        IntColumn* ic = cols_[i]->as_int();
        row.set(i, ic->get(idx));
      } else if (typ == 'B') {
        BoolColumn* bc = cols_[i]->as_bool();
        row.set(i, bc->get(idx));
      } else if (typ == 'D') {
        DoubleColumn* dc = cols_[i]->as_double();
        row.set(i, dc->get(idx));
      } else {
        StringColumn* sc = cols_[i]->as_string();
        row.set(i, sc->get(idx));
      }
    }
    row.set_idx(idx);
  }

  /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undedined.  */
  void add_row(Row& row) {
    assert(row.schema_->equals(schema_));
    schema_->add_row();
    for (size_t i = 0; i < schema_->width(); ++i) {
      char typ = schema_->col_type(i);
      if (typ == 'I') {
        IntColumn* ic = cols_[i]->as_int();
        ic->push_back(row.get_int(i));
      } else if (typ == 'B') {
        BoolColumn* bc = cols_[i]->as_bool();
        bc->push_back(row.get_bool(i));
      } else if (typ == 'D') {
        DoubleColumn* dc = cols_[i]->as_double();
        dc->push_back(row.get_double(i));
      } else {
        StringColumn* sc = cols_[i]->as_string();
        sc->push_back(row.get_string(i));
      }
    }
  }

  /**
   * Use the row's internal idx to change the DF's values at that row
   * to this row's value. */
  void update_row(Row& row) {
    size_t idx = row.get_idx();

    // for loop to update col values
    for (size_t i = 0; i < schema_->width(); ++i) {
      char typ = schema_->col_type(i);
      if (typ == 'I') {
        IntColumn* ic = cols_[i]->as_int();
        ic->set(idx, row.get_int(i));
      } else if (typ == 'B') {
        BoolColumn* bc = cols_[i]->as_bool();
        bc->set(idx, row.get_bool(i));
      } else if (typ == 'D') {
        DoubleColumn* dc = cols_[i]->as_double();
        dc->set(idx, row.get_double(i));
      } else {
        StringColumn* sc = cols_[i]->as_string();
        sc->set(idx, row.get_string(i));
      }
    }
  }

  /** The number of rows in the dataframe. */
  size_t nrows() {
    return schema_->length();
  }

  /** The number of columns in the dataframe.*/
  size_t ncols() {
    return schema_->width();
  }

  /** Visit rows in order */
  void map(Rower& r) {
    Row* row = new Row(*schema_);
    for (size_t i = 0; i < schema_->length(); ++i) {
      fill_row(i, *row);
      if (r.accept(*row)) update_row(*row);
    }
    delete row;
  }

  /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
  DataFrame* filter(Rower& r) {
    Row* row = new Row(*schema_);
    DataFrame* df = new DataFrame(*schema_);
    for (size_t i = 0; i < schema_->length(); ++i) {
      fill_row(i, *row);
      if (r.accept(*row)) df->add_row(*row);
    }
    delete row;
    return df;
  }

  /** Print the dataframe in SoR format to standard output. */
  void print() {
    Print* p = new Print();
    map(*p);
    delete p;
  }

  /** This method clones the rower and executes the map in parallel. Join is
   *  used at the end to merge the results. */
   void pmap(Rower& r) {

     Rower* clone1 = r.clone();
     Rower* clone2 = r.clone();
     DFThread* t = new DFThread(this, clone1, 0, schema_);
     DFThread* t2 = new DFThread(this, clone2, 1, schema_);

     t->start();
     t2->start();

     t->join();
     t2->join();

     clone1->join_delete(clone2);
     r.join_delete(clone1);

     delete t;
     delete t2;

   }

  /** A DFThread wraps a Thread and contains a Rower.
   *  author: armani.a@husky.neu.edu, horn.s@husky.neu.edu */
  class DFThread : public Thread {
  public:

    std::thread thread_;
    Rower* r_;
    DataFrame* df_;
    size_t threadId_;
    Schema* schema_;

    DFThread(DataFrame* df, Rower* rower, size_t cur_thread, Schema * schema) {
      r_ = rower;
      threadId_ = cur_thread;
      schema_ = schema;
      df_ = df;
    }

    /** Calls accept on correct rows based on thread. */
    void run() {
      Row* row = new Row(*schema_);

      size_t endIndex, startIndex;
      if (threadId_ == 0) {
        endIndex = schema_->length() / 2;
        startIndex = 0;
      } else {
        endIndex = schema_->length();
        startIndex = schema_->length() / 2;
      }

      for (size_t i = startIndex; i < endIndex; ++i) {
        df_->fill_row(i, *row);
        if (r_->accept(*row)) df_->update_row(*row);
      }

      delete row;
    }

  };
};
