// lang::CwC

#pragma once
#include "column.h"
#include "row.h"
#include "schema.h"
#include "helper.h"
#include <cstdlib>
#include "object.h"
#include "string.h"
#include "array.h"
#include "KVStore.h"

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
  KVStore* kv_;     // kvstore for columns to use

  /** Create a data frame with the same columns as the given df but no rows */
  DataFrame(DataFrame& df): DataFrame(*df.schema_, df.kv_) {}

  /** Create a data frame from a schema and columns. Results are undefined if
    * the columns do not match the schema. */
  DataFrame(Schema& schema, KVStore* kv) {
    schema_ = new Schema(schema);
    cols_ = new Column*[schema_->width()];
    kv_ = kv;

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

  const char* serialize(DataFrame* df) {
      ByteArray* barr = new ByteArray();

      barr->push_string("cls:\n");

      Schema scm = df->get_schema();
      ColumnSerializer s(kv_);

      for (size_t i = 0; i < scm.width(); ++i) {
          if (i != 0) barr->push_back('\n');
          barr->push_string("\tcol:\n");
          const char* ser_col = s.serialize(df->cols_[i]);
          barr->push_string(ser_col);
          delete[] ser_col;
      }

      const char* str = barr->as_bytes();
      delete barr;

      return str;
  }

  DataFrame* get_dataframe(const char* str) {
      // should only be one line, no while loop
      size_t i = 0;
      // get the type of this line
      char type_buff[4];
      memcpy(type_buff, &str[i], 3);
      type_buff[3] = 0;
      // this is the columns line
      if (strcmp(type_buff, "cls") == 0) {
        Schema scm;
        DataFrame* df = new DataFrame(scm, kv_);
        i += 5;
        ColumnSerializer cs(kv_);
        while (i < strlen(str)) {
          i += 8;
          Column* col = cs.get_column(&str[i], &i);
          df->add_column(col);
        }
        return df;
      }
      else return nullptr;
  }

  /**
   *  create and return a df of 1 col with the values in from of size sz,
   *  and make it the value of the given key in the kvstore */
  DataFrame* from_array(Key* key, KVStore* kv, size_t sz, Array* from);

  /**
   *  Create and return a df of 1 value (scalar). Integer Version.
   *  This would be useful for storing a value such as a sum.
   *  Also assigns the dataframe to a key in the KDFMapping. */
  DataFrame* from_scalar(Key* key, KVStore* kv, int val);

  /**
   *  Create and return a df of 1 value (scalar). Double Version.
   *  This would be useful for storing a value such as a sum.
   *  Also assigns the dataframe to a key in the KDFMapping. */
  DataFrame* from_scalar(Key* key, KVStore* kv, double val);

  /**
   *  Create and return a df of 1 value (scalar). Boolean Version.
   *  Also assigns the dataframe to a key in the KDFMapping. */
  DataFrame* from_scalar(Key* key, KVStore* kv, bool val);

  /**
   *  Create and return a df of 1 value (scalar). String Version.
   *  Possible uses are a concatenated String.
   *  Also assigns the dataframe to a key in the KDFMapping. */
  DataFrame* from_scalar(Key* key, KVStore* kv, String* val);

  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() {
    return *schema_;
  }

  /** Adds a column to this dataframe, updates the schema, the new column
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
    Column* c = col; //get_new_col_(type);

    /**
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
    c->finalize();
    // add col to schema
    */
    schema_->add_column(type);

    // add the new column into cols_ by copying values into a new column**
    Column** tmp = cols_;
    cols_ = new Column*[schema_->width()];
    for (size_t i = 0; i < schema_->width() - 1; ++i) {
      cols_[i] = tmp[i];
    }
    cols_[schema_->width() - 1] = c;
    delete[] tmp;
  }

  // gets a new column of the given type
  Column* get_new_col_(char type) {
      Column* c;
      if (type == 'I') {
        c = new IntColumn(kv_);
      } else if (type == 'B') {
        c = new BoolColumn(kv_);
      } else if (type == 'D') {
        c = new DoubleColumn(kv_);
      } else if (type == 'S') {
        c = new StringColumn(kv_);
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

  /** finalize all columns, should be called after calling add_row many times */
  void finalize_all() {
    for (size_t i = 0; i < ncols(); ++i) {
      Column* c = cols_[i];
      if (c->type_ == 'I') c->as_int()->finalize();
      else if (c->type_ == 'D') c->as_double()->finalize();
      else if (c->type_ == 'B') c->as_bool()->finalize();
      else if (c->type_ == 'S') c->as_string()->finalize();
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
      r.accept(*row);
    }
    delete row;
  }
};

/*************************************************************************
 * DFArray::
 * Holds DF pointers. The strings are external.  Nullptr is a valid
 * value.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class DFArray : public Array {
public:

		DataFrame*** arr_;     // internal array of DataFrame* arrays
		size_t num_arr_;    // number of DataFrame* arrays there are in arr_
		size_t size_;       // number of DataFrame*s total there are in arr_

		// default construtor - initialize as an empty StringArray
		DFArray() {
				set_type_('D');
				size_ = 0;
				num_arr_ = 0;
				arr_ = new DataFrame**[0];
		}

		// destructor - delete arr_, its sub-arrays, and their dfs
		~DFArray() {
				for (size_t i = 0; i < num_arr_; ++i) {
						delete[] arr_[i];
				}
				delete[] arr_;
		}

		/** returns true if this is equal to that */
		bool equals(Object* that) {
				if (that == this) return true;
				DFArray* x = dynamic_cast<DFArray*>(that);
				if (x == nullptr) return false;
				if (size_ != x->size_) return false;
				for (size_t i = 0; i < size_; ++i) {
						if (get(i) != x->get(i)) return false;
				}
				return true;
		}

		/** gets the hash code value */
		size_t hash() {
				size_t hash = 0;
				for (size_t i = 0; i < size_; ++i) {
						hash += 17;
				}
				return hash;
		}

		/**
		 * turns this Array* into an DFArray* (assuming it is one)
		 * @returns this Array as an DFArray*
		 */
		DFArray* as_df() {
				return this;
		}

		/** Returns the df at idx; undefined on invalid idx.
		 * @param idx: index of DF* to get
		 * @returns the DF* at that index
		 */
		DataFrame* get(size_t idx) {
				assert(idx < size_);
				return arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
		}

		/**
		 * Acquire ownership for the df.
		 * replaces the DF* at given index with given DF*
		 * @param idx: the index at which to place this value
		 * @param val: val to put at the index
		 */
		void set(size_t idx, DataFrame* val) {
				assert(idx < size_);
				arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE] = val;
		}

		/**
		 * push the given val to the end of the Array
		 * @param val: DF* to push back
		 */
		void push_back(DataFrame* val) {
				// the last DF** in arr_ is full
				// copy DF**s into a new DF*** - copies pointers but not payload
				if (size_ % STRING_ARR_SIZE == 0) {
						// increment size values
						++size_;
						++num_arr_;

						// create new DF** and initialize with val at first idx
						DataFrame** dfs = new DataFrame*[STRING_ARR_SIZE];
						dfs[0] = val;

						// set up a temp DF***, overwrite arr_ with new DF***
						DataFrame*** tmp = arr_;
						arr_ = new DataFrame**[num_arr_];

						// for loop to copy values from temp into arr_
						for (size_t i = 0; i < num_arr_ - 1; ++i) {
								arr_[i] = tmp[i];
						}

						// add new DF** into arr_ and delete the temp
						arr_[num_arr_ - 1] = dfs;
						delete[] tmp;
				// we have room in the last DF** of arr_ - add the val
				} else {
						arr_[size_ / STRING_ARR_SIZE][size_ % STRING_ARR_SIZE] = val;
						++size_;
				}
		}

		/** remove DF at given idx */
		void remove(size_t idx) {
				assert(idx < size_);
				if (idx == size_ - 1) {
						arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE] = NULL;
				} else {
						for (size_t i = idx; i < size_; ++i) {
								set(i, arr_[(i + 1) / STRING_ARR_SIZE][(i + 1) % STRING_ARR_SIZE]);
						}
				}
				--size_;
				if (size_ % STRING_ARR_SIZE == 0) --num_arr_;
		}

		/**
		 * get the amount of DFs in the Array
		 * @returns the amount of DFs in the Array
		 */
		size_t size() {
				return size_;
		}
};

DataFrame* DataFrame::from_array(Key* key, KVStore* kv, size_t sz, Array* from) {
  Schema scm("");
  DataFrame* df = new DataFrame(scm, kv);
  Column* c = get_new_col_(from->get_type());
  if (from->as_int() != nullptr) {
    IntArray* ia = from->as_int();
    IntColumn* ic = c->as_int();
    for (size_t i = 0; i < sz; ++i) {
      ic->push_back(ia->get(i));
    }
    ic->finalize();
    df->schema_->add_column('I');
  } else if (from->as_bool() != nullptr) {
    BoolArray* ba = from->as_bool();
    BoolColumn* bc = c->as_bool();
    for (size_t i = 0; i < sz; ++i) {
      bc->push_back(ba->get(i));
    }
    bc->finalize();
    df->schema_->add_column('B');
  } else if (from->as_double() != nullptr) {
    DoubleArray* da = from->as_double();
    DoubleColumn* dc = c->as_double();
    for (size_t i = 0; i < sz; ++i) {
      dc->push_back(da->get(i));
    }
    dc->finalize();
    df->schema_->add_column('D');
  } else if (from->as_string() != nullptr) {
    StringArray* sa = from->as_string();
    StringColumn* sc = c->as_string();
    for (size_t i = 0; i < sz; ++i) {
      sc->push_back(sa->get(i));
    }
    sc->finalize();
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

/**
 *  Create and return a df of 1 value (scalar). Integer Version.
 *  This would be useful for storing a value such as a sum.
 *  Also assigns the dataframe to a key in the KDFMapping. */
DataFrame* DataFrame::from_scalar(Key* key, KVStore* kv, int val) {
  Schema scm("");
  DataFrame* df = new DataFrame(scm, kv);
  Column* c = get_new_col_('I');
  IntColumn* ic = c->as_int();
  ic->push_back(val);
  df->schema_->add_column('I');
  df->schema_->numrows_ = 1;

  Column** cols = new Column*[1];
  cols[0] = c;
  delete[] df->cols_;
  df->cols_ = cols;

  kv->put(key, df);
  return df;
}

/**
 *  Create and return a df of 1 value (scalar). Double Version.
 *  This would be useful for storing a value such as a sum.
 *  Also assigns the dataframe to a key in the KDFMapping. */
DataFrame* DataFrame::from_scalar(Key* key, KVStore* kv, double val) {
  Schema scm("");
  DataFrame* df = new DataFrame(scm, kv);
  Column* c = get_new_col_('D');
  DoubleColumn* dc = c->as_double();
  dc->push_back(val);
  df->schema_->add_column('D');
  df->schema_->numrows_ = 1;

  Column** cols = new Column*[1];
  cols[0] = c;
  delete[] df->cols_;
  df->cols_ = cols;

  kv->put(key, df);
  return df;
}

/**
 *  Create and return a df of 1 value (scalar). Boolean Version.
 *  Also assigns the dataframe to a key in the KDFMapping. */
DataFrame* DataFrame::from_scalar(Key* key, KVStore* kv, bool val) {
  Schema scm("");
  DataFrame* df = new DataFrame(scm, kv);
  Column* c = get_new_col_('B');
  BoolColumn* bc = c->as_bool();
  bc->push_back(val);
  df->schema_->add_column('B');
  df->schema_->numrows_ = 1;

  Column** cols = new Column*[1];
  cols[0] = c;
  delete[] df->cols_;
  df->cols_ = cols;

  kv->put(key, df);
  return df;
}

/**
 *  Create and return a df of 1 value (scalar). String Version.
 *  Possible uses are a concatenated String.
 *  Also assigns the dataframe to a key in the KDFMapping. */
DataFrame* DataFrame::from_scalar(Key* key, KVStore* kv, String* val) {
  Schema scm("");
  DataFrame* df = new DataFrame(scm, kv);

  Column* c = get_new_col_('S');
  StringColumn* sc = c->as_string();
  sc->push_back(val);
  df->schema_->add_column('S');
  df->schema_->numrows_ = 1;

  Column** cols = new Column*[1];
  cols[0] = c;
  delete[] df->cols_;
  df->cols_ = cols;

  kv->put(key, df);
  return df;
}

/**
 * Gets the dataframe at a specific key.
 * @param key: the key whose value we want to get
 * @returns the value that corresponds with the given key
 */
DataFrame* KVStore::get_df(Key* key) {
  Schema scm("");
  DataFrame df_(scm, this);
  // this chunk is stored here
  if (key->getHomeNode() == index()) {
    int ind = -1;
    for (size_t i = 0; i < size_; ++i) {
      if (key->equals(keys_->get(i))) {
        ind = i;
        break;
      }
    }
    if (ind == -1) {
      return nullptr;
    }
    DataFrame* df = df_.get_dataframe(values_->get(ind)->c_str());
    return df;
  }
  // TODO else request from network
  return nullptr;
}

/**
 * Sets the value at the specified key to the value.
 * If the key already exists, its value is replaced.
 * If the key does not exist, a key-value pair is created.
 * @param key: the key whose value we want to set
 * @param value: the value we want associated with the key
 */
void KVStore::put(Key* key, DataFrame* value) {
  Schema scm("");
  DataFrame df_(scm, this);

  // choose which node this chunk will go to
  key->setHomeNode(next_node_);
  ++next_node_;
  if (next_node_ == num_nodes_) next_node_ = 0;

  // does this chunk belong here
  if (key->getHomeNode() == index()) {
    // yes - add to map
    keys_->push_back(key);
    values_->push_back(df_.serialize(value));
    ++size_;
  } else {
    // TODO: no - send to correct node
  }
}
