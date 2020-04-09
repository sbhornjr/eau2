// lang::CwC

#pragma once

#include "string.h"
#include "object.h"
#include "KVStore.h"
#include "key.h"
#include "chunk.h"
#include <math.h>
#include <stdarg.h>
#include <string>
#include <time.h>
#include <stdlib.h>

using namespace std;

/** @designers: vitekj@me.com, course staff */

// class definitions
class IntColumn;
class BoolColumn;
class DoubleColumn;
class StringColumn;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Column : public Object {
public:
    char type_; // one of 'I' (integer), 'B' (boolean), 'D' (double), 'S' (String*)
    size_t size_; // # of elements in this column
    bool done_; // is this column complete
    int chunk_no_;       // which chunk is the current (-1 if none)
    KeyArray* keys_;          // array of keys of chunks
    size_t num_chunks_;    // number of bool chunks in this col
    KVStore* kv_;          // where to send chunks
    size_t id_;             // id of this column

    /** Type converters: Return same column under its actual type, or
     *  nullptr if of the wrong type.  */

    // returns this Column as an IntColumn*, or nullptr if not an IntColumn*
    virtual IntColumn* as_int() {return nullptr;}

    // returns this Column as a BoolColumn*, or nullptr if not a BoolColumn*
    virtual BoolColumn* as_bool() {return nullptr;}

    // returns this Column as a DoubleColumn*, or nullptr if not a DoubleColumn*
    virtual DoubleColumn* as_double() {return nullptr;}

    // returns this Column as a StringColumn*, or nullptr if not a StringColumn*
    virtual StringColumn* as_string() {return nullptr;}

    /** Type appropriate push_back methods. Calling the wrong method is
     * undefined behavior. **/

    // if we're here, this means the method did not exist in the subclass
    // and it shouldn't have been called. assert false.
    virtual void push_back(int val) { assert(false); }
    virtual void push_back(bool val) { assert(false); }
    virtual void push_back(double val) { assert(false); }
    virtual void push_back(String* val) { assert(false); }

    virtual void delete_all() {}
    virtual void finalize() {}

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'D'. */
    char get_type() {
        return type_;
    }

    /** set the type of this column
     * only should be called from the constructors of subclasses
     */
    void set_type_(char type) {
        type_ = type;
    }

    /**
     * get the amount of elements in the column
     * @returns the amount of elements in the column
     */
    size_t size() {
        return size_;
    }
};

/*************************************************************************
 * IntColumn::
 * Holds primitive int values, unwrapped.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class IntColumn : public Column {
public:

    IntChunk* chunk_;       // current chunk

    // default constructor - initialize as an empty IntColumn
    IntColumn(KVStore* kv) {
        set_type_('I');
        size_ = 0;
        num_chunks_ = 0;
        keys_ = new KeyArray();
        done_ = false;
        kv_ = kv;
        id_ = kv->get_id();
        chunk_ = new IntChunk();
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of ints in the args
     * @param ...: the ints, handled by va_list etc.
     */
    IntColumn(KVStore* kv, int n, ...) {
        set_type_('I');
        done_ = true;
        kv_ = kv;
        id_ = kv->get_id();

        // each int* in arr_ will be of size
        chunk_ = new IntChunk();

        // set the number of num_arr_ we will have based on n
        if (n % ARR_SIZE == 0) num_chunks_ = n / ARR_SIZE;
        else num_chunks_ = (n / ARR_SIZE) + 1;

        // initialize arr_ and n
        keys_ = new KeyArray();
        size_t sn = n;
        size_ = sn;

        size_t curr_chunk = 0;    // the current chunk we are at
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill keys_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - send to kv
            if (chunk_->full_) {
                string k = to_string(id_) + "_" + to_string(curr_chunk);
                Key* key = new Key(new String(k.c_str()), 0, id_);
                keys_->push_back(key);
                kv_->put(key, chunk_);
                ++curr_chunk;
                chunk_ = new IntChunk();
            }
            // add the current int to ints
            chunk_->push_back(va_arg(args, int));
        }
        // send the last chunk
        string k = to_string(id_) + "_" + to_string(curr_chunk);
        Key* key = new Key(new String(k.c_str()), 0, id_);
        keys_->push_back(key);
        kv_->put(key, chunk_);
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~IntColumn() {
      kv_->kill(id_);
      delete keys_;
    }

    /**
     * gets the int at the given position in the column
     * @param idx: index of int to get
     * @returns the int at that index
     */
    int get(size_t idx) {
        assert(idx < size_);

        // we have the chunk this val is in
        if (chunk_no_ >= 0 && idx / ARR_SIZE == (size_t)chunk_no_) {
            return chunk_->get(idx % ARR_SIZE);
        // we don't have the chunk -> get it
        } else {
            Key* k = keys_->get(idx / ARR_SIZE);
            chunk_ = kv_->get_chunk(k)->as_int();
            chunk_no_ = idx / ARR_SIZE;
            return chunk_->get(idx % ARR_SIZE);
        }
    }

    /**
     * turns this Column* into an IntColumn* (assuming it is one)
     * @returns this column as an IntColumn*
     */
    IntColumn* as_int() {
        return this;
    }

    /**
     * push the given val to the end of the column
     * @param val: int to push back
     */
    virtual void push_back(int val) {
        if (done_) return;
        // the chunk is full
        if (chunk_->full_) {
            // increment size values
            ++size_;

            // send chunk to kv
            string k = to_string(id_) + "_" + to_string(num_chunks_);
            Key* key = new Key(new String(k.c_str()), 0, id_);
            keys_->push_back(key);
            kv_->put(key, chunk_);

            ++num_chunks_;

            // create new IntChunk and initialize with val at first idx
            chunk_ = new IntChunk();
            chunk_->push_back(val);
        // we have room in the chunk - add the val
        } else {
            chunk_->push_back(val);
            ++size_;
        }
    }

    /**
     * this column is finished being constructed - send last chunk
     */
    void finalize() {
        // send chunk to kv
        string k = to_string(id_) + "_" + to_string(num_chunks_);
        Key* key = new Key(new String(k.c_str()), 0, id_);
        keys_->push_back(key);
        kv_->put(key, chunk_);

        chunk_ = nullptr;
        chunk_no_ = -1;

        done_ = true;
    }
};

// Other primitive column classes similar...

/*************************************************************************
 * BoolColumn::
 * Holds primitive bool values, unwrapped.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class BoolColumn : public Column {
public:

    BoolChunk* chunk_;       // current chunk

    // default constructor - initialize as an empty BoolColumn
    BoolColumn(KVStore* kv) {
        set_type_('B');
        size_ = 0;
        num_chunks_ = 0;
        keys_ = new KeyArray();
        done_ = false;
        kv_ = kv;
        id_ = kv->get_id();
        chunk_ = new BoolChunk();
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of bools in the args
     * @param ...: the bools, handled by va_list etc.
     */
    BoolColumn(KVStore* kv, int n, ...) {
        set_type_('B');
        done_ = true;
        kv_ = kv;
        id_ = kv->get_id();

        // each bool chunk in arr_ will be of size
        chunk_ = new BoolChunk();

        // set the number of num_arr_ we will have based on n
        if (n % BOOL_ARR_SIZE == 0) num_chunks_ = n / BOOL_ARR_SIZE;
        else num_chunks_ = (n / ARR_SIZE) + 1;

        // initialize arr_ and n
        keys_ = new KeyArray();
        size_t sn = n;
        size_ = sn;

        size_t curr_chunk = 0;    // the current chunk we are at
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill keys_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - send to kv
            if (chunk_->full_) {
                string k = to_string(id_) + "_" + to_string(curr_chunk);
                Key* key = new Key(new String(k.c_str()), 0, id_);
                keys_->push_back(key);
                kv_->put(key, chunk_);
                ++curr_chunk;
                chunk_ = new BoolChunk();
            }
            // add the current int to ints
            chunk_->push_back(va_arg(args, bool));
        }
        // send the last chunk
        string k = to_string(id_) + "_" + to_string(curr_chunk);
        Key* key = new Key(new String(k.c_str()), 0, id_);
        keys_->push_back(key);
        kv_->put(key, chunk_);
        va_end(args);
    }

    // destructor - delete keys
    ~BoolColumn() {
      kv_->kill(id_);
      delete keys_;
    }

    /**
     * gets the bool at the given position in the column
     * @param idx: index of int to get
     * @returns the int at that index
     */
    bool get(size_t idx) {
        assert(idx < size_);

        // we have the chunk this val is in
        if (chunk_no_ >= 0 && idx / BOOL_ARR_SIZE == (size_t)chunk_no_) {
            return chunk_->get(idx % BOOL_ARR_SIZE);
        // we don't have the chunk -> get it
        } else {
            Key* k = keys_->get(idx / BOOL_ARR_SIZE);
            chunk_ = kv_->get_chunk(k)->as_bool();
            chunk_no_ = idx / BOOL_ARR_SIZE;
            return chunk_->get(idx % BOOL_ARR_SIZE);
        }
    }

    /**
     * turns this Column* into an BoolColumn* (assuming it is one)
     * @returns this column as an BoolColumn*
     */
    BoolColumn* as_bool() {
        return this;
    }

    /**
     * push the given val to the end of the column
     * @param val: bool to push back
     */
    virtual void push_back(bool val) {
        if (done_) return;
        // the chunk is full
        if (chunk_->full_) {
            // increment size values
            ++size_;

            // send chunk to kv
            string k = to_string(id_) + "_" + to_string(num_chunks_);
            Key* key = new Key(new String(k.c_str()), 0, id_);
            keys_->push_back(key);
            kv_->put(key, chunk_);

            ++num_chunks_;

            // create new BoolChunk and initialize with val at first idx
            chunk_ = new BoolChunk();
            chunk_->push_back(val);
        // we have room in the chunk - add the val
        } else {
            chunk_->push_back(val);
            ++size_;
        }
    }

    /**
     * this column is finished being constructed - send last chunk
     */
    void finalize() {
        // send chunk to kv
        string k = to_string(id_) + "_" + to_string(num_chunks_);
        Key* key = new Key(new String(k.c_str()), 0, id_);
        keys_->push_back(key);
        kv_->put(key, chunk_);

        chunk_ = nullptr;
        chunk_no_ = -1;

        done_ = true;
    }
};

/*************************************************************************
 * DoubleColumn::
 * Holds primitive double values, unwrapped.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class DoubleColumn : public Column {
public:

    DoubleChunk* chunk_;       // current chunk

    // default constructor - initialize as an empty DoubleColumn
    DoubleColumn(KVStore* kv) {
        set_type_('D');
        size_ = 0;
        num_chunks_ = 0;
        keys_ = new KeyArray();
        done_ = false;
        kv_ = kv;
        id_ = kv->get_id();
        chunk_ = new DoubleChunk();
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of dubs in the args
     * @param ...: the dubs, handled by va_list etc.
     */
    DoubleColumn(KVStore* kv, int n, ...) {
        set_type_('D');
        done_ = true;
        kv_ = kv;
        id_ = kv->get_id();

        // each Double chunk in arr_ will be of size
        chunk_ = new DoubleChunk();

        // set the number of num_arr_ we will have based on n
        if (n % ARR_SIZE == 0) num_chunks_ = n / ARR_SIZE;
        else num_chunks_ = (n / ARR_SIZE) + 1;

        // initialize arr_ and n
        keys_ = new KeyArray();
        size_t sn = n;
        size_ = sn;

        size_t curr_chunk = 0;    // the current chunk we are at
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill keys_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - send to kv
            if (chunk_->full_) {
                string k = to_string(id_) + "_" + to_string(curr_chunk);
                Key* key = new Key(new String(k.c_str()), 0, id_);
                keys_->push_back(key);
                kv_->put(key, chunk_);
                ++curr_chunk;
                chunk_ = new DoubleChunk();
            }
            // add the current double to chunk
            chunk_->push_back(va_arg(args, double));
        }
        // send the last chunk
        string k = to_string(id_) + "_" + to_string(curr_chunk);
        Key* key = new Key(new String(k.c_str()), 0, id_);
        keys_->push_back(key);
        kv_->put(key, chunk_);
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~DoubleColumn() {
      kv_->kill(id_);
      delete keys_;
    }

    /**
     * gets the double at the given position in the column
     * @param idx: index of double to get
     * @returns the double at that index
     */
    double get(size_t idx) {
        assert(idx < size_);

        // we have the chunk this val is in
        if (chunk_no_ >= 0 && idx / ARR_SIZE == (size_t)chunk_no_) {
            return chunk_->get(idx % ARR_SIZE);
        // we don't have the chunk -> get it
        } else {
            Key* k = keys_->get(idx / ARR_SIZE);
            chunk_ = kv_->get_chunk(k)->as_double();
            chunk_no_ = idx / ARR_SIZE;
            return chunk_->get(idx % ARR_SIZE);
        }
    }

    /**
     * turns this Column* into an DoubleColumn* (assuming it is one)
     * @returns this column as an DoubleColumn*
     */
    DoubleColumn* as_double() {
        return this;
    }

    /**
     * push the given val to the end of the column
     * @param val: double to push back
     */
    virtual void push_back(double val) {
        if (done_) return;
        // the chunk is full
        if (chunk_->full_) {
            // increment size values
            ++size_;

            // send chunk to kv
            string k = to_string(id_) + "_" + to_string(num_chunks_);
            Key* key = new Key(new String(k.c_str()), 0, id_);
            keys_->push_back(key);
            kv_->put(key, chunk_);

            ++num_chunks_;

            // create new doubleChunk and initialize with val at first idx
            chunk_ = new DoubleChunk();
            chunk_->push_back(val);
        // we have room in the chunk - add the val
        } else {
            chunk_->push_back(val);
            ++size_;
        }
    }

    /**
     * this column is finished being constructed - send last chunk
     */
    void finalize() {
        // send chunk to kv
        string k = to_string(id_) + "_" + to_string(num_chunks_);
        Key* key = new Key(new String(k.c_str()), 0, id_);
        keys_->push_back(key);
        kv_->put(key, chunk_);

        chunk_ = nullptr;
        chunk_no_ = -1;

        done_ = true;
    }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class StringColumn : public Column {
public:

    StringChunk* chunk_;       // current chunk

    // default constructor - initialize as an empty StringColumn
    StringColumn(KVStore* kv) {
        set_type_('S');
        size_ = 0;
        num_chunks_ = 0;
        keys_ = new KeyArray();
        done_ = false;
        kv_ = kv;
        id_ = kv->get_id();
        chunk_ = new StringChunk();
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of Strings in the args
     * @param ...: the Strings, handled by va_list etc.
     */
    StringColumn(KVStore* kv, int n, ...) {
        set_type_('S');
        done_ = true;
        kv_ = kv;
        id_ = kv->get_id();

        // each String chunk in arr_ will be of size
        chunk_ = new StringChunk();

        // set the number of num_arr_ we will have based on n
        if (n % STRING_ARR_SIZE == 0) num_chunks_ = n / STRING_ARR_SIZE;
        else num_chunks_ = (n / STRING_ARR_SIZE) + 1;

        // initialize arr_ and n
        keys_ = new KeyArray();
        size_t sn = n;
        size_ = sn;

        size_t curr_chunk = 0;    // the current chunk we are at
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill keys_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - send to kv
            if (chunk_->full_) {
                string k = to_string(id_) + "_" + to_string(curr_chunk);
                Key* key = new Key(new String(k.c_str()), 0, id_);
                keys_->push_back(key);
                kv_->put(key, chunk_);
                ++curr_chunk;
                chunk_ = new StringChunk();
            }
            // add the current String to chunk
            chunk_->push_back(va_arg(args, String*));
        }
        // send the last chunk
        string k = to_string(id_) + "_" + to_string(curr_chunk);
        Key* key = new Key(new String(k.c_str()), 0, id_);
        keys_->push_back(key);
        kv_->put(key, chunk_);
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~StringColumn() {
      kv_->kill(id_);
      delete keys_;
    }

    /**
     * gets the String at the given position in the column
     * @param idx: index of String to get
     * @returns the String at that index
     */
    String* get(size_t idx) {
        assert(idx < size_);

        // we have the chunk this val is in
        if (chunk_no_ >= 0 && idx / ARR_SIZE == (size_t)chunk_no_) {
            return chunk_->get(idx % STRING_ARR_SIZE);
        // we don't have the chunk -> get it
        } else {
            Key* k = keys_->get(idx / STRING_ARR_SIZE);
            chunk_ = kv_->get_chunk(k)->as_string();
            chunk_no_ = idx / STRING_ARR_SIZE;
            return chunk_->get(idx % STRING_ARR_SIZE);
        }
    }

    /**
     * turns this Column* into an StringColumn* (assuming it is one)
     * @returns this column as an StringColumn*
     */
    StringColumn* as_string() {
        return this;
    }

    /**
     * push the given val to the end of the column
     * @param val: String to push back
     */
    virtual void push_back(String* val) {
        if (done_) return;
        // the chunk is full
        if (chunk_->full_) {
            // increment size values
            ++size_;

            // send chunk to kv
            string k = to_string(id_) + "_" + to_string(num_chunks_);
            Key* key = new Key(new String(k.c_str()), 0, id_);
            keys_->push_back(key);
            kv_->put(key, chunk_);

            ++num_chunks_;

            // create new StringChunk and initialize with val at first idx
            chunk_ = new StringChunk();
            chunk_->push_back(val);
        // we have room in the chunk - add the val
        } else {
            chunk_->push_back(val);
            ++size_;
        }
    }

    /**
     * this column is finished being constructed - send last chunk
     */
    void finalize() {
        // send chunk to kv
        string k = to_string(id_) + "_" + to_string(num_chunks_);
        Key* key = new Key(new String(k.c_str()), 0, id_);
        keys_->push_back(key);
        kv_->put(key, chunk_);

        chunk_ = nullptr;
        chunk_no_ = -1;

        done_ = true;
    }
};

/**
  * Serializes Column types.
  * @authors armani.a@husky.neu.edu, horn.s@husky.neu.edu
  */
class ColumnSerializer : public Serializer {
public:

  KVStore* kc_;

  ColumnSerializer(KVStore* kc) {
    kc_ = kc;
  }

  const char* serialize(Column* col) {
      ByteArray* barr = new ByteArray();

      // serialize the type
      barr->push_string("\t\ttyp: ");
      barr->push_back((char)col->get_type());

      // serialize the size
      Serializer s;
      barr->push_string("\n\t\tsiz: ");
      const char* ser_siz = s.serialize(col->size_);
      barr->push_string(ser_siz);
      delete[] ser_siz;

      // serialize the id
      barr->push_string("\n\t\tid_: ");
      const char* ser_id = s.serialize(col->id_);
      barr->push_string(ser_id);
      delete[] ser_id;

      // serialize the column keys
      barr->push_string("\n\t\tkys:\n");
      const char* ser_keys = Serializer::serialize(col->keys_);
      barr->push_string(ser_keys);
      delete[] ser_keys;

      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

  Column* get_column(const char* str, size_t* ii) {
    char type;

    size_t i = 0;

    // get the type of this line - should be typ
    char type_buff[4];
    memcpy(type_buff, &str[i], 3);
    type_buff[3] = 0;

    // first needs to be type so we know which column to create
    if (strcmp(type_buff, "typ") != 0) return nullptr;

    // get the type
    i += 5;
    type = str[i];

    Column* col;

    // create correct column
    if (type == 'I') {
      IntColumn* c = new IntColumn(kc_);
      delete c->chunk_;
      col = c;
    }
    else if (type == 'B') {
      BoolColumn* c = new BoolColumn(kc_);
      delete c->chunk_;
      col = c;
    }
    else if (type == 'D') {
      DoubleColumn* c = new DoubleColumn(kc_);
      delete c->chunk_;
      col = c;
    }
    else if (type == 'S') {
      StringColumn* c = new StringColumn(kc_);
      delete c->chunk_;
      col = c;
    }

    // get size
    Serializer s;
    i += 4; //siz
    size_t siz = s.get_size(&str[i], &i);

    // get id
    i += 2; // id_
    size_t id = s.get_size(&str[i], &i);

    // get keys
    i += 2; // kys
    KeyArray* keys = s.get_key_array(&str[i], &i);

    delete col->keys_;
    col->keys_ = keys;
    col->id_ = id;
    col->size_ = siz;
    col->num_chunks_ = keys->size();
    col->done_ = true;
    col->chunk_no_ = -1;

    (*ii) += i;

    return col;
  }
};
