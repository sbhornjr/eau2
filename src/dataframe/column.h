// lang::CwC

#pragma once

#include "string.h"
#include "object.h"
#include "map.h"
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
    KChunkMap* kv_;          // where to send chunks
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
    IntColumn(KChunkMap* kv) {
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
    IntColumn(KChunkMap* kv, int n, ...) {
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
                Key* key = new Key(new String(k.c_str()), 0);
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
        Key* key = new Key(new String(k.c_str()), 0);
        keys_->push_back(key);
        kv_->put(key, chunk_);
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~IntColumn() {
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
            chunk_ = kv_->get(k)->as_int();
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
            Key* key = new Key(new String(k.c_str()), 0);
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
        Key* key = new Key(new String(k.c_str()), 0);
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
    BoolColumn(KChunkMap* kv) {
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
    BoolColumn(KChunkMap* kv, int n, ...) {
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
                Key* key = new Key(new String(k.c_str()), 0);
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
        Key* key = new Key(new String(k.c_str()), 0);
        keys_->push_back(key);
        kv_->put(key, chunk_);
        va_end(args);
    }

    // destructor - delete keys
    ~BoolColumn() {
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
            chunk_ = kv_->get(k)->as_bool();
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
            Key* key = new Key(new String(k.c_str()), 0);
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
        Key* key = new Key(new String(k.c_str()), 0);
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
    DoubleColumn(KChunkMap* kv) {
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
    DoubleColumn(KChunkMap* kv, int n, ...) {
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
                Key* key = new Key(new String(k.c_str()), 0);
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
        Key* key = new Key(new String(k.c_str()), 0);
        keys_->push_back(key);
        kv_->put(key, chunk_);
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~DoubleColumn() {
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
            chunk_ = kv_->get(k)->as_double();
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
            Key* key = new Key(new String(k.c_str()), 0);
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
        Key* key = new Key(new String(k.c_str()), 0);
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
    StringColumn(KChunkMap* kv) {
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
    StringColumn(KChunkMap* kv, int n, ...) {
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
                Key* key = new Key(new String(k.c_str()), 0);
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
        Key* key = new Key(new String(k.c_str()), 0);
        keys_->push_back(key);
        kv_->put(key, chunk_);
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~StringColumn() {
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
            chunk_ = kv_->get(k)->as_string();
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
            Key* key = new Key(new String(k.c_str()), 0);
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
        Key* key = new Key(new String(k.c_str()), 0);
        keys_->push_back(key);
        kv_->put(key, chunk_);

        chunk_ = nullptr;
        chunk_no_ = -1;

        done_ = true;
    }
};
