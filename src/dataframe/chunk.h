#pragma once
#include "object.h"
#include "string.h"
#include "array.h"
#include "serial.h"

// Types of chunks.
class IntChunk;
class BoolChunk;
class DoubleChunk;
class StringChunk;

/**
  * A Chunk is a portion of a column.
  * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
  */
class Chunk : public Object {
public:
    char type_;
    size_t size_;
    bool full_;

    /** Type converters: Return same chunk under its actual type, or
     *  nullptr if of the wrong type.  */

    // returns this Column as an IntChunk*, or nullptr if not an IntChunk*
    virtual IntChunk* as_int() {return nullptr;}

    // returns this Column as a BoolChunk*, or nullptr if not a BoolChunk*
    virtual BoolChunk* as_bool() {return nullptr;}

    // returns this Column as a DoubleChunk*, or nullptr if not a DoubleChunk*
    virtual DoubleChunk* as_double() {return nullptr;}

    // returns this Column as a StringChunk*, or nullptr if not a StringChunk*
    virtual StringChunk* as_string() {return nullptr;}

    /** Return the type of this Chunk as a char: 'S', 'B', 'I' and 'D'. */
    char get_type() {
        return type_;
    }

    /** set the type of this column
     * only should be called from the constructors of subclasses
     */
    void set_type_(char type) {
        type_ = type;
    }

    /** Number of values in the chunk.
      */
    size_t size() {
        return size_;
    }
};

/**
  * A portion of a column that holds ints.
  * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
  */
class IntChunk : public Chunk {
public:
    IntArray* arr_;

    IntChunk() {
        set_type_('I');
        size_ = 0;
        arr_ = new IntArray();
        full_ = false;
    }

    ~IntChunk() {
      delete arr_;
    }

    void push_back(int val) {
        arr_->push_back(val);
        if (++size_ == ARR_SIZE) full_ = true;
    }

    int get(size_t idx) {
        return arr_->get(idx);
    }

    // returns this Column as an IntChunk*, or nullptr if not an IntChunk*
    virtual IntChunk* as_int() {return this;}
};

/**
  * A portion of a column that holds bools.
  * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
  */
class BoolChunk : public Chunk {
public:
    BoolArray* arr_;

    BoolChunk() {
        set_type_('B');
        size_ = 0;
        arr_ = new BoolArray();
        full_ = false;
    }

    ~BoolChunk() {
      delete arr_;
    }

    void push_back(bool val) {
        arr_->push_back(val);
        if (++size_ == BOOL_ARR_SIZE) full_ = true;
    }

    bool get(size_t idx) {
        return arr_->get(idx);
    }

    // returns this Column as an BoolChunk*, or nullptr if not an BoolChunk*
    virtual BoolChunk* as_bool() {return this;}
};

/**
  *
  * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
  */
class DoubleChunk : public Chunk {
public:
    DoubleArray* arr_;

    DoubleChunk() {
        set_type_('D');
        size_ = 0;
        arr_ = new DoubleArray();
        full_ = false;
    }

    ~DoubleChunk() {
      delete arr_;
    }

    void push_back(double val) {
        arr_->push_back(val);
        if (++size_ == ARR_SIZE) full_ = true;
    }

    double get(size_t idx) {
        return arr_->get(idx);
    }

    // returns this Column as an DoubleChunk*, or nullptr if not an DoubleChunk*
    virtual DoubleChunk* as_double() {return this;}
};

/**
  * A portion of a column that holds Strings.
  * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
  */
class StringChunk : public Chunk {
public:
    StringArray* arr_;

    StringChunk() {
        set_type_('S');
        size_ = 0;
        arr_ = new StringArray();
        full_ = false;
    }

    ~StringChunk() {
      arr_->delete_all();
      delete arr_;
    }

    void push_back(String* val) {
        arr_->push_back(val);
        if (++size_ == STRING_ARR_SIZE) full_ = true;
    }

    String* get(size_t idx) {
        return arr_->get(idx);
    }

    // returns this Column as an StringChunk*, or nullptr if not an StringChunk*
    virtual StringChunk* as_string() {return this;}
};

/*************************************************************************
 * ChunkArray::
 * Holds Chunk pointers. The strings are external.  Nullptr is a valid
 * value.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class ChunkArray : public Array {
public:

    Chunk*** arr_;     // internal array of Chunk* arrays
    size_t num_arr_;    // number of Chunk* arrays there are in arr_
    size_t size_;       // number of Chunk*s total there are in arr_

    // default construtor - initialize as an empty ChunkArray
    ChunkArray() {
        set_type_('C');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new Chunk**[0];
    }

    // destructor - delete arr_, its sub-arrays, and their chunks
    ~ChunkArray() {
      for (size_t i = 0; i < num_arr_; ++i) {
        for (size_t j = 0; j < size_; ++j) {
          delete arr_[i][j];
        }
        delete[] arr_[i];
      }
      delete[] arr_;
    }

    /** returns true if this is equal to that */
    bool equals(Object* that) {
        if (that == this) return true;
        ChunkArray* x = dynamic_cast<ChunkArray*>(that);
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
     * turns this Array* into an ChunkArray* (assuming it is one)
     * @returns this Array as an ChunkArray*
     */
    ChunkArray* as_chunk() {
        return this;
    }

    /** Returns the chunk at idx; undefined on invalid idx.
     * @param idx: index of Chunk* to get
     * @returns the Chunk* at that index
     */
    Chunk* get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
    }

    /**
     * Acquire ownership for the df.
     * replaces the Chunk* at given index with given Chunk*
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, Chunk* val) {
        assert(idx < size_);
        arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE] = val;
    }

    /**
     * push the given val to the end of the Array
     * @param val: Chunk* to push back
     */
    void push_back(Chunk* val) {
        // the last DF** in arr_ is full
        // copy DF**s into a new DF*** - copies pointers but not payload
        if (size_ % STRING_ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new DF** and initialize with val at first idx
            Chunk** chunks = new Chunk*[STRING_ARR_SIZE];
            chunks[0] = val;

            // set up a temp DF***, overwrite arr_ with new DF***
            Chunk*** tmp = arr_;
            arr_ = new Chunk**[num_arr_];

            // for loop to copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new DF** into arr_ and delete the temp
            arr_[num_arr_ - 1] = chunks;
            delete[] tmp;
        // we have room in the last DF** of arr_ - add the val
        } else {
            arr_[size_ / STRING_ARR_SIZE][size_ % STRING_ARR_SIZE] = val;
            ++size_;
        }
    }

    /** remove chunk at given idx */
    void remove(size_t idx) {
        assert(idx < size_);
        if (idx == size_ - 1) {
            delete arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
        } else {
            delete arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
            for (size_t i = idx; i < size_; ++i) {
                set(i, arr_[(i + 1) / STRING_ARR_SIZE][(i + 1) % STRING_ARR_SIZE]);
            }
        }
        --size_;
        if (size_ % STRING_ARR_SIZE == 0) --num_arr_;
    }

    /**
     * get the amount of Strings in the Array
     * @returns the amount of Strings in the Array
     */
    size_t size() {
        return size_;
    }
};


/**
  * Serializes Chunk types.
  * @authors armani.a@husky.neu.edu, horn.s@husky.neu.edu
  */
class ChunkSerializer: public Serializer {
public:

  const char* serialize(Chunk* chunk) {
    ByteArray* barr = new ByteArray();

    // serialize the type of chunk
    barr->push_string("typ: ");
    char ser_type[1];
    ser_type[0] = chunk->type_;
    barr->push_string(ser_type);

    // serialize the elements of chunk
    const char* ser_elm;
    if (chunk->type_ == 'I') {
      IntChunk* ic = chunk->as_int();
      ser_elm = Serializer::serialize(ic->arr_);
    } else if (chunk->type_ == 'D') {
      DoubleChunk* dc = chunk->as_double();
      ser_elm = Serializer::serialize(dc->arr_);
    } else if (chunk->type_ == 'B') {
      BoolChunk* bc = chunk->as_bool();
      ser_elm = Serializer::serialize(bc->arr_);
    } else if (chunk->type_ == 'S') {
      StringChunk* sc = chunk->as_string();
      ser_elm = Serializer::serialize(sc->arr_);
    }

    barr->push_back('\n');
    barr->push_string(ser_elm);
    delete[] ser_elm;

    const char* str = barr->as_bytes();
    delete barr;
    return str;
  }

  Chunk* get_chunk(const char* str) {
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
    i += 2;

    Serializer s;
    Chunk* ch;

    // create correct column
    if (type == 'I') {
      IntChunk* c = new IntChunk();
      delete c->arr_;
      c->arr_ = s.get_int_array(&str[i]);
      c->size_ = c->arr_->size();
      ch = c;
    }
    else if (type == 'B') {
      BoolChunk* c = new BoolChunk();
      delete c->arr_;
      c->arr_ = s.get_bool_array(&str[i]);
      c->size_ = c->arr_->size();
      ch = c;
    }
    else if (type == 'D') {
      DoubleChunk* c = new DoubleChunk();
      delete c->arr_;
      c->arr_ = s.get_double_array(&str[i]);
      c->size_ = c->arr_->size();
      ch = c;
    }
    else if (type == 'S') {
      StringChunk* c = new StringChunk();
      delete c->arr_;
      c->arr_ = s.get_string_array(&str[i]);
      c->size_ = c->arr_->size();
      ch = c;
    }

    return ch;
  }
};
