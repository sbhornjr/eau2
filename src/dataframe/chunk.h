#pragma once
#include "object.h"
#include "string.h"
#include "array.h"

class IntChunk;
class BoolChunk;
class DoubleChunk;
class StringChunk;

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

    size_t size() {
        return size_;
    }
};

class IntChunk : public Chunk {
public:
    IntArray* arr_;

    IntChunk() {
        set_type_('I');
        size_ = 0;
        arr_ = new IntArray();
        full_ = false;
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

class BoolChunk : public Chunk {
public:
    BoolArray* arr_;

    BoolChunk() {
        set_type_('B');
        size_ = 0;
        arr_ = new BoolArray();
        full_ = false;
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

class DoubleChunk : public Chunk {
public:
    DoubleArray* arr_;

    DoubleChunk() {
        set_type_('D');
        size_ = 0;
        arr_ = new DoubleArray();
        full_ = false;
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

class StringChunk : public Chunk {
public:
    StringArray* arr_;

    StringChunk() {
        set_type_('S');
        size_ = 0;
        arr_ = new StringArray();
        full_ = false;
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
