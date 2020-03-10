// lang::CwC

#pragma once
#include "../string.h"
#include <math.h>
#include <stdarg.h>

/** @designers: vitekj@me.com, course staff */

// class definitions
class IntColumn;
class BoolColumn;
class FloatColumn;
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
    char type_; // one of 'I' (integer), 'B' (boolean), 'F' (float), 'S' (String*)

    /** Type converters: Return same column under its actual type, or
     *  nullptr if of the wrong type.  */

    // returns this Column as an IntColumn*, or nullptr if not an IntColumn*
    virtual IntColumn* as_int() {return nullptr;}

    // returns this Column as a BoolColumn*, or nullptr if not a BoolColumn*
    virtual BoolColumn* as_bool() {return nullptr;}

    // returns this Column as a FloatColumn*, or nullptr if not a FloatColumn*
    virtual FloatColumn* as_float() {return nullptr;}

    // returns this Column as a StringColumn*, or nullptr if not a StringColumn*
    virtual StringColumn* as_string() {return nullptr;}

    /** Type appropriate push_back methods. Calling the wrong method is
     * undefined behavior. **/

    // if we're here, this means the method did not exist in the subclass
    // and it shouldn't have been called. assert false.
    virtual void push_back(int val) { assert(false); }
    virtual void push_back(bool val) { assert(false); }
    virtual void push_back(float val) { assert(false); }
    virtual void push_back(String* val) { assert(false); }

    /** Returns the number of elements in the column. Pure virtual. */
    virtual size_t size() = 0;

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
    char get_type() {
        return type_;
    }

    /** set the type of this column
     * only should be called from the constructors of subclasses
     */
    void set_type_(char type) {
        type_ = type;
    }
};

/*************************************************************************
 * IntColumn::
 * Holds primitive int values, unwrapped.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class IntColumn : public Column {
public:

    int** arr_;         // internal array of int arrays
    size_t num_arr_;    // number of int arrays in arr_ 
    size_t size_;       // number of ints total in arr_


    // default constructor - initialize as an empty IntColumn
    IntColumn() {
        set_type_('I');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new int*[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of ints in the args
     * @param ...: the ints, handled by va_list etc.
     */
    IntColumn(int n, ...) {
        set_type_('I');

        // each int* in arr_ will be of size 10
        int* ints = new int[10];

        // set the number of num_arr_ we will have based on n
        if (n % 10 == 0) num_arr_ = n / 10;
        else num_arr_ = (n / 10) + 1;

        // initialize arr_ and n
        arr_ = new int*[num_arr_];
        //arr_[0] = ints;
        size_ = n;

        size_t curr_arr = 0;    // the current int* we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < n; ++i) {
            // our array of size 10 is filled - add to arr_
            if (i % 10 == 0 && i != 0) {
                arr_[curr_arr] = ints;
                ++curr_arr;
                ints = new int[10];
            }
            // add the current int to ints
            ints[i % 10] = va_arg(args, int);
        }
        // add the last array into arr_
        arr_[curr_arr] = ints;
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~IntColumn() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * gets the int at the given position in the column
     * @param idx: index of int to get
     * @returns the int at that index
     */
    int get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / 10][idx % 10];
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
        // the last int* in arr_ is full
        // copy int*s into a new int** - copies pointers but not payload
        if (size_ % 10 == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new int* and initialize with val at first idx
            int* ints = new int[10];
            ints[0] = val;

            // set up a temp int**, overwrite arr_ with new int**
            int** tmp = arr_;
            arr_ = new int*[num_arr_];

            // for loop copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new int* into arr_ and delete the temp
            arr_[num_arr_ - 1] = ints;
            delete[] tmp;
        // we have room in the last int* of arr_ - add the val
        } else {
            arr_[size_ / 10][size_ % 10] = val;
            ++size_;
        }
    }

    /**
     * replaces the int at given index with given int
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, int val) {
        assert(idx < size_);
        arr_[idx / 10][idx % 10] = val;
    }

    /**
     * get the amount of ints in the column
     * @returns the amount of ints in the column
     */
    size_t size() {
        return size_;
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

    bool** arr_;        // internal array of bool arrays
    size_t num_arr_;    // number of bool arrays in arr_
    size_t size_;       // number of bools total in arr_

    // default constructor - initialize as an empty BoolColumn
    BoolColumn() {
        set_type_('B');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new bool*[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of bools in the args
     * @param ...: the bools, handled by va_list etc.
     */
    BoolColumn(int n, ...) {
        set_type_('B');

        // each bool* in arr_ will be of size 10
        bool* bools = new bool[10];

        // set the number of num_arr_ we will have based on n
        if (n % 10 == 0) num_arr_ = n / 10;
        else num_arr_ = (n / 10) + 1;

        // initialize arr_ and n
        arr_ = new bool*[num_arr_];
        //arr_[0] = bools;
        size_ = n;

        size_t curr_arr_ = 0;   // the current bool* we are at in arr)
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < n; ++i) {
            // our array of size 10 is filled - add to arr_
            if (i % 10 == 0 && i != 0) {
                arr_[curr_arr_] = bools;
                ++curr_arr_;
                bools = new bool[10];
            }
            // add the current bool to bools
            bools[i % 10] = va_arg(args, int);
        }
        // add the last array into arr_
        arr_[curr_arr_] = bools;
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~BoolColumn() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * gets the bool at the given position in the column
     * @param idx: index of bool to get
     * @returns the bool at that index
     */
    bool get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / 10][idx % 10];
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
        // the last bool* in arr_ is full
        // copy bool*s into a new bool** - copies pointers but not payload
        if (size_ % 10 == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new bool* and initialize with val at first idx
            bool* bools = new bool[10];
            bools[0] = val;

            // set up a temp bool**, overwrite arr_ with new bool**
            bool** tmp = arr_;
            arr_ = new bool*[num_arr_];

            // for loop copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new bool* into arr_ and delete the temp
            arr_[num_arr_ - 1] = bools;
            delete[] tmp;
        // we have room in the last bool* of arr_ - add the val
        } else {
            arr_[size_ / 10][size_ % 10] = val;
            ++size_;
        }
    }

    /**
     * replaces the bool at given index with given bool
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, bool val) {
        assert(idx < size_);
        arr_[idx / 10][idx % 10] = val;
    }

    /**
     * get the amount of ints in the column
     * @returns the amount of ints in the column
     */
    size_t size() {
        return size_;
    }
};

/*************************************************************************
 * FloatColumn::
 * Holds primitive float values, unwrapped.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class FloatColumn : public Column {
public:

    float** arr_;       // internal array of float arrays
    size_t num_arr_;    // number of float arrays in arr_
    size_t size_;       // number of floats total in arr_

    // default constructor - initialize as empty FloatColumn
    FloatColumn() {
        set_type_('F');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new float*[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of floats in the args
     * @param ...: the floats, handled by va_list etc.
     */
    FloatColumn(int n, ...) {
        set_type_('F');

        // each bool* in arr_ will be of size 10
        float* floats = new float[10];

        // set the number of num_arr_ we will have based on n
        if (n % 10 == 0) num_arr_ = n / 10;
        else num_arr_ = (n / 10) + 1;

        // initialize arr_ and n
        arr_ = new float*[num_arr_];
        //arr_[0] = floats;
        size_ = n;

        size_t curr_arr_ = 0;   // the current float* we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < n; ++i) {
            // our array of size 10 is filled - add to arr_
            if (i % 10 == 0 && i != 0) {
                arr_[curr_arr_] = floats;
                ++curr_arr_;
                floats = new float[10];
            }
            // add the current float into floats
            floats[i % 10] = va_arg(args, double);
        }
        // add the last array into arr_
        arr_[curr_arr_] = floats;
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~FloatColumn() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * gets the float at the given position in the column
     * @param idx: index of float to get
     * @returns the float at that index
     */
    float get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / 10][idx % 10];
    }

    /**
     * turns this Column* into an FloatColumn* (assuming it is one)
     * @returns this column as an FloatColumn*
     */
    FloatColumn* as_float() {
        return this;
    }

    /**
     * push the given val to the end of the column
     * @param val: float to push back
     */
    virtual void push_back(float val) {
        // the last float* in arr_ is full
        // copy float*s into a new float** - copies pointers but not payload
        if (size_ % 10 == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new float* and initialize val at first idx
            float* floats = new float[10];
            floats[0] = val;

            // set up a temp float**, overwrite arr_ with new float**
            float** tmp = arr_;
            arr_ = new float*[num_arr_];

            // for loop to copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new float* into arr_ and delete the temp
            arr_[num_arr_ - 1] = floats;
            delete[] tmp;
        // we have room in the last float* of arr_ - add the val
        } else {
            arr_[size_ / 10][size_ % 10] = val;
            ++size_;
        }
    }

    /**
     * replaces the float at given index with given float
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, float val) {
        assert(idx < size_);
        arr_[idx / 10][idx % 10] = val;
    }

    /**
     * get the amount of floats in the column
     * @returns the amount of floats in the column
     */
    size_t size() {
        return size_;
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

    String*** arr_;     // internal array of String* arrays
    size_t num_arr_;    // number of String* arrays there are in arr_
    size_t size_;       // number of String*s total there are in arr_

    // default construtor - initialize as an empty StringColumn
    StringColumn() {
        set_type_('S');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new String**[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of String* in the args
     * @param ...: the Strings, handled by va_list etc.
     */
    StringColumn(int n, ...) {
        set_type_('S');

        // each String** in arr_ will be of size 10
        String** strings = new String*[10];

        // set the number of num_arr_ we will have based on n
        if (n % 10 == 0) num_arr_ = n / 10;
        else num_arr_ = (n / 10) + 1;

        // initialize arr_ and n
        arr_ = new String**[num_arr_];
        //arr_[0] = strings;
        size_ = n;

        size_t curr_arr_ = 0;   // the current String** we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < n; ++i) {
            // our array of size 10 is filled - add to arr_
            if (i % 10 == 0 && i != 0) {
                arr_[curr_arr_] = strings;
                ++curr_arr_;
                strings = new String*[10];
            }
            // add the current String* to strings - copy value
            String* s = new String(*va_arg(args, String*));
            strings[i % 10] = s;
        }
        // add the last array into arr_
        arr_[curr_arr_] = strings;
        va_end(args);
    }

    // destructor - delete arr_, its sub-arrays, and their strings
    ~StringColumn() {
        for (size_t i = 0; i < num_arr_; ++i) {
            for (size_t j = 0; j < 10 && (i * 10) + j < size_; ++j) {
                delete arr_[i][j];
            }
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * turns this Column* into an StringColumn* (assuming it is one)
     * @returns this column as an StringColumn*
     */
    StringColumn* as_string() {
        return this;
    }

    /** Returns the string at idx; undefined on invalid idx.
     * @param idx: index of String* to get
     * @returns the String* at that index
     */
    String* get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / 10][idx % 10];
    }

    /**
     * Acquire ownership fo the string.
     * replaces the int at given index with given String*
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, String* val) {
        assert(idx < size_);
        String* s = new String(*val);
        delete arr_[idx / 10][idx % 10];
        arr_[idx / 10][idx % 10] = s;
    }

    /**
     * push the given val to the end of the column
     * @param val: String* to push back
     */
    void push_back(String* val) {
        // the last String** in arr_ is full
        // copy String**s into a new String*** - copies pointers but not payload
        if (size_ % 10 == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new String** and initialize with val at first idx
            String** strings = new String*[10];
            String* s = new String(*val);
            strings[0] = s;

            // set up a temp String***, overwrite arr_ with new String***
            String*** tmp = arr_;
            arr_ = new String**[num_arr_];

            // for loop to copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new String** into arr_ and delete the temp
            arr_[num_arr_ - 1] = strings;
            delete[] tmp;
        // we have room in the last String** of arr_ - add the val
        } else {
            String* s = new String(*val);
            arr_[size_ / 10][size_ % 10] = s;
            ++size_;
        }
    }

    /**
     * get the amount of Strings in the column
     * @returns the amount of Strings in the column
     */
    size_t size() {
        return size_;
    }
};
