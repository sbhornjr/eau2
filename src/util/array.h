// lang::CwC

#pragma once
#include "string.h"
#include "key.h"
#include "column.h"
#include "dataframe.h"
#include <math.h>
#include <stdarg.h>

/** @designers: vitekj@me.com, course staff */

// class definitions
class IntArray;
class BoolArray;
class DoubleArray;
class StringArray;
class KeyArray;
class ByteArray;
class DFArray;
class DataFrame;

/**************************************************************************
 * Array ::
 * Represents one Array of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Arrays are mutable, equality is pointer
 * equality.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class Array : public Object {
public:
    char type_; // one of 'I' (integer), 'B' (boolean), 'D' (double), 'S' (String*)

    /** Type converters: Return same Array under its actual type, or
     *  nullptr if of the wrong type.  */

    // returns this Array as an IntArray*, or nullptr if not an IntArray*
    virtual IntArray* as_int() {return nullptr;}

    // returns this Array as a BoolArray*, or nullptr if not a BoolArray*
    virtual BoolArray* as_bool() {return nullptr;}

    // returns this Array as a DoubleArray*, or nullptr if not a DoubleArray*
    virtual DoubleArray* as_double() {return nullptr;}

    // returns this Array as a StringArray*, or nullptr if not a StringArray*
    virtual StringArray* as_string() {return nullptr;}

    // returns this Array as a ByteArray*, or nullptr if not a ByteArray*
    virtual ByteArray* as_char() {return nullptr;}

    // returns this Array as a ByteArray*, or nullptr if not a ByteArray*
    virtual DFArray* as_df() {return nullptr;}

    /** Type appropriate push_back methods. Calling the wrong method is
     * undefined behavior. **/

    // if we're here, this means the method did not exist in the subclass
    // and it shouldn't have been called. assert false.
    virtual void push_back(int val) { assert(false); }
    virtual void push_back(bool val) { assert(false); }
    virtual void push_back(double val) { assert(false); }
    virtual void push_back(String* val) { assert(false); }
    virtual void push_back(char val) { assert(false); }

    /** Returns the number of elements in the Array. Pure virtual. */
    virtual size_t size() = 0;

    virtual void delete_all() {}

    /** Return the type of this Array as a char: 'S', 'B', 'I' and 'D'. */
    char get_type() {
        return type_;
    }

    /** set the type of this Array
     * only should be called from the constructors of subclasses
     */
    void set_type_(char type) {
        type_ = type;
    }
};

/*************************************************************************
 * IntArray::
 * Holds primitive int values, unwrapped.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class IntArray : public Array {
public:

    int** arr_;         // internal array of int arrays
    size_t num_arr_;    // number of int arrays in arr_
    size_t size_;       // number of ints total in arr_


    // default constructor - initialize as an empty IntArray
    IntArray() {
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
    IntArray(int n, ...) {
        set_type_('I');

        // each int* in arr_ will be of size
        int* ints = new int[ARR_SIZE];

        // set the number of num_arr_ we will have based on n
        if (n % ARR_SIZE == 0) num_arr_ = n / ARR_SIZE;
        else num_arr_ = (n / ARR_SIZE) + 1;

        // initialize arr_ and n
        arr_ = new int*[num_arr_];
        size_t sn = n;
        size_ = n;

        size_t curr_arr = 0;    // the current int* we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size 10 is filled - add to arr_
            if (i % ARR_SIZE == 0 && i != 0) {
                arr_[curr_arr] = ints;
                ++curr_arr;
                ints = new int[ARR_SIZE];
            }
            // add the current int to ints
            ints[i % ARR_SIZE] = va_arg(args, int);
        }
        // add the last array into arr_
        arr_[curr_arr] = ints;
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~IntArray() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * gets the int at the given position in the Array
     * @param idx: index of int to get
     * @returns the int at that index
     */
    int get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / ARR_SIZE][idx % ARR_SIZE];
    }

    /**
     * turns this Array* into an IntArray* (assuming it is one)
     * @returns this Array as an IntArray*
     */
    IntArray* as_int() {
        return this;
    }

    /**
     * push the given val to the end of the Array
     * @param val: int to push back
     */
    virtual void push_back(int val) {
        // the last int* in arr_ is full
        // copy int*s into a new int** - copies pointers but not payload
        if (size_ % ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new int* and initialize with val at first idx
            int* ints = new int[ARR_SIZE];
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
            arr_[size_ / ARR_SIZE][size_ % ARR_SIZE] = val;
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
        arr_[idx / ARR_SIZE][idx % ARR_SIZE] = val;
    }

    /** remove int at given idx */
    void remove(size_t idx) {
        assert(idx < size_);
        if (idx == size_ - 1) {
            arr_[idx / ARR_SIZE][idx % ARR_SIZE] = 0;
        } else {
            for (size_t i = idx; i < size_; ++i) {
                set(i, arr_[(i + 1) / ARR_SIZE][(i + 1) % ARR_SIZE]);
            }
        }
        --size_;
        if (size_ % ARR_SIZE == 0) --num_arr_;
    }

    /**
     * get the amount of ints in the Array
     * @returns the amount of ints in the Array
     */
    size_t size() {
        return size_;
    }
};

// Other primitive Array classes similar...

/*************************************************************************
 * BoolArray::
 * Holds primitive bool values, unwrapped.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class BoolArray : public Array {
public:

    bool** arr_;        // internal array of bool arrays
    size_t num_arr_;    // number of bool arrays in arr_
    size_t size_;       // number of bools total in arr_

    // default constructor - initialize as an empty BoolArray
    BoolArray() {
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
    BoolArray(int n, ...) {
        set_type_('B');

        // each bool* in arr_ will be of size
        bool* bools = new bool[BOOL_ARR_SIZE];

        // set the number of num_arr_ we will have based on n
        if (n % BOOL_ARR_SIZE == 0) num_arr_ = n / BOOL_ARR_SIZE;
        else num_arr_ = (n / BOOL_ARR_SIZE) + 1;

        // initialize arr_ and n
        arr_ = new bool*[num_arr_];
        size_t sn = n;
        size_ = n;

        size_t curr_arr_ = 0;   // the current bool* we are at in arr)
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - add to arr_
            if (i % BOOL_ARR_SIZE == 0 && i != 0) {
                arr_[curr_arr_] = bools;
                ++curr_arr_;
                bools = new bool[BOOL_ARR_SIZE];
            }
            // add the current bool to bools
            bools[i % BOOL_ARR_SIZE] = va_arg(args, int);
        }
        // add the last array into arr_
        arr_[curr_arr_] = bools;
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~BoolArray() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * gets the bool at the given position in the Array
     * @param idx: index of bool to get
     * @returns the bool at that index
     */
    bool get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / BOOL_ARR_SIZE][idx % BOOL_ARR_SIZE];
    }

    /**
     * turns this Array* into an BoolArray* (assuming it is one)
     * @returns this Array as an BoolArray*
     */
    BoolArray* as_bool() {
        return this;
    }

    /**
     * push the given val to the end of the Array
     * @param val: bool to push back
     */
    virtual void push_back(bool val) {
        // the last bool* in arr_ is full
        // copy bool*s into a new bool** - copies pointers but not payload
        if (size_ % BOOL_ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new bool* and initialize with val at first idx
            bool* bools = new bool[BOOL_ARR_SIZE];
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
            arr_[size_ / BOOL_ARR_SIZE][size_ % BOOL_ARR_SIZE] = val;
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
        arr_[idx / BOOL_ARR_SIZE][idx % BOOL_ARR_SIZE] = val;
    }

    /**
     * get the amount of ints in the Array
     * @returns the amount of ints in the Array
     */
    size_t size() {
        return size_;
    }
};

/*************************************************************************
 * DoubleArray::
 * Holds primitive double values, unwrapped.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class DoubleArray : public Array {
public:

    double** arr_;       // internal array of double arrays
    size_t num_arr_;    // number of double arrays in arr_
    size_t size_;       // number of doubles total in arr_

    // default constructor - initialize as empty DoubleArray
    DoubleArray() {
        set_type_('D');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new double*[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of doubles in the args
     * @param ...: the doubles, handled by va_list etc.
     */
    DoubleArray(int n, ...) {
        set_type_('D');

        // each bool* in arr_ will be of size
        double* doubles = new double[ARR_SIZE];

        // set the number of num_arr_ we will have based on n
        if (n % ARR_SIZE == 0) num_arr_ = n / ARR_SIZE;
        else num_arr_ = (n / ARR_SIZE) + 1;

        // initialize arr_ and n
        arr_ = new double*[num_arr_];
        size_t sn = n;
        size_ = n;

        size_t curr_arr_ = 0;   // the current double* we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - add to arr_
            if (i % ARR_SIZE == 0 && i != 0) {
                arr_[curr_arr_] = doubles;
                ++curr_arr_;
                doubles = new double[ARR_SIZE];
            }
            // add the current double into doubles
            doubles[i % ARR_SIZE] = va_arg(args, double);
        }
        // add the last array into arr_
        arr_[curr_arr_] = doubles;
        va_end(args);
    }

    // destructor - delete arr_ and its sub-arrays
    ~DoubleArray() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /** returns true if this is equal to that */
    bool equals(Object* that) {
        if (that == this) return true;
        DoubleArray* x = dynamic_cast<DoubleArray*>(that);
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
            hash += get(i);
        }
        return hash;
    }

    /**
     * gets the double at the given position in the Array
     * @param idx: index of double to get
     * @returns the double at that index
     */
    double get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / ARR_SIZE][idx % ARR_SIZE];
    }

    /**
     * turns this Array* into an DoubleArray* (assuming it is one)
     * @returns this Array as an DoubleArray*
     */
    DoubleArray* as_double() {
        return this;
    }

    /**
     * push the given val to the end of the Array
     * @param val: double to push back
     */
    virtual void push_back(double val) {
        // the last double* in arr_ is full
        // copy double*s into a new double** - copies pointers but not payload
        if (size_ % ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new double* and initialize val at first idx
            double* doubles = new double[ARR_SIZE];
            doubles[0] = val;

            // set up a temp double**, overwrite arr_ with new double**
            double** tmp = arr_;
            arr_ = new double*[num_arr_];

            // for loop to copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new double* into arr_ and delete the temp
            arr_[num_arr_ - 1] = doubles;
            delete[] tmp;
        // we have room in the last double* of arr_ - add the val
        } else {
            arr_[size_ / ARR_SIZE][size_ % ARR_SIZE] = val;
            ++size_;
        }
    }

    /**
     * replaces the double at given index with given double
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, double val) {
        assert(idx < size_);
        arr_[idx / ARR_SIZE][idx % ARR_SIZE] = val;
    }

    /**
     * get the amount of doubles in the Array
     * @returns the amount of doubles in the Array
     */
    size_t size() {
        return size_;
    }
};

/*************************************************************************
 * StringArray::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class StringArray : public Array {
public:

    String*** arr_;     // internal array of String* arrays
    size_t num_arr_;    // number of String* arrays there are in arr_
    size_t size_;       // number of String*s total there are in arr_

    // default construtor - initialize as an empty StringArray
    StringArray() {
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
    StringArray(int n, ...) {
        set_type_('S');

        // each String** in arr_ will be of size 10
        String** strings = new String*[STRING_ARR_SIZE];

        // set the number of num_arr_ we will have based on n
        if (n % STRING_ARR_SIZE == 0) num_arr_ = n / STRING_ARR_SIZE;
        else num_arr_ = (n / STRING_ARR_SIZE) + 1;

        // initialize arr_ and n
        arr_ = new String**[num_arr_];
        size_t sn = n;
        size_ = n;

        size_t curr_arr_ = 0;   // the current String** we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - add to arr_
            if (i % STRING_ARR_SIZE == 0 && i != 0) {
                arr_[curr_arr_] = strings;
                ++curr_arr_;
                strings = new String*[STRING_ARR_SIZE];
            }
            // add the current String* to strings - copy value
            String* s = va_arg(args, String*);
            strings[i % STRING_ARR_SIZE] = s;
        }
        // add the last array into arr_
        arr_[curr_arr_] = strings;
        va_end(args);
    }

    // destructor - delete arr_, its sub-arrays, and their strings
    ~StringArray() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /** delete all strings */
    void delete_all() {
        for (size_t i = 0; i < num_arr_; ++i) {
            for (size_t j = 0; j < STRING_ARR_SIZE && (i * STRING_ARR_SIZE) + j < size_; ++j) {
                delete arr_[i][j];
            }
            delete[] arr_[i];
        }
        size_ = 0;
        num_arr_ = 0;
        delete[] arr_;
        arr_ = new String**[num_arr_];
    }

    /** returns true if this is equal to that */
    bool equals(Object* that) {
        if (that == this) return true;
        StringArray* x = dynamic_cast<StringArray*>(that);
        if (x == nullptr) return false;
        if (size_ != x->size_) return false;
        for (size_t i = 0; i < size_; ++i) {
            if (!get(i)->equals(x->get(i))) return false;
        }
        return true;
    }

    /** gets the hash code value */
    size_t hash() {
        size_t hash = 0;
        for (size_t i = 0; i < size_; ++i) {
            hash += get(i)->hash();
        }
        return hash;
    }

    /**
     * turns this Array* into an StringArray* (assuming it is one)
     * @returns this Array as an StringArray*
     */
    StringArray* as_string() {
        return this;
    }

    /** Returns the string at idx; undefined on invalid idx.
     * @param idx: index of String* to get
     * @returns the String* at that index
     */
    String* get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
    }

    /**
     * Acquire ownership of the string.
     * replaces the String* at given index with given String*
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, String* val) {
        assert(idx < size_);
        String* s = val;
        delete arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
        arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE] = s;
    }

    /**
     * push the given val to the end of the Array
     * @param val: String* to push back
     */
    void push_back(String* val) {
        // the last String** in arr_ is full
        // copy String**s into a new String*** - copies pointers but not payload
        if (size_ % STRING_ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new String** and initialize with val at first idx
            String** strings = new String*[STRING_ARR_SIZE];
            String* s = val;
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
            String* s = val;
            arr_[size_ / STRING_ARR_SIZE][size_ % STRING_ARR_SIZE] = s;
            ++size_;
        }
    }

    /** remove String at given idx */
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
     * get the amount of Strings in the Array
     * @returns the amount of Strings in the Array
     */
    size_t size() {
        return size_;
    }
};

/*************************************************************************
 * ByteArray::
 * Holds primitive char values, unwrapped.
 * @authors: horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class ByteArray : public Array {
public:

    char** arr_;         // internal array of char arrays
    size_t num_arr_;    // number of char arrays in arr_
    size_t size_;       // number of chars total in arr_


    // default constructor - initialize as an empty ByteArray
    ByteArray() {
        set_type_('C');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new char*[0];
    }

    /** NOT USED
     * constructor with values given - initialize all values into arr_
     * @param n: number of chars in the args
     * @param ...: the chars, handled by va_list etc.
     *
    ByteArray(int n, ...) {
        set_type_('C');

        // each char* in arr_ will be of size 10
        char* chars = new char[10];

        // set the number of num_arr_ we will have based on n
        if (n % 10 == 0) num_arr_ = n / 10;
        else num_arr_ = (n / 10) + 1;

        // initialize arr_ and n
        arr_ = new char*[num_arr_];
        //arr_[0] = chars;
        size_ = n;

        size_t curr_arr = 0;    // the current char* we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < n; ++i) {
            // our array of size 10 is filled - add to arr_
            if (i % 10 == 0 && i != 0) {
                arr_[curr_arr] = chars;
                ++curr_arr;
                chars = new char[10];
            }
            // add the current char to chars
            chars[i % 10] = va_arg(args, char);
        }
        // add the last array into arr_
        arr_[curr_arr] = chars;
        va_end(args);
    }*/

    // constructor turning a string into a ByteArray
    ByteArray(const char* str) {
      set_type_('C');

      size_t sz = strlen(str);

      // each char* in arr_ will be of size 10
      char* chars = new char[STRING_ARR_SIZE];

      // set the number of num_arr_ we will have based on n
      if (sz % BOOL_ARR_SIZE == 0) num_arr_ = sz / BOOL_ARR_SIZE;
      else num_arr_ = (sz / BOOL_ARR_SIZE) + 1;

      // initialize arr_ and n
      arr_ = new char*[num_arr_];
      //arr_[0] = chars;
      size_ = sz;

      size_t curr_arr = 0;    // the current char* we are at in arr_

      // for loop to fill arr_
      for (size_t i = 0; i < sz; ++i) {
          // our array of size 10 is filled - add to arr_
          if (i % BOOL_ARR_SIZE == 0 && i != 0) {
              arr_[curr_arr] = chars;
              ++curr_arr;
              chars = new char[BOOL_ARR_SIZE];
          }
          // add the current char to chars
          chars[i % BOOL_ARR_SIZE] = str[i];
      }
      // add the last array into arr_
      arr_[curr_arr] = chars;
    }

    // destructor - delete arr_ and its sub-arrays
    ~ByteArray() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /**
     * gets the char at the given position in the Array
     * @param idx: index of char to get
     * @returns the char at that index
     */
    char get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / BOOL_ARR_SIZE][idx % BOOL_ARR_SIZE];
    }

    /**
     * turns this Array* into an ByteArray* (assuming it is one)
     * @returns this Array as an ByteArray*
     */
    ByteArray* as_char() {
        return this;
    }

    /**
     * push the given val to the end of the Array
     * @param val: char to push back
     */
    virtual void push_back(char val) {
        // the last char* in arr_ is full
        // copy char*s into a new char** - copies pointers but not payload
        if (size_ % BOOL_ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new char* and initialize with val at first idx
            char* chars = new char[BOOL_ARR_SIZE];
            chars[0] = val;

            // set up a temp char**, overwrite arr_ with new char**
            char** tmp = arr_;
            arr_ = new char*[num_arr_];

            // for loop copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new char* into arr_ and delete the temp
            arr_[num_arr_ - 1] = chars;
            delete[] tmp;
        // we have room in the last char* of arr_ - add the val
        } else {
            arr_[size_ / BOOL_ARR_SIZE][size_ % BOOL_ARR_SIZE] = val;
            ++size_;
        }
    }

    /** adds the given string to the back of the array */
    void push_string(const char* str) {
      for (size_t i = 0; str[i] != 0; ++i) {
        push_back((char)str[i]);
      }
    }

    /**
     * replaces the char at given index with given char
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, char val) {
        assert(idx < size_);
        arr_[idx / BOOL_ARR_SIZE][idx % BOOL_ARR_SIZE] = val;
    }

    /**
     * get the amount of chars in the Array
     * @returns the amount of chars in the Array
     */
    size_t size() {
        return size_;
    }

    /** returns this byte array as a string of bytes*/
    const char* as_bytes() {
      char* str = new char[size_ + 1];
      for (size_t i = 0; i < size_; ++i) {
        str[i] = get(i);
      }
      str[size_] = 0;
      return str;
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
     * get the amount of Strings in the Array
     * @returns the amount of Strings in the Array
     */
    size_t size() {
        return size_;
    }
};


/*************************************************************************
 * KeyArray::
 * Holds key pointers. The keys are external (not owned).  Nullptr is a valid
 * value.
 * @authors horn.s@husky.neu.edu, armani.a@husky.neu.edu
 */
class KeyArray : public Array {
public:

    Key*** arr_;     // internal array of Key* arrays
    size_t num_arr_;    // number of Key* arrays there are in arr_
    size_t size_;       // number of Key*s total there are in arr_

    // default construtor - initialize as an empty StringArray
    KeyArray() {
        set_type_('K');
        size_ = 0;
        num_arr_ = 0;
        arr_ = new Key**[0];
    }

    /**
     * constructor with values given - initialize all values into arr_
     * @param n: number of Key* in the args
     * @param ...: the Keys, handled by va_list etc.
     */
    KeyArray(int n, ...) {
        set_type_('K');

        // each String** in arr_ will be of size 10
        Key** keys = new Key*[STRING_ARR_SIZE];

        // set the number of num_arr_ we will have based on n
        if (n % STRING_ARR_SIZE == 0) num_arr_ = n / STRING_ARR_SIZE;
        else num_arr_ = (n / STRING_ARR_SIZE) + 1;

        // initialize arr_ and n
        arr_ = new Key**[num_arr_];
        size_t sn = n;
        size_ = n;

        size_t curr_arr_ = 0;   // the current String** we are at in arr_
        va_list args;           // args given
        va_start(args, n);

        // for loop to fill arr_
        for (size_t i = 0; i < sn; ++i) {
            // our array of size is filled - add to arr_
            if (i % STRING_ARR_SIZE == 0 && i != 0) {
                arr_[curr_arr_] = keys;
                ++curr_arr_;
                keys = new Key*[STRING_ARR_SIZE];
            }
            // add the current Key* to keys - copy value
            Key* k = va_arg(args, Key*);
            keys[i % STRING_ARR_SIZE] = k;
        }
        // add the last array into arr_
        arr_[curr_arr_] = keys;
        va_end(args);
    }

    // destructor - delete arr_, its sub-arrays, and their keys
    ~KeyArray() {
        for (size_t i = 0; i < num_arr_; ++i) {
            delete[] arr_[i];
        }
        delete[] arr_;
    }

    /** delete all keys */
    void delete_all() {
        for (size_t i = 0; i < num_arr_; ++i) {
            for (size_t j = 0; j < STRING_ARR_SIZE && (i * STRING_ARR_SIZE) + j < size_; ++j) {
                delete arr_[i][j];
            }
            delete[] arr_[i];
        }
        size_ = 0;
        num_arr_ = 0;
        delete[] arr_;
        arr_ = new Key**[num_arr_];
    }

    /** returns true if this is equal to that */
    bool equals(Object* that) {
        if (that == this) return true;
        KeyArray* x = dynamic_cast<KeyArray*>(that);
        if (x == nullptr) return false;
        if (size_ != x->size_) return false;
        for (size_t i = 0; i < size_; ++i) {
            if (!get(i)->equals(x->get(i))) return false;
        }
        return true;
    }

    /** gets the hash code value */
    size_t hash() {
        size_t hash = 0;
        for (size_t i = 0; i < size_; ++i) {
            hash += get(i)->hash();
        }
        return hash;
    }

    /**
     * turns this Array* into an KeyArray* (assuming it is one)
     * @returns this Array as an KeyArray*
     */
    KeyArray* as_key() {
        return this;
    }

    /** Returns the key at idx; undefined on invalid idx.
     * @param idx: index of Key* to get
     * @returns the Key* at that index
     */
    Key* get(size_t idx) {
        assert(idx < size_);
        return arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
    }

    /**
     * Acquire ownership of the Key.
     * replaces the Key* at given index with given Key*
     * @param idx: the index at which to place this value
     * @param val: val to put at the index
     */
    void set(size_t idx, Key* val) {
        assert(idx < size_);
        Key* k = val;
        delete arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE];
        arr_[idx / STRING_ARR_SIZE][idx % STRING_ARR_SIZE] = k;
    }

    /**
     * push the given val to the end of the Array
     * @param val: Key* to push back
     */
    void push_back(Key* val) {
        // the last Key** in arr_ is full
        // copy Key**s into a new Key*** - copies pointers but not payload
        if (size_ % STRING_ARR_SIZE == 0) {
            // increment size values
            ++size_;
            ++num_arr_;

            // create new Key** and initialize with val at first idx
            Key** keys = new Key*[STRING_ARR_SIZE];
            Key* k = val;
            keys[0] = k;

            // set up a temp Key***, overwrite arr_ with new Key***
            Key*** tmp = arr_;
            arr_ = new Key**[num_arr_];

            // for loop to copy values from temp into arr_
            for (size_t i = 0; i < num_arr_ - 1; ++i) {
                arr_[i] = tmp[i];
            }

            // add new Key** into arr_ and delete the temp
            arr_[num_arr_ - 1] = keys;
            delete[] tmp;
        // we have room in the last Key** of arr_ - add the val
        } else {
            Key* k = val;
            arr_[size_ / STRING_ARR_SIZE][size_ % STRING_ARR_SIZE] = k;
            ++size_;
        }
    }

    /** remove Key at given idx */
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
     * get the amount of Keys in the Array
     * @returns the amount of Keys in the Array
     */
    size_t size() {
        return size_;
    }
};
