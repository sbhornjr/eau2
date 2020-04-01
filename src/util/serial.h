// lang::CwC
#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "object.h"
#include "string.h"
#include "array.h"
#include "message.h"
#include "dataframe.h"

using namespace std;

class Column;
class DataFrame;
class Chunk;

class Serializer: public Object {
public:

    const char* serialize(size_t sz);
    const char* serialize(double d);
    const char* serialize(struct sockaddr_in addr);
    struct sockaddr_in get_sockaddr_in(const char* str, size_t* i);
    size_t get_size(const char* str, size_t* i);
    int get_int(const char* str, size_t* i);
    const char* serialize(String* str);
    String* get_string(const char* str);
    const char* serialize(StringArray* sarr);
    StringArray* get_string_array(const char* str);
    const char* serialize(DoubleArray* darr);
    DoubleArray* get_double_array(const char* str);
    const char* serialize(BoolArray* arr);
    BoolArray* get_bool_array(const char* str);
    const char* serialize(IntArray* iarr);
    IntArray* get_int_array(const char* str);
    const char* serialize(Message* msg);
    Message* get_message(const char* str);
    Message* get_ack_(const char* str, Message* msg);
    Message* get_kill_(const char* str, Message* msg);
    Message* get_text_(const char* str, Message* msg);
    const char* serialize_(Text* t);
    const char* serialize_(Register* reg);
    Message* get_register_(const char* str, Message* msg);
    const char* serialize_(Directory* dir);
    Message* get_directory_(const char* str, Message* msg);
    const char* serialize(Column* col);
    const char* serialize_(KeyArray* keys);
    const char* serialize_(Key* key);
    const char* serialize(DataFrame* df);
    DataFrame* get_dataframe(const char* str);
    Column* get_column(const char* str, size_t* ii);
    const char* serialize(Chunk* chunk);
    Chunk* get_chunk(const char* str);
};
