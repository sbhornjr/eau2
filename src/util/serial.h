// lang::CwC
#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "object.h"
#include "string.h"
#include "array.h"
#include <sys/socket.h>
#include <netinet/in.h>

using namespace std;

class Serializer: public Object {
public:

  const char* serialize(size_t sz) {
      ByteArray* barr = new ByteArray();
      char siz[6];
      sprintf(siz, "%zu", sz);
      barr->push_string(siz);
      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

  const char* serialize(double d) {
      ByteArray* barr = new ByteArray();
      char dbl[16];
      sprintf(dbl, "%f", d);
      barr->push_string(dbl);
      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

  const char* serialize(struct sockaddr_in addr) {
      ByteArray* barr = new ByteArray();

      char buff[100];
      memset(buff, 0, 100);

      char fam[4];            // buff for sin_family (ip4, ip6, non)
      memset(fam, 0, 4);
      if (addr.sin_family == AF_INET) memcpy(fam, "ip4", 3);
      else if (addr.sin_family == AF_INET6) memcpy(fam, "ip6", 3);
      else if (addr.sin_family == AF_UNSPEC) memcpy(fam, "non", 3);

      // addr: sin_family, sin_port, sin_addr.s_addr
      sprintf(buff, "\tfam: %s\n\tprt: %hu\n\tadr: %i",
              fam, addr.sin_port, addr.sin_addr.s_addr);
      barr->push_string(buff);

      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

  struct sockaddr_in get_sockaddr_in(const char* str, size_t* i) {
      size_t port;
      int adr;
      size_t n = 5;
      char fam[4];
      memset(fam, 0, 4);

      // go through lines of str
      while (n < strlen(str)) {
          ++n;
          // get the type of this line
          char type_buff[4];
          memcpy(type_buff, &str[n], 3);
          type_buff[3] = 0;
          // this is a family line
          if (strcmp(type_buff, "fam") == 0) {
              n += 5;
              memcpy(fam, &str[n], 3);
              n += 4;
          }
          // this is a prt line
          else if (strcmp(type_buff, "prt") == 0) port = get_size(&str[n], &n);
          // this is a adr line
          else if (strcmp(type_buff, "adr") == 0) adr = get_int(&str[n], &n);
          else break;
      }

      struct sockaddr_in addr;
      struct in_addr inadr;
      if (strcmp(fam, "ip4") == 0) addr.sin_family = AF_INET;
      else if (strcmp(fam, "ip6") == 0) addr.sin_family = AF_INET6;
      else if (strcmp(fam, "non") == 0) addr.sin_family = AF_UNSPEC;

      addr.sin_port = port;
      inadr.s_addr = adr;
      addr.sin_addr = inadr;

      (*i) += (n - 1);

      return addr;
  }


  size_t get_size(const char* str, size_t* i) {
      size_t new_line_loc, sz;
      size_t n = 5;
      // find the end of the line
      for (size_t j = n; str[j] != '\n' && str[j] != 0; ++j) {
          new_line_loc = j;
      }
      ++new_line_loc;

      // get the sender idx
      char buff[new_line_loc - n + 1];
      memcpy(buff, &str[n], new_line_loc - n);
      buff[new_line_loc - n] = 0;
      sz = atoi(buff);

      (*i) += new_line_loc + 1;

      return sz;
  }

  int get_int(const char* str, size_t* i) {
      return (int)get_size(str, i);
  }

  const char* serialize(String* str) {
      ByteArray* barr = new ByteArray();

      // wrap string literal in quotes
      barr->push_back('\"');
      barr->push_string(str->c_str());
      barr->push_back('\"');

      const char* str_ = barr->as_bytes();
      delete barr;
      return str_;
  }

  String* get_string(const char* str) {
      return new String(str);
  }

  const char* serialize(StringArray* sarr) {
      ByteArray* barr = new ByteArray();

      // serialize the size
      barr->push_string("siz: ");
      const char* ser_sz = serialize(sarr->size());
      barr->push_string(ser_sz);
      delete[] ser_sz;

      // serialize the array
      barr->push_string("\narr: ");
      for (size_t i = 0; i < sarr->size() - 1; ++i) {
          const char* ser_str = serialize(sarr->get(i));
          barr->push_string(ser_str);
          barr->push_string(", ");
          delete ser_str;
      }


      // add last element of array
      const char* ser_str = serialize(sarr->get(sarr->size() - 1));
      barr->push_string(ser_str);
      delete[] ser_str;

      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

  StringArray* get_string_array(const char* str) {
      size_t arr_size, new_line_loc, i;

      StringArray* sarr = new StringArray();

      // go through lines of str
      i = 0;
      while (i < strlen(str)) {
          // get the type of this line
          char type_buff[4];
          memcpy(type_buff, &str[i], 3);
          type_buff[3] = 0;
          // this is a sz line
          if (strcmp(type_buff, "siz") == 0) {
              i += 4;
              // find the end of the line
              for (size_t j = i; str[j] != '\n' && str[j] != 0; ++j) {
                  new_line_loc = j;
              }
              ++new_line_loc;
              // get the size
              char sz_buff[new_line_loc - i + 1];
              memcpy(sz_buff, &str[i], new_line_loc - i);
              sz_buff[new_line_loc - i] = 0;
              arr_size = atoi(sz_buff);
              i = new_line_loc + 1;
          // this is an ar line
          } else if (strcmp(type_buff, "arr") == 0) {
              // get the string array line
              char* ar_line = duplicate(&str[i + 5]);
              i += 5;
              // split into tokens and add to sarr
              char* string;
              string = strtok(ar_line, "\", ");
              while (string != NULL) {
                  String* new_str = new String(string);
                  sarr->push_back(new_str);
                  string = strtok(NULL, "\", ");
              }
              // find the end of the line
              for (size_t j = i; str[j] != '\n' && str[j] != 0; ++j) {
                  new_line_loc = j;
              }
              i = new_line_loc + 1;
              delete[] ar_line;
          }
      }

      // make sure sizes match up
      assert(arr_size == sarr->size());

      return sarr;
  }

  const char* serialize(DoubleArray* darr) {
      ByteArray* barr = new ByteArray();

      // serialize size
      barr->push_string("siz: ");
      const char* ser_sz = serialize(darr->size());
      barr->push_string(ser_sz);
      delete[] ser_sz;

      // serialize array
      barr->push_string("\narr: ");
      for (size_t i = 0; i < darr->size() - 1; ++i) {
          const char* ser_dbl = serialize(darr->get(i));
          barr->push_string(ser_dbl);
          barr->push_string(", ");
          delete[] ser_dbl;
      }

      // add last element of array
      const char* ser_dbl = serialize(darr->get(darr->size() - 1));
      barr->push_string(ser_dbl);
      delete[] ser_dbl;

      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

  DoubleArray* get_double_array(const char* str) {
      size_t arr_size, new_line_loc, i;

      DoubleArray* darr = new DoubleArray();

      // go through lines of str
      i = 0;
      while (i < strlen(str)) {
          //printf("%zu, %zu\n", i, strlen(str));
          // get the type of this line
          char type_buff[4];
          memcpy(type_buff, &str[i], 3);
          type_buff[3] = 0;
          // this is a sz line
          if (strcmp(type_buff, "siz") == 0) {
              i += 5;
              // find the end of the line
              for (size_t j = i; str[j] != '\n' && str[j] != 0; ++j) {
                  new_line_loc = j;
              }
              ++new_line_loc;
              // get the size
              char sz_buff[new_line_loc - i + 1];
              memcpy(sz_buff, &str[i], new_line_loc - i);
              sz_buff[new_line_loc - i] = 0;
              arr_size = atoi(sz_buff);
              i = new_line_loc + 1;
          // this is an ar line
          } else if (strcmp(type_buff, "arr") == 0) {
              // get the double array line
              char* ar_line = duplicate(&str[i + 5]);
              i += 5;
              // split into tokens and add to darr
              char* next_double;
              next_double = strtok(ar_line, ", ");
              while (next_double != NULL) {
                  i += strlen(next_double) + 2;
                  darr->push_back(atof(next_double));
                  next_double = strtok(NULL, ", ");
              }
              delete[] ar_line;
          }
      }

      // make sure sizes match up
      assert(arr_size == darr->size());

      return darr;
  }

  const char* serialize(BoolArray* arr) {
      ByteArray* barr = new ByteArray();

      // serialize size
      barr->push_string("siz: ");
      const char* ser_sz = serialize(arr->size());
      barr->push_string(ser_sz);
      delete[] ser_sz;

      // serialize array
      barr->push_string("\narr: ");
      for (size_t i = 0; i < arr->size() - 1; ++i) {
          if (arr->get(i)) barr->push_string("true");
          else barr->push_string("false");
          barr->push_string(", ");
      }

      // add last element of array
      if (arr->get(arr->size() - 1)) barr->push_string("true");
      else barr->push_string("false");

      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

  BoolArray* get_bool_array(const char* str) {
      size_t arr_size, new_line_loc, i;

      BoolArray* barr = new BoolArray();

      // go through lines of str
      i = 0;
      while (i < strlen(str)) {
          //printf("%zu, %zu\n", i, strlen(str));
          // get the type of this line
          char type_buff[4];
          memcpy(type_buff, &str[i], 3);
          type_buff[3] = 0;
          // this is a sz line
          if (strcmp(type_buff, "siz") == 0) {
              i += 5;
              // find the end of the line
              for (size_t j = i; str[j] != '\n' && str[j] != 0; ++j) {
                  new_line_loc = j;
              }
              ++new_line_loc;
              // get the size
              char sz_buff[new_line_loc - i + 1];
              memcpy(sz_buff, &str[i], new_line_loc - i);
              sz_buff[new_line_loc - i] = 0;
              arr_size = atoi(sz_buff);
              i = new_line_loc + 1;
          // this is an ar line
          } else if (strcmp(type_buff, "arr") == 0) {
              // get the double array line
              char* ar_line = duplicate(&str[i + 5]);
              i += 5;
              // split into tokens and add to darr
              char* next_bool;
              next_bool = strtok(ar_line, ", ");
              while (next_bool != NULL) {
                  i += strlen(next_bool) + 2;
                  barr->push_back(atof(next_bool));
                  next_bool = strtok(NULL, ", ");
              }
              delete[] ar_line;
          }
      }

      // make sure sizes match up
      assert(arr_size == barr->size());

      return barr;
  }

  const char* serialize(IntArray* iarr) {
      ByteArray* barr = new ByteArray();

      // serialize size
      barr->push_string("siz: ");
      const char* ser_sz = serialize(iarr->size());
      barr->push_string(ser_sz);
      delete[] ser_sz;

      // serialize array
      barr->push_string("\narr: ");
      for (size_t i = 0; i < iarr->size() - 1; ++i) {
          const char* ser_int = serialize((double)iarr->get(i));
          barr->push_string(ser_int);
          barr->push_string(", ");
          delete[] ser_int;
      }

      // add last element of array
      const char* ser_int = serialize((double)iarr->get(iarr->size() - 1));
      barr->push_string(ser_int);
      delete[] ser_int;

      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

  IntArray* get_int_array(const char* str) {
      size_t arr_size, new_line_loc, i;

      IntArray* iarr = new IntArray();

      // go through lines of str
      i = 0;
      while (i < strlen(str)) {
          //printf("%zu, %zu\n", i, strlen(str));
          // get the type of this line
          char type_buff[4];
          memcpy(type_buff, &str[i], 3);
          type_buff[3] = 0;
          // this is a sz line
          if (strcmp(type_buff, "siz") == 0) {
              i += 5;
              // find the end of the line
              for (size_t j = i; str[j] != '\n' && str[j] != 0; ++j) {
                  new_line_loc = j;
              }
              ++new_line_loc;
              // get the size
              char sz_buff[new_line_loc - i + 1];
              memcpy(sz_buff, &str[i], new_line_loc - i);
              sz_buff[new_line_loc - i] = 0;
              arr_size = atoi(sz_buff);
              i = new_line_loc + 1;
          // this is an ar line
          } else if (strcmp(type_buff, "arr") == 0) {
              // get the int array line
              char* ar_line = duplicate(&str[i + 5]);
              i += 5;
              // split into tokens and add to darr
              char* next_int;
              next_int = strtok(ar_line, ", ");
              while (next_int != NULL) {
                  i += strlen(next_int) + 2;
                  iarr->push_back(atof(next_int));
                  next_int = strtok(NULL, ", ");
              }
              delete[] ar_line;
          }
      }

      // make sure sizes match up
      assert(arr_size == iarr->size());

      return iarr;
  }

  /**
    * TODO
    */
  const char* serialize(Key* key) {
    return nullptr;
  }

  const char* serialize(KeyArray* keys) {
      ByteArray* barr = new ByteArray();

      // serialize each key
      for (size_t i = 0; i < keys->size(); ++i) {
          barr->push_string("\t\t\tkey:\n");
          const char* ser_key = serialize(keys->get(i));
          barr->push_string(ser_key);
          if (i != keys->size() - 1) barr->push_back('\n');
          delete ser_key;
      }

      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

};
