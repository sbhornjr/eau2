// lang::CwC

#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "object.h"
#include "string.h"
#include "array.h"
#include "network/message.h"

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

    const char* serialize(float f) {
        ByteArray* barr = new ByteArray();
        char flt[16];
        sprintf(flt, "%f", f);
        barr->push_string(flt);
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
            delete[] ser_str;
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
                    delete new_str;
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

    const char* serialize(FloatArray* farr) {
        ByteArray* barr = new ByteArray();

        // serialize size
        barr->push_string("siz: ");
        const char* ser_sz = serialize(farr->size());
        barr->push_string(ser_sz);
        delete[] ser_sz;

        // serialize array
        barr->push_string("\narr: ");
        for (size_t i = 0; i < farr->size() - 1; ++i) {
            const char* ser_flt = serialize(farr->get(i));
            barr->push_string(ser_flt);
            barr->push_string(", ");
            delete[] ser_flt;
        }

        // add last element of array
        const char* ser_flt = serialize(farr->get(farr->size() - 1));
        barr->push_string(ser_flt);
        delete[] ser_flt;

        const char* str = barr->as_bytes();
        delete barr;
        return str;
    }

    FloatArray* get_float_array(const char* str) {
        size_t arr_size, new_line_loc, i;

        FloatArray* farr = new FloatArray();

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
                // get the float array line
                char* ar_line = duplicate(&str[i + 5]);
                i += 5;
                // split into tokens and add to farr
                char* next_float;
                next_float = strtok(ar_line, ", ");
                while (next_float != NULL) {
                    i += strlen(next_float) + 2;
                    farr->push_back(atof(next_float));
                    next_float = strtok(NULL, ", ");
                }
                delete[] ar_line;
            }
        }

        // make sure sizes match up
        assert(arr_size == farr->size());

        return farr;
    }

    const char* serialize(Message* msg) {
        MsgKind kind = msg->get_kind();
        ByteArray* barr = new ByteArray();

        // base message elements: kind, sender, target, id
        char buff[50];
        memset(buff, 0, 50);
        sprintf(buff, "knd: %c\nsnd: %zu\ntgt: %zu\nidx: %zu",
                kind, msg->sender_, msg->target_, msg->id_);
        barr->push_string(buff);

        // call the correct sub-serializer
        if (kind == MsgKind::Status) {
            const char* ser_stat = serialize_(dynamic_cast<Status*>(msg));
            barr->push_string(ser_stat);
            delete[] ser_stat;
        }
        else if (kind == MsgKind::Register) {
            const char* ser_reg = serialize_(dynamic_cast<Register*>(msg));
            barr->push_string(ser_reg);
            delete[] ser_reg;
        }
        else if (kind == MsgKind::Directory) {
            const char* ser_dir = serialize_(dynamic_cast<Directory*>(msg));
            barr->push_string(ser_dir);
            delete[] ser_dir;
        }
        else if(kind == MsgKind::Text) {
            const char* ser_text = serialize_(dynamic_cast<Text*>(msg));
            barr->push_string(ser_text);
            delete[] ser_text;
        }

        const char* str = barr->as_bytes();
        delete barr;
        return str;
    }

    Message* get_message(const char* str) {
        MsgKind kind;
        size_t sender, target, idx, new_line_loc, i;

        // go through lines of str
        i = 0;
        while (i < strlen(str)) {
            // get the type of this line
            char type_buff[4];
            memcpy(type_buff, &str[i], 3);
            type_buff[3] = 0;
            // this is a kind line
            if (strcmp(type_buff, "knd") == 0) {
                i += 5;
                // get the kind
                kind = (MsgKind)str[i];
                // find the end of the line
                for (size_t j = i; str[j] != '\n' && str[j] != 0; ++j) {
                    new_line_loc = j;
                }
                ++new_line_loc;
                i = new_line_loc + 1;
            }
            // this is a snd line
            else if (strcmp(type_buff, "snd") == 0) sender = get_size(&str[i], &i);
            // this is a tgt line
            else if (strcmp(type_buff, "tgt") == 0) target = get_size(&str[i], &i);
            // this is an idx line
            else if (strcmp(type_buff, "idx") == 0) idx = get_size(&str[i], &i);
            else break;
        }

        // create base message object
        Message* msg = new Message(kind, sender, target, idx);

        // get derived message object and return
        if (kind == MsgKind::Ack) return get_ack_(&str[i], msg);
        else if (kind == MsgKind::Status) return get_status_(&str[i], msg);
        else if (kind == MsgKind::Register) return get_register_(&str[i], msg);
        else if (kind == MsgKind::Directory) return get_directory_(&str[i], msg);
        else if (kind == MsgKind::Kill) return get_kill_(&str[i], msg);
        else if (kind == MsgKind::Text) return get_text_(&str[i], msg);
        return msg;
    }

    Message* get_ack_(const char* str, Message* msg) {
        assert(msg->kind_ == MsgKind::Ack);

        // make Ack object
        Ack* ack = new Ack(msg->target_, msg->id_);

        // set sender correctly
        ack->sender_ = msg->sender_;

        delete msg;
        return ack;
    }

    Message* get_kill_(const char* str, Message* msg) {
        assert(msg->kind_ == MsgKind::Kill);

        // make Kill object
        Kill* kill = new Kill(msg->target_, msg->id_);

        // set sender correctly
        kill->sender_ = msg->sender_;

        delete msg;
        return kill;
    }

    Message* get_text_(const char* str, Message* msg) {
      assert(msg->kind_ == MsgKind::Text);

      // go through lines of str
      size_t i = 0;
      String* message;
      String* src;
      while (i < strlen(str)) {
          // get the type of this line
          char type_buff[4];
          memcpy(type_buff, &str[i], 3);
          type_buff[3] = 0;
          // this is a message line
          if (strcmp(type_buff, "msg") == 0) {
            i += 5;
            char* msg_line = duplicate(&str[i]);
            const char* msg = strtok(msg_line, "\"");
            message = get_string(msg);
            i += strlen(msg) + 3;
          }
          // this is a source
          else if (strcmp(type_buff, "src") == 0) {
            i += 5;
            char* ip_line = duplicate(&str[i]);
            const char* msg = strtok(ip_line, "\"");
            src = get_string(msg);
            i += strlen(msg) + 3;
          }
      }

      // Make Text Object
      Text* text = new Text(msg->target_, msg->id_, message->c_str(), src->c_str());

      // set sender correctly
      text->sender_ = msg->sender_;

      delete msg;
      return text;
    }

    const char* serialize_(Text* t) {
      ByteArray* barr = new ByteArray();

      // serialize the message
      barr->push_string("\nmsg: ");
      const char* ser_msg = serialize(t->msg_);
      barr->push_string(ser_msg);
      delete[] ser_msg;

      // serialize the message
      barr->push_string("\nsrc: ");
      const char* ser_ip = serialize(t->senderip_);
      barr->push_string(ser_ip);
      delete[] ser_ip;
      
      const char* str = barr->as_bytes();
      delete barr;
      return str;
    }

    const char* serialize_(Status* stat) {
        ByteArray* barr = new ByteArray();
        barr->push_back('\n');

        // add the serialized message field
        barr->push_string("msg: ");
        const char* ser_msg = serialize(stat->msg_);
        barr->push_string(ser_msg);
        delete[] ser_msg;

        const char* str = barr->as_bytes();
        delete barr;
        return str;
    }

    Message* get_status_(const char* str, Message* msg) {
        assert(msg->kind_ == MsgKind::Status);

        // get the msg field of the Status
        char buff[strlen(&str[6])];
        memset(buff, 0, strlen(&str[6]));
        memcpy(buff, &str[6], strlen(&str[6]) - 1);

        // make Status object
        Status* stat = new Status(msg->target_, msg->id_, buff);

        // set sender correctly
        stat->sender_ = msg->sender_;

        delete msg;

        return stat;
    }

    const char* serialize_(Register* reg) {
        ByteArray* barr = new ByteArray();

        // sockaddr_in serialization
        barr->push_string("\nclt:\n");
        const char* ser_clt = serialize(reg->client_);
        barr->push_string(ser_clt);
        delete[] ser_clt;

        // serialize the port
        barr->push_string("\nprt: ");
        const char* ser_prt = serialize(reg->port_);
        barr->push_string(ser_prt);
        delete[] ser_prt;

        const char* str = barr->as_bytes();
        delete barr;
        return str;
    }

    Message* get_register_(const char* str, Message* msg) {
        assert(msg->kind_ == MsgKind::Register);

        struct sockaddr_in client;
        size_t i, port;

        // go through lines of str
        i = 0;
        while (i < strlen(str)) {
            // get the type of this line
            char type_buff[4];
            memcpy(type_buff, &str[i], 3);
            type_buff[3] = 0;
            // this is a client line
            if (strcmp(type_buff, "clt") == 0) {
                client = get_sockaddr_in(&str[i], &i);
            }
            // this is a port line
            else if (strcmp(type_buff, "prt") == 0) port = get_size(&str[i], &i);
        }

        Register* reg = new Register(msg->target_, msg->id_, client, port);
        reg->sender_ = msg->sender_;

        delete msg;

        return reg;
    }

    const char* serialize_(Directory* dir) {
        ByteArray* barr = new ByteArray();

        // serialize the number of clients
        barr->push_string("\ncls: ");
        const char* ser_cls = serialize(dir->clients_);
        barr->push_string(ser_cls);
        delete[] ser_cls;

        // serialize the array of ports
        barr->push_string("\npts: ");
        for (size_t i = 0; i < dir->clients_ - 1; ++i) {
            const char* ser_prt = serialize(dir->ports_[i]);
            barr->push_string(ser_prt);
            barr->push_back(' ');
            delete[] ser_prt;
        }
        // add last port
        const char* ser_prt = serialize(dir->ports_[dir->clients_ - 1]);
        barr->push_string(ser_prt);
        delete[] ser_prt;
        barr->push_back('\n');

        // serialize the array of addresses
        const char* addrs = serialize(dir->addresses_);
        char buff[strlen(addrs) - 7];
        memcpy(buff, &addrs[7], strlen(addrs) - 7);
        buff[strlen(addrs) - 7] = 0;
        barr->push_string(buff);
        delete[] addrs;

        const char* str = barr->as_bytes();
        delete barr;
        return str;
    }

    Message* get_directory_(const char* str, Message* msg) {
        assert(msg->kind_ == MsgKind::Directory);

        size_t clients, i, new_line_loc;
        size_t* ports;
        StringArray* sarr = new StringArray();

        // go through lines of str
        i = 0;
        while (i < strlen(str)) {
            // get the type of this line
            char type_buff[4];
            memcpy(type_buff, &str[i], 3);
            type_buff[3] = 0;
            // this is a kind line
            if (strcmp(type_buff, "cls") == 0) clients = get_size(&str[i], &i);
            // this is a pts line
            else if (strcmp(type_buff, "pts") == 0) {
                assert(clients);    // clients must come first
                // get the array line
                ports = new size_t[clients];
                i += 5;
                for (size_t j = i; str[j] != '\n' && j < strlen(str); ++j) {
                    new_line_loc = j;
                }
                ++new_line_loc;
                char ar_line[strlen(&str[i])];
                memset(ar_line, 0, strlen(&str[i]));
                memcpy(ar_line, &str[i], new_line_loc - i);
                size_t n = 0;
                // split into tokens and add to ports
                char* next_port;
                next_port = strtok(ar_line, " \n");
                while (next_port != NULL) {
                    ports[n] = atoi(next_port);
                    ++n;
                    i += strlen(next_port) + 1;
                    next_port = strtok(NULL, " ");
                }
            }
            // this is an arr line
            else if (strcmp(type_buff, "arr") == 0) {
                assert(clients);    // clients must come first
                // get the array line
                char* ar_line = duplicate(&str[i + 5]);
                i += 5;
                // split into tokens and add to addresses
                char* next_ip;
                next_ip = strtok(ar_line, "\", \n");
                while (next_ip != NULL) {
                    String* n_ip = new String(next_ip);
                    sarr->push_back(n_ip);
                    i += strlen(next_ip) + 4;
                    next_ip = strtok(NULL, "\", \n");
                    delete n_ip;
                }
                delete[] ar_line;
            }
        }

        Directory* dir = new Directory(msg->target_, msg->id_, clients, ports, sarr);
        dir->sender_ = msg->sender_;

        delete msg;
        delete[] ports;
        delete sarr;

        return dir;
    }
};
