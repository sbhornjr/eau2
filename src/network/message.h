// lang::CwC

#pragma once
#include "object.h"
#include "string.h"
#include "array.h"
#include "serial.h"
#include <netinet/in.h>

enum class MsgKind {Ack='a', Nack='n', Put='p',
                    Reply='r',  Get='g', WaitAndGet='w',
                    Kill='k',   Register='t',  Directory='d',
                    Text='x'};

class Message : public Object {
public:

    MsgKind kind_;  // the message kind
    size_t sender_; // the index of the sender node
    size_t target_; // the index of the receiver node
    size_t id_;     // an id t unique within the node

    Message(MsgKind kind, size_t sender, size_t target, size_t id) {
        kind_ = kind;
        sender_ = sender;
        target_ = target;
        id_ = id;
    }

    MsgKind get_kind() {
        return kind_;
    }
};

class Ack : public Message {
public:
    Ack(size_t sender, size_t target, size_t id)
    : Message(MsgKind::Ack, sender, target, id) {}
};

class Text : public Message {
public:
   String* msg_; // owned
   String* senderip_; // owned

   Text(size_t sender, size_t target, size_t id, const char* msg, const char* ip)
   : Message(MsgKind::Text, sender, target, id), msg_(new String(msg)),
             senderip_(new String(ip)) {}

   ~Text() {
     delete msg_;
     delete senderip_;
   }
};

class Register : public Message {
public:
    struct sockaddr_in client_;
    size_t port_;

    Register(size_t sender, size_t target, size_t id, struct sockaddr_in client, size_t port)
    : Message(MsgKind::Register, sender, target, id), client_(client), port_(port) {}
};

class Directory : public Message {
public:
   size_t clients_;
   size_t * ports_;  // owned
   StringArray* addresses_;  // owned; strings owned

   Directory(size_t sender, size_t target, size_t id, size_t clients, size_t* ports, StringArray* addresses)
   : Message(MsgKind::Directory, sender, target, id), clients_(clients) {
       ports_ = new size_t[clients];
       addresses_ = new StringArray();
       for (size_t i = 0; i < clients; ++i) {
           ports_[i] = ports[i];
           String* adr = new String(addresses->get(i)->c_str());
           addresses_->push_back(adr);
       }
   }

   ~Directory() {
     delete[] ports_;
     addresses_->delete_all();
     delete addresses_;
   }
};

class Kill : public Message {
public:
    Kill(size_t sender, size_t target, size_t id)
    : Message(MsgKind::Kill, sender, target, id) {}
};

class Get : public Message {
public:
    size_t idx;

    Get(size_t sender, size_t target, size_t id, size_t idx_)
    : Message(MsgKind::Get, sender, target, id) {
        idx = idx_;
    }
};

class WaitAndGet : public Message {
public:
    size_t idx;

    WaitAndGet(size_t sender, size_t target, size_t id, size_t idx_)
    : Message(MsgKind::WaitAndGet, sender, target, id) {
        idx = idx_;
    }
};

class Put : public Message {
public:
    Key* key;
    const char* value;

    Put(size_t sender, size_t target, size_t id, Key* key_, const char* value_)
    : Message(MsgKind::Put, sender, target, id) {
        key = key_;
        value = value_;
    }
};

/**
  * Serializes Message types.
  * @authors armani.a@husky.neu.edu, horn.s@husky.neu.edu
  */
class MessageSerializer : public Serializer {
public:

  const char* serialize(Message* msg) {
      MsgKind kind = msg->get_kind();
      ByteArray* barr = new ByteArray();

      // base message elements: kind, sender, target, id
      char buff[50];
      memset(buff, 0, 50);
      sprintf(buff, "knd: %c\nsnd: %zu\ntgt: %zu\nidx: %zu",
              (char)kind, msg->sender_, msg->target_, msg->id_);
      barr->push_string(buff);

      // call the correct sub-serializer
      if (kind == MsgKind::Register) {
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
      else if (kind == MsgKind::Register) return get_register_(&str[i], msg);
      else if (kind == MsgKind::Directory) return get_directory_(&str[i], msg);
      else if (kind == MsgKind::Kill) return get_kill_(&str[i], msg);
      else if (kind == MsgKind::Text) return get_text_(&str[i], msg);
      return msg;
  }

  Message* get_ack_(const char* str, Message* msg) {
      assert(msg->kind_ == MsgKind::Ack);

      // make Ack object
      Ack* ack = new Ack(msg->sender_, msg->target_, msg->id_);

      delete msg;
      return ack;
  }

  Message* get_kill_(const char* str, Message* msg) {
      assert(msg->kind_ == MsgKind::Kill);

      // make Kill object
      Kill* kill = new Kill(msg->sender_, msg->target_, msg->id_);

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
              delete[] msg_line;
          }
          // this is a source
          else if (strcmp(type_buff, "src") == 0) {
              i += 5;
              char* ip_line = duplicate(&str[i]);
              const char* msg = strtok(ip_line, "\"");
              src = get_string(msg);
              i += strlen(msg) + 3;
              delete[] ip_line;
          }
      }

      // Make Text Object
      Text* text = new Text(msg->sender_, msg->target_, msg->id_,
                          message->c_str(), src->c_str());

      delete msg;
      delete message;
      delete src;
      return text;
  }

  const char* serialize_(Text* t) {
      ByteArray* barr = new ByteArray();

      // serialize the message
      barr->push_string("\nmsg: ");
      const char* ser_msg = Serializer::serialize(t->msg_);
      barr->push_string(ser_msg);
      delete[] ser_msg;

      // serialize the message
      barr->push_string("\nsrc: ");
      const char* ser_ip = Serializer::serialize(t->senderip_);
      barr->push_string(ser_ip);
      delete[] ser_ip;

      const char* str = barr->as_bytes();
      delete barr;
      return str;
  }

  const char* serialize_(Register* reg) {
      ByteArray* barr = new ByteArray();

      // sockaddr_in serialization
      barr->push_string("\nclt:\n");
      const char* ser_clt = Serializer::serialize(reg->client_);
      barr->push_string(ser_clt);
      delete[] ser_clt;

      // serialize the port
      barr->push_string("\nprt: ");
      const char* ser_prt = Serializer::serialize(reg->port_);
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

      Register* reg = new Register(msg->sender_, msg->target_, msg->id_, client, port);

      delete msg;

      return reg;
  }

  const char* serialize_(Directory* dir) {
      ByteArray* barr = new ByteArray();

      // serialize the number of clients
      barr->push_string("\ncls: ");
      const char* ser_cls = Serializer::serialize(dir->clients_);
      barr->push_string(ser_cls);
      delete[] ser_cls;

      // serialize the array of ports
      barr->push_string("\npts: ");
      for (size_t i = 0; i < dir->clients_ - 1; ++i) {
          const char* ser_prt = Serializer::serialize(dir->ports_[i]);
          barr->push_string(ser_prt);
          barr->push_back(' ');
          delete[] ser_prt;
      }

      // add last port
      const char* ser_prt = Serializer::serialize(dir->ports_[dir->clients_ - 1]);
      barr->push_string(ser_prt);
      delete[] ser_prt;
      barr->push_back('\n');

      // serialize the array of addresses
      const char* addrs = Serializer::serialize(dir->addresses_);
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
              }
              delete[] ar_line;
          }
      }

      Directory* dir = new Directory(msg->sender_, msg->target_, msg->id_,
                                  clients, ports, sarr);

      delete msg;
      delete[] ports;
      sarr->delete_all();
      delete sarr;

      return dir;
  }

};
