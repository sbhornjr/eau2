// lang::CwC

#pragma once
#include "../object.h"
#include "../string.h"
#include "../array.h"
#include <netinet/in.h>

enum class MsgKind {Ack='a', Nack='n', Put='p',
                    Reply='r',  Get='g', WaitAndGet='w', Status='s',
                    Kill='k',   Register='t',  Directory='d', Text='x'};

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
    Ack(size_t target, size_t id): Message(MsgKind::Ack, 0, target, id) {}
};

class Status : public Message {
public:
   String* msg_; // owned

   Status(size_t target, size_t id, const char* msg)
   : Message(MsgKind::Status, 0, target, id), msg_(new String(msg)) {}

   ~Status() {
     delete msg_;
   }
};

class Text : public Message {
public:
   String* msg_; // owned
   String* senderip_; // owned

   Text(size_t target, size_t id, const char* msg, const char* ip)
   : Message(MsgKind::Text, 0, target, id), msg_(new String(msg)),
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

    Register(size_t target, size_t id, struct sockaddr_in client, size_t port)
    : Message(MsgKind::Register, 0, target, id), client_(client), port_(port) {}
};

class Directory : public Message {
public:
   size_t clients_;
   size_t * ports_;  // owned
   StringArray* addresses_;  // owned; strings owned

   Directory(size_t target, size_t id, size_t clients, size_t* ports, StringArray* addresses)
   : Message(MsgKind::Directory, 0, target, id), clients_(clients) {
       ports_ = new size_t[clients];
       addresses_ = new StringArray();
       for (size_t i = 0; i < clients; ++i) {
           ports_[i] = ports[i];
           String* adr = new String(addresses->get(i)->c_str());
           addresses_->push_back(adr);
           delete adr;
       }
   }

   ~Directory() {
     delete[] ports_;
     delete addresses_;
   }
};

class Kill : public Message {
public:
    Kill(size_t target, size_t id): Message(MsgKind::Kill, 0, target, id) {}
};
