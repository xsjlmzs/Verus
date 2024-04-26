#ifndef PROXY_H
#define PROXY_H

#include "common.h"
#include "client.h"
#include "consistent_hash.hpp"

#include <functional>

class Proxy
{
private:
    Configuration* config_;
    Connection* conn_;
    Client* client_;
    consistent_hash_map<uint32, std::hash<std::string> > key_hash;
    consistent_hash_map<uint32, std::hash<std::string> > node_hash;

public:
    Proxy(Configuration* config, Connection* conn, Client* client);
    ~Proxy();

    void Run();
    uint64 GenerateTid();
};


#endif
