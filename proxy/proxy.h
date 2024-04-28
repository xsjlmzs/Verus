#ifndef PROXY_H
#define PROXY_H

#include "common.h"
#include "client.h"
#include "utils.h"
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
    bool deconstructor_invoked_;

    uint32 proxy_id_;
    std::thread worker_;
public:
    Proxy(Configuration* config, Connection* conn, Client* client, uint32 proxy_id);
    ~Proxy();

    void HeartBeat();
    uint32 Hash(std::string key);
    void Run();
    uint64 GenerateTid();
    void Join();
};


#endif
