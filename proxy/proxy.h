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
    std::vector<std::thread> thread_list_;
public:
    Proxy(Configuration* config, Connection* conn, Client* client, uint32 proxy_id);
    ~Proxy();

    void HeartBeat();
    uint32 Hash(std::string key);
    void Work(uint32 thread_id);
    void Run();
};


#endif
