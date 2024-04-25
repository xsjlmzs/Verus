# ifndef CLIENT_H
# define CLIENT_H

#include <string>
#include <vector>

#include <zmqpp/zmqpp.hpp>

#include "common.h"
#include "tpcc.h"
class Client
{
private:
    std::vector<Node> servers_;
    zmqpp::context cxt_;
    zmqpp::socket* client_socket_;

    Configuration* config_;
    uint32 percent_mp_;
    Tpcc tpcc;

public:
    Client(Configuration* config, uint32 mp, uint32 hot_records);
    ~Client();

    void Run();

    void SendClientRequest(const PB::Txn& txn);
    void LoadConfig(std::string filename);
};


#endif
