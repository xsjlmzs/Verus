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

    Configuration* config_;
    uint32 mp_percent_;
    Tpcc tpcc;

public:
    Client(Configuration* config, uint32 mtxn_percent, uint32 warehouse);
    ~Client();

    void Run();
    void LoadConfig(std::string filename);
    void NewTxn(PB::Txn** txn, uint64 txn_id);
};


#endif
