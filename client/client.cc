
#include <random>

#include "client.h"

Client::Client(Configuration* config, uint32 mp, uint32 warehouse)
    : config_(config), mp_percent_(mp), tpcc(config, warehouse)
{
    
}

Client::~Client()
{
}

PB::Txn CmdsToTxn(const std::vector<std::vector<std::string>>& cmds)
{
    static int counter = 0;
    PB::Txn txn;
    for (const std::vector<std::string> &cmd : cmds)
    {
        PB::Command* msg_cmd = txn.add_commands();
        if (cmd[0] == "GET")
        {
            msg_cmd->set_type(PB::OpType::GET);
            msg_cmd->set_key(cmd[1]);
        }
        else if (cmd[0] == "PUT")
        {
            msg_cmd->set_type(PB::OpType::PUT);
            msg_cmd->set_key(cmd[1]);
            msg_cmd->set_value(cmd[2]);
        }
        else
        {
            LOG(ERROR) << "unsupport operation";
            continue;
        }
    }
    txn.set_txn_id(counter++);
    return txn;
}

void Client::Run() 
{
    std::vector<std::vector<std::string>> commands; 
    bool in_txn = false;
    while (true)
    {
        std::cout << "command>> ";

        std::string input;
        std::getline(std::cin, input);

        std::vector<std::string>&& command = split(input, ' ');
        
        if (command.empty())
        {
            LOG(ERROR) << "command is empty";
            continue;
        }

        if (command[0] == "GET")
        {
            std::string key = command[1];
            commands.push_back(command);
        }
        else if (command[0] == "PUT")
        {
            std::string key = command[1];
            std::string value = command[2];
            commands.push_back(command);
        }
        else if (command[0] == "BEGIN")
        {
            LOG(INFO) << "receive BEGIN command, transaction begin";
            in_txn = true;
        }
        else if (command[0] == "END")
        {
            in_txn = false;
        }
        else if (command[0] == "DELETE")
        {
            //TODO :
            continue;
        }
        else if (command[0] == "EXIT")
        {
            break;
        }
        else
        {
            LOG(ERROR) << "unsupprt operation";
            continue;
        }

        if (!in_txn)
        {
            PB::Txn&& txn = CmdsToTxn(commands);
            // send txn
            // 
            commands.clear();
        }
    }
    
}

void Client::LoadConfig(std::string filename)
{

    std::ifstream address;
    if (!OpenFile(filename, address))
    {
        LOG(ERROR) << "load server config error";
        return ;
    }
    
    std::string ip_line;
    while (std::getline(address, ip_line))
    {
        if (ip_line.empty() || ip_line[0]=='#')
        {
            continue;
        }
        
        std::vector<std::string> ip_port = split(ip_line, ':');
        if (ip_port.size() != 2)
        {
            LOG(ERROR) << "wrong ip format";
        }
        else
        {
            std::string ip = ip_port[0]; int port = std::stoi(ip_port[1]);
            Node node;
            node.host = ip; node.port = port;
            servers_.push_back(node);
        }
    }
}

void Client::NewTxn(PB::Txn** txn, uint64 txn_id)
{
    uint32 parition_id = uint32(rand())%config_->replica_size_;
    if (config_->replica_size_ > 1 && (uint32)(rand() % 100) < mp_percent_)
    {
        uint64 other;
        do {
            other = (uint32) (rand() % config_->replica_size_);
        } while (other == parition_id);
        *txn = tpcc.TpccTxnMP(txn_id, parition_id, other);
    }
    else
    {
        *txn = tpcc.TpccTxnSP(txn_id,parition_id); // attention!
    }
}

// not used
void GetRandomKey(std::set<uint64>* keys, uint32 begin, uint32 end, uint16 num)
{
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<uint32> uniform_dist(begin, end);
    for (uint16 i = 0; i < num; i++)
    {
        uint32 key = 0;
        do
        {
            key = uniform_dist(gen);
        } while (keys->count(key));
        keys->insert(key);
    }
}
