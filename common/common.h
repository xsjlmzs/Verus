#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>
#include <queue>
#include <fstream>
#include <zmqpp/zmqpp.hpp>

#include "message.pb.h"
#include "utils.h"

#define XLOGE printf
#define XLOGI printf

struct Node
{
    int node_id;
    int replica_id;
    int partition_id; // not used

    std::string host;
    int port;

    void Print()
    {
        std::cout << "node_id = " << node_id << '\n'
                  << "replica_id = " << replica_id << '\n'
                  << "partition_id = " << partition_id << '\n'
                  << "host = " << host << '\n'
                  << "port = " << port << std::endl;
    }

    std::string GetSocket()
    {
        return host + ':' + std::to_string(port);
    }
};

class Configuration
{
private:
    /* data */
    int ReadServers(const std::string& path);
    int ReadProxy(const std::string& path);
public:
    int replica_size_;
    int replica_num_;

    std::map<uint32, Node*> all_nodes_; 
    Node* proxy_;

    std::string servers_config_file_;
    std::string proxy_config_file_;
    // std::map<int, int> replica_size; // <replica_id, size>
    // std::map<std::pair<int, int>, int> node_ids; // <<replica_id, partition_id>, node_id>
    Configuration(const std::string spath, std::string ppath);
    ~Configuration();
};

class Connection
{
private:
    Configuration* config_;

    AtomicMap<string, AtomicQueue<PB::MessageProto>*> channel_results_;

    AtomicQueue<string>* new_channel_queue_;

    AtomicQueue<string>* delete_channel_queue_;

    AtomicQueue<PB::MessageProto>* send_message_queue_;

    unordered_map<std::string, std::vector<PB::MessageProto> > undelivered_messages_;

    zmqpp::context cxt_;
    int remote_port_; // listen in
    zmqpp::socket* remote_in_; // remote node in
    std::map<uint32, zmqpp::socket*> remote_out_;

    // not used
    int client_port_;
    zmqpp::socket* client_resp_;

    bool deconstructor_invoked_;

    std::thread thread_;

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
public:
    explicit Connection(Configuration* config, uint32 node_id);
    ~Connection();

    void Run();
    void ListenClientThread();

    void NewChannel(std::string channel);
    void DeleteChannel(std::string channel);
    bool GetMessage(const std::string& channel, PB::MessageProto* msg);
    void Send(const PB::MessageProto& msg);

    std::queue<PB::ClientRequest> client_reqs_;
    std::queue<PB::ClientReply> replies_;
};

std::vector<std::string> split(const std::string&, char);
PB::Command PackCommand(const std::string&, const std::string&, const std::string&);
PB::OpType GetOpType(const std::string&);
bool OpenFile(const std::string&, std::ifstream&);



#endif