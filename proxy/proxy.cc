#include "proxy.h"

Proxy::Proxy(Configuration* config, Connection* conn, Client* client)
    : config_(config), conn_(conn),client_(client)
{
    deconstructor_invoked_ = false;
    proxy_id_ = config_->proxy_->node_id;

    LOG(INFO) << "Proxy init Start";
    HeartBeat();
    LOG(INFO) << "Proxy init End";
    worker_ = std::thread(&Proxy::Run, this);
}

Proxy::~Proxy()
{
}

void Proxy::HeartBeat()
{
    std::string channel = "Heartbeat";
    conn_->NewChannel(channel);
    PB::MessageProto sync_msg;
    sync_msg.set_dest_channel(channel);
    sync_msg.set_src_channel(channel);
    sync_msg.set_src_node_id(proxy_id_);

    for (std::map<uint32, Node*>::iterator iter = config_->all_nodes_.begin();
        iter != config_->all_nodes_.end(); ++iter)
    {
        uint32 remote_server_id = iter->first;
        sync_msg.set_dest_node_id(remote_server_id);
        conn_->Send(sync_msg);
    }
    
    conn_->DeleteChannel(channel);
}

uint32 Proxy::Hash(std::string key)
{
    int nparts = config_->replica_size_;
    uint64 key_uint64 = StringToUInt64(key);
    return uint32 (key_uint64 % uint64(nparts));
}

void Proxy::Run()
{
    std::string channel = "Proxy";
    conn_->NewChannel(channel);
    std::vector<PB::Txn*> subtxns(config_->replica_size_, nullptr);
    while (!deconstructor_invoked_)
    {
        PB::Txn* txn = nullptr;
        client_->NewTxn(&txn, GenerateTid());
        bool read_only_txn = true;
        std::unordered_set<uint32> related_nodes;
        // distribute
        for (size_t i = 0; i < txn->commands_size(); i++)
        {
            PB::Command cmd = txn->commands(i);
            if (cmd.type() == PB::OpType::PUT)
            {
                read_only_txn = false;
            }
            uint32 server_id = Hash(cmd.key());
            if (subtxns[server_id] == nullptr)
            {
                subtxns[server_id] = new PB::Txn();
                subtxns[server_id]->set_txn_id(txn->txn_id());
                subtxns[server_id]->set_status(PB::TxnStatus::PEND);
            }
            subtxns[server_id]->mutable_commands()->Add(std::move(cmd));
            related_nodes.insert(server_id);
        }

        // send
        for (size_t i = 0; i < subtxns.size(); i++)
        {
            if (subtxns[i] != nullptr)
            {
                for (uint32 node_id : related_nodes)
                {
                    txn->add_related_nodes(node_id);
                }
                txn->add_received_nodes(i);
                
                PB::MessageProto mp;
                mp.set_type(PB::MessageProto_MessageType_SINGLETXN);
                mp.set_allocated_single_txn(subtxns[i]);
                // delete subtxns[i];
                subtxns[i] == nullptr;
                mp.set_src_node_id(proxy_id_);
                mp.set_src_channel(channel);
                mp.set_dest_node_id(i);
                mp.set_dest_channel(channel);
                conn_->Send(mp);
            }
        }
    }
}

uint64 Proxy::GenerateTid()
{
    uint64 tid = GetTime();
    return tid;
}

void Proxy::Join()
{
    worker_.join();
}
