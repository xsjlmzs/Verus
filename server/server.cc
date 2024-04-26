
#include "server.h"

extern int thread_num;
extern uint32 epoch_length;
extern uint64 run_epoch;
extern taas::Isolation isol;
extern uint32 limit_txns;
namespace taas 
{
    Server::Server(Configuration *config, Connection *conn, uint32 node_id)
        :config_(config), conn_(conn), isolation(isol), limit_epoch_(run_epoch), limit_txns_(limit_txns),deconstructor_invoked_(false)
    {
        server_id_ = node_id;
        parition_id_ = node_id % config->replica_size_;
        replica_id_ = node_id / config_->replica_size_;
        storage_ = new Storage();
        epoch_manager_ = &EpochManager::GetInstance();
        thread_pool_ = new ThreadPool(thread_num);
        thread_pool_->init();
        // init 
        crdt_map_.resize(thread_num);
        // mutexes_local_txns_.resize(thread_num);
        local_txns_.resize(thread_num);
        remote_txns_.resize(thread_num);
        commit_txns_.resize(thread_num);
        abort_txns_.resize(thread_num);
        LOG(INFO) << "Start Sync All Servers";

        HeartbeatAllServers();

        LOG(INFO) << "Sync Servers Complete";

        worker_ = std::thread(&Server::Run, this);
        // listen_thread.join();
    }

    Server::~Server() 
    {
        deconstructor_invoked_ = true;
        delete storage_, thread_pool_;
    }

    void Server::Execute(PB::Txn* txn, uint64 cur_epoch_mod)
    {
        storage_->LockRead();
        txn->set_status(PB::TxnStatus::EXEC);
        for (size_t i = 0; i < txn->commands().size(); i++)
        {
            const PB::Command& stat = txn->commands(i);
            if (stat.type() == PB::OpType::GET)
            {
                std::string value = storage_->get(stat.key());
                txn->add_read_set(value);
            }
            else if (stat.type() == PB::OpType::PUT)
            {

                PB::Txn::KeyValue* kv = txn->add_write_set();
                kv->set_key(stat.key());
                kv->set_value(stat.value());
            }
        }
        storage_->UnlockRead();
        txn->set_end_ts(GetTime());
        txn->set_end_epoch(epoch_manager_->GetPhysicalEpoch());
        txn->set_status(PB::TxnStatus::PRECOMMIT);
        std::unique_lock<std::mutex> lk(mutexes_local_txns_[cur_epoch_mod]);
        local_txns_[cur_epoch_mod].push_back(*txn);
    }

    void Server::HeartbeatAllServers()
    {
        std::string channel = "Heartbeat";
        conn_->NewChannel(channel);
        PB::MessageProto sync_msg;
        sync_msg.set_type(PB::MessageProto_MessageType_HEARTBEAT);
        sync_msg.set_dest_channel(channel);
        sync_msg.set_src_node_id(server_id_);
        for (std::map<uint32, Node*>::iterator iter = config_->all_nodes_.begin();
            iter != config_->all_nodes_.end(); ++iter)
        {
            uint32 remote_server_id = iter->first;
            if (remote_server_id == server_id_)
                continue;
            sync_msg.set_dest_node_id(remote_server_id);
            conn_->Send(sync_msg);
        }
        
        // waiting for the replies from rest servers of cluster
        int sync_server_cnt = 1;
        // sync_msg.Clear();
        while (sync_server_cnt < config_->all_nodes_.size())
        {
            if(conn_->GetMessage(channel, &sync_msg))
            {
                sync_server_cnt++;
            }
            usleep(100);
        }
        // sync complete
        conn_->DeleteChannel("Heartbeat");
    }

    bool Server::WriteIntent(const PB::Txn& txn, uint64 index)
    {
        for (const auto &stat : txn.commands())
        {
            if (stat.type() == PB::OpType::PUT)
            {
                if (crdt_map_[index].count(stat.key()))
                {
                    // exist earlier record
                    const Metadata& prev_data = crdt_map_[index][stat.key()];
                    const Metadata cur_data
                    {
                        sen: txn.start_epoch(),
                        rna: (uint64)txn.related_nodes_size(),
                        csn: txn.txn_id()
                    };
                    if (prev_data.sen < txn.start_epoch())
                    {
                        crdt_map_[index][stat.key()] = cur_data;
                    }
                    else if (prev_data.sen == txn.start_epoch())
                    {
                        if (prev_data.rna > cur_data.rna)
                        {
                            crdt_map_[index][stat.key()] = cur_data;
                        }
                        else if (prev_data.rna == cur_data.rna)
                        {
                            if (prev_data.csn > cur_data.csn)
                            {
                                crdt_map_[index][stat.key()] = cur_data;
                            }
                            else
                            {
                                return false;
                            }
                            
                        }
                        else
                        {
                            return false;
                        }
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    const Metadata cur_data
                    {
                        sen: txn.start_epoch(),
                        rna: (uint64)txn.related_nodes_size(),
                        csn: txn.txn_id()
                    };
                    // write intent successfully
                    crdt_map_[index][stat.key()] = cur_data;
                }
            }
        }
        return true;
    }

    bool Server::ValidateWS(const PB::Txn& txn, uint64 index)
    {
        for (const auto &stat : txn.commands())
        {
            if (stat.type() == PB::OpType::PUT)
            {
                if (crdt_map_[index][stat.key()].csn == txn.txn_id())
                {
                    // don't eliminate
                }
                else
                {
                    return false;
                }
            }
        }
        return true;
    }

    bool Server::ValidateReadSet(const PB::Txn& txn)
    {
        bool validate_res = true;
        std::string last_read_version = "";
        for (auto &&stat : txn.commands())
        {
            if (stat.type() == PB::OpType::GET)
            {
                std::string read_version = stat.value();
                storage_->LockRead();
                std::string cur_version = storage_->get(stat.key());
                storage_->UnlockRead();
                switch (isolation)
                {
                case kReadCommit:
                    validate_res = true;
                    break;
                case kRepeatableRead:
                    if (cur_version != read_version)
                        validate_res = false;
                    break;
                case kSnapshotIsolation:
                    if (last_read_version.empty())
                        last_read_version = read_version;
                    else if(last_read_version != read_version)
                        validate_res = false;
                    break;
                case kSerilizable: // not support
                    break;
                default:
                    break;
                }
                if (cur_version != stat.value())
                    return false;
            }
            if (!validate_res)
                break;
        }
        return validate_res;
    }

    void Server::Run()
    {
        launch_ts_ = GetTime();
        while (!deconstructor_invoked_)
        {
            uint64 start_time = GetTime();
            uint64 cur_epoch = epoch_manager_->GetPhysicalEpoch();
            uint64 cur_epoch_mod = cur_epoch % thread_num;
            
            // reach max running epoch, exit
            if (cur_epoch == limit_epoch_+1)
            {
                std::unique_lock<std::mutex> lk(cv_mutex_);
                cv_.wait(lk, [this]{return this->epoch_manager_->GetCommittedEpoch() == this->limit_epoch_; });
                lk.unlock();
                thread_pool_->shutdown();
                break;
            }
            
            // start a epoch
            LOG(INFO) << "------ epoch "<< cur_epoch << " start ------";
            if (!local_txns_[cur_epoch_mod].empty())
            {
                local_txns_[cur_epoch_mod].clear();
            }
            
            while (GetTime() - start_time < epoch_manager_->GetEpochDuration())
            {
                if (local_txns_[cur_epoch_mod].size() > limit_txns_)
                {
                    // dont receive new txns anymore 
                    usleep(100);
                    continue;
                }
                PB::MessageProto *received_txn = nullptr;
                conn_->GetMessage("proxy", received_txn);
                received_txn->mutable_single_txn()->set_status(PB::TxnStatus::PEND);
                received_txn->mutable_single_txn()->set_start_ts(GetTime());
                thread_pool_->submit(std::bind(&Server::Execute, this, received_txn->mutable_single_txn(), cur_epoch_mod));
            }
            epoch_manager_->AddPhysicalEpoch();
            LOG(INFO) << "epoch : " << cur_epoch << " " << local_txns_[cur_epoch].size() << " txns collected, start distribute and merge";
            // process with all other shard peer
            // worker
            thread_pool_->submit(std::bind(&Server::Work, this, cur_epoch));
            LOG(INFO) << "------ epoch "<< cur_epoch << " end ------";

        }
    }

    // // deprecated
    // std::vector<PB::MessageProto>* Server::Distribute(const std::vector<PB::Txn>& local_txns, uint64 epoch)
    // {
    //     std::string channel = "Shard_" + std::to_string(epoch);
    //     conn_->NewChannel(channel);
    //     std::map<uint32, PB::MessageProto> batch_subtxns;
    //     // prepare msg for sending to in region nodes
    //     for (std::map<uint32, Node*>::const_iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
    //     {
    //         if (iter->second->replica_id != config_->replica_id_)
    //            continue;
    //         uint32 remote_server_id = iter->first;
    //         PB::MessageProto mp;
    //         mp.set_src_node_id(server_id_);
    //         mp.set_dest_node_id(remote_server_id);
    //         mp.set_dest_channel(channel);
    //         mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
    //         batch_subtxns[remote_server_id] = mp;
    //     }

    //     // split txn into subtxns
    //     for (size_t i = 0; i < local_txns.size(); i++)
    //     {
    //         const PB::Txn& txn = local_txns.at(i);
    //         std::map<uint32, PB::Txn> subtxns; // <node_id, subtxn>
    //         for (size_t j = 0; j < txn.commands_size(); j++)
    //         {
    //             const PB::Command& stat = txn.commands(j);
    //             // find responsible node for the key in region
    //             int partition_id = config_->LookupPartition(stat.key());
    //             uint32 machine_id = config_->LookupMachineID(partition_id);
    //             if (subtxns.count(machine_id) == 0)
    //             {
    //                 PB::Txn subtxn;
    //                 subtxn.set_txn_id(txn.txn_id());
    //                 subtxn.set_start_epoch(txn.start_epoch());
    //                 subtxn.set_status(PB::TxnStatus::PEND);
    //                 subtxns[machine_id] = subtxn;
    //             }
    //             subtxns[machine_id].add_commands()->CopyFrom(stat);
    //         }

    //         // compile subtxns to batch
    //         for(std::map<uint32, PB::Txn>::iterator iter = subtxns.begin(); iter != subtxns.end(); ++iter)
    //         {
    //             uint32 remote_server_id = iter->first;
    //             const PB::Txn& subtxn = iter->second;
    //             batch_subtxns[remote_server_id].mutable_batch_txns()->add_txns()->CopyFrom(subtxn);
    //         }   
    //     }

    //     // send batch_subtxns to all in-region peers
    //     for (std::map<uint32, PB::MessageProto>::iterator iter = batch_subtxns.begin(); iter != batch_subtxns.end(); ++iter)
    //     {
    //         // iter->second.set_debug_info(std::to_string(epoch));
    //         conn_->Send(iter->second);
    //     }
    //     LOG(INFO) << "epoch : " << epoch << " have sent " << batch_subtxns.size() << " Distribute() msgs and barrier";
    //     // barrier : wait for all other msg arrive
    //     int recv_msg_cnt = 0;
    //     PB::MessageProto recv_subtxn;
    //     std::vector<PB::MessageProto>* inregion_subtxns = new std::vector<PB::MessageProto>();
    //     while (recv_msg_cnt < config_->replica_size_)
    //     {
    //         if(conn_->GetMessage(channel, &recv_subtxn))
    //         {
    //             recv_msg_cnt++;
    //             inregion_subtxns->push_back(recv_subtxn);
    //         }
    //         else
    //         {
    //             usleep(100);
    //         }
    //     }
    //     LOG(INFO) << "epoch : " << epoch << " Distribute() barrier end"; 
    //     conn_->DeleteChannel(channel);
    //     return inregion_subtxns;
    // }

    // send in-region subtxn to all other region's peer node
    void  Server::Replicate(uint64 epoch)
    {
        std::string replicate_channel = std::string("replicate_") + std::to_string(epoch);
        conn_->NewChannel(replicate_channel);
        // replicate local txn to other replicas
        PB::MessageProto local_txns_mp;
        local_txns_mp.set_type(PB::MessageProto::BATCHTXNS);
        local_txns_mp.mutable_batch_txns()->set_commit_epoch(epoch);
        uint64 epoch_mod = epoch % thread_num;
        for (const PB::Txn& txn : local_txns_[epoch_mod])
        {
            local_txns_mp.mutable_batch_txns()->add_txns()->CopyFrom(txn);
        }
        for (size_t i = 0; i < config_->replica_num_; i++)
        {
            if (i != replica_id_)
            {
                local_txns_mp.set_src_node_id(server_id_);
                local_txns_mp.set_src_channel(replicate_channel);
                local_txns_mp.set_dest_node_id(i * config_->replica_size_ + parition_id_);
                local_txns_mp.set_dest_channel(replicate_channel);
                conn_->Send(local_txns_mp);
            }
        }
        // wait for other replica's broadcast
        uint32 have_replicated = 1; // iteself
        PB::MessageProto* msg = nullptr;
        while (have_replicated < config_->replica_num_)
        {
            if (conn_->GetMessage(replicate_channel, msg))
            {
                for (const PB::Txn& txn : msg->batch_txns().txns())
                {
                    remote_txns_[epoch_mod].push_back(txn);
                }
            }
            else
            {
                usleep(100);
            }
        }
    }

    // process crdt merge
    void Server::Merge(uint64 epoch)
    {
        std::string channel = "merge_" + std::to_string(epoch);
        conn_->NewChannel(channel);
        // write intent for local txns and remote txns
        uint64 epoch_mod = epoch % thread_num;
        for (PB::Txn& txn : local_txns_[epoch_mod])
        {
            bool res = WriteIntent(txn, epoch_mod);
            if (!res)
                txn.set_status(PB::TxnStatus::ABORT);
        }
        for (PB::Txn& txn : remote_txns_[epoch_mod])
        {
            bool res = WriteIntent(txn, epoch_mod);
            if (!res)
                txn.set_status(PB::TxnStatus::ABORT);
        }
        // wait & write intent for WaitTxns
        {
            std::shared_lock<std::shared_mutex> lk(mutex_wait_);
            for (PB::Txn& txn: wait_txns_)
            {
                bool res = WriteIntent(txn, epoch_mod);
                if (!res)
                    txn.set_status(PB::TxnStatus::ABORT);
            }
        }

        // validate write set
        for (PB::Txn& txn : local_txns_[epoch_mod])
        {
            if (txn.status() == PB::ABORT)
            {
                abort_txns_[epoch_mod][txn.txn_id()] = txn;
                continue;
            }
            if (ValidateWS(txn, epoch_mod))
            {
                txn.set_status(PB::TxnStatus::COMMIT);
                commit_txns_[epoch_mod][txn.txn_id()] = txn;
            }
            else
            {
                txn.set_status(PB::TxnStatus::ABORT);
                abort_txns_[epoch_mod][txn.txn_id()] = txn;
            }
        }
    }

    void Server::ValidateAtomic(uint64 epoch)
    {
        uint64 epoch_mod = epoch % thread_num;
        std::vector<std::vector<TxnRes> > send_buf;
        send_buf.resize(config_->all_nodes_.size());
        // arrange distributed txn's result
        for (const auto& kv : commit_txns_[epoch_mod])
        {
            const PB::Txn& txn = kv.second;
            if (txn.related_nodes().size() > 1) // distributed txn
            {
                for (size_t i = 0; i < txn.related_nodes().size(); i++)
                {
                    if (txn.related_nodes(i) != server_id_)
                    {
                        send_buf[txn.related_nodes(i)].push_back(TxnRes{txn.txn_id(), true});
                    }
                }
            }
        }
        for (const auto& kv : abort_txns_[epoch_mod])
        {
            const PB::Txn& txn = kv.second;
            if (txn.related_nodes().size() > 1) // distributed txn
            {
                for (size_t i = 0; i < txn.related_nodes().size(); i++)
                {
                    send_buf[txn.related_nodes(i)].push_back(TxnRes{txn.txn_id(), false});
                }
            }
        }
        
        // send all txn's result
        std::string channel = "validate_" + std::to_string(epoch);
        int related_nodes_size = 0;
        for (size_t i = 0; i < send_buf.size(); i++)
        {
            if (!send_buf[i].empty())
            {
                related_nodes_size ++;
                PB::MessageProto mp;
                mp.set_type(PB::MessageProto_MessageType_CATXNIDS);
                mp.set_src_node_id(server_id_);
                mp.set_src_channel(channel);
                mp.set_dest_node_id(i);
                mp.set_dest_channel(channel);
                for (const TxnRes &txn_res : send_buf[i])
                {
                    PB::CATxnIds::CATxnId txnres_pb;
                    txnres_pb.set_txn_id(txn_res.txn_id);
                    txnres_pb.set_txn_status(txn_res.commit ? PB::TxnStatus::COMMIT : PB::TxnStatus::ABORT);
                    mp.mutable_txn_result()->add_ca_txn_ids()->CopyFrom(txnres_pb);
                }
                conn_->Send(mp);
            }
        }

        // receive related nodes's msg
        int received_nodes_size = 0;
        PB::MessageProto* msg = nullptr;
        while (received_nodes_size < related_nodes_size)
        {
            if (conn_->GetMessage(channel, msg))
            {
                received_nodes_size ++;
                for (size_t i = 0; i < msg->txn_result().ca_txn_ids().size(); i++)
                {
                    uint64 txn_id = msg->txn_result().ca_txn_ids(i).txn_id();
                    bool commit = msg->txn_result().ca_txn_ids(i).txn_status() == PB::TxnStatus::COMMIT ? true : false;
                    auto iter = commit_txns_[epoch_mod].find(txn_id);
                    if (iter == commit_txns_[epoch_mod].end())
                        continue;
                    if (commit)
                        iter->second.mutable_received_nodes()->Add(msg->src_node_id());
                    else
                        iter->second.set_status(PB::TxnStatus::ABORT);
                }
            }
        }

        // process waiting txns
        for (const auto &kv : commit_txns_[epoch_mod])
        {
            const PB::Txn& txn = kv.second;
            if (txn.status() != PB::TxnStatus::ABORT)
            {
                if (txn.received_nodes_size() < txn.related_nodes_size())
                {
                    std::unique_lock<std::shared_mutex> lk(mutex_wait_);
                    wait_txns_.push_back(txn);
                }
            }
        }
    }

    void Server::EpochWrite(uint64 epoch)
    {
        // wait until last epoch's write completed
        {
            std::unique_lock<std::mutex> lk(cv_mutex_);
            cv_.wait(lk, [this, epoch]{return this->epoch_manager_->GetCommittedEpoch() == epoch-1; });
            lk.unlock();
        }
        uint64 epoch_mod = epoch % thread_num;
        for (const auto &kv : commit_txns_[epoch_mod])
        {
            const PB::Txn& txn = kv.second;
            if (txn.status() == PB::TxnStatus::COMMIT && !txn.read_only())
            {
                storage_->LockWrite();
                for (size_t i = 0; i < txn.write_set_size(); i++)
                {
                    storage_->put(txn.write_set(i).key(), txn.write_set(i).value());
                }
                storage_->UnlockWrite();
            }
        }
        
    }

    bool Server::CheckAtomic(const PB::Txn& txn, bool committed)
    {
        int total_write_cnt = 0, success_write_cnt = 0;
        for (auto &&stat : txn.commands())
        {
            if (stat.type() == PB::OpType::PUT)
            {
                total_write_cnt ++;
                std::string query_val =  storage_->get(stat.key());
                if (query_val == stat.value())
                {
                    success_write_cnt ++;
                }
            }
        }
        if (committed)
            return total_write_cnt == success_write_cnt ? true : false;
        else
            return success_write_cnt ? false : true;
    }
    void Server::PrintStatistic(uint32 epoch)
    {
        std::string filename = "./report." + UInt32ToString(server_id_) + "." + UInt32ToString(epoch);
        std::ofstream file(filename);
        std::string report;
        uint64 cur_lantency = 0;
        uint32 cur_txn_cnt = 0;
        uint32 abort_cnt = 0;
        for (size_t i = 0; i < local_txns_[epoch].size(); i++)
        {
            abort_cnt ++;
            if (local_txns_[epoch][i].status() == PB::TxnStatus::ABORT)
                continue;
            abort_cnt --;
            uint64 single_latency = local_txns_[epoch][i].end_ts() - local_txns_[epoch][i].start_ts();
            cur_lantency += single_latency;
            cur_txn_cnt ++;
        }
        LOG(ERROR) << "epoch: " << epoch << " abort_cnt: " << abort_cnt << " commit_cnt: " << cur_txn_cnt;
        {
            std::lock_guard<std::mutex> lock(cnt_mutex_);
            done_txn_cnt_ += cur_txn_cnt;
            done_total_latency_ += cur_lantency;
            // txns per second
            report.append("avg_throught : " + UInt64ToString(done_txn_cnt_ * 1000L / (GetTime() - launch_ts_)) + "\n");
            report.append("avg_lantency : " + UInt64ToString(done_total_latency_ / done_txn_cnt_) + "\n");
        }

        file << report;
    }
    // worker
    void Server::Work(uint64 epoch)
    {
        std::vector<std::pair<uint64, uint64>> latencies; // <begin ts, end ts>
        // Exec Read instantly subtxn in shard node
        // process replicate & collect all out-region subtxns
        LOG(INFO) << "epoch : " << epoch << " Start Replicate";
        // replicate subtxn and share 
        Replicate(epoch);
        // determinstic process merge
        // return value : kvs all will write in db 
        LOG(INFO) << "epoch : " << epoch << " Start Merge";
        Merge( epoch);
        LOG(INFO) << "epoch : " << epoch << " Merge Finish";
        // atomic batch write in
        for (size_t i = 0; i < local_txns_[epoch].size(); i++)
            local_txns_[epoch][i].set_end_ts(GetTime());
        
        PrintStatistic(epoch);
        // Check Correctness
        #ifdef CHECK_ATOMIC
        std::set<uint64> committed_tid_set;
        for (size_t i = 0; i < committable_subtxns->size(); i++)
        {
            const PB::Txn& txn = committable_subtxns->at(i);
            committed_tid_set.insert(txn.txn_id());
        }
        bool atomic_test = true;
        for (auto &&subtxns : *inregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                bool part_res = CheckAtomic(subtxn, committed_tid_set.count(subtxn.txn_id()));
                atomic_test &= part_res;
                if (!part_res && committed_tid_set.count(subtxn.txn_id()))
                    LOG(ERROR) << "epoch : " << epoch << " committed but can't get again";
                else if(!part_res)
                    LOG(ERROR) << "epoch : " << epoch << " abort but can still get" << committed_tid_set.count(subtxn.txn_id());
            }
        }
        if (!atomic_test)
            LOG(ERROR) << "epoch : " << epoch << " inregion check failed";  
        for (auto &&subtxns : *outregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                bool part_res = CheckAtomic(subtxn, committed_tid_set.count(subtxn.txn_id()));
                atomic_test &= part_res;
            }
        }
        if (!atomic_test)
            LOG(ERROR) << "epoch : " << epoch << " cant pass the subtxn's atomic test";   
        #endif

        epoch_manager_->AddCommittedEpoch();
        cv_.notify_all();
    }

    void Server::Join()
    {
        worker_.join();
    }
} // namespace taas



