
#include "server.h"

extern uint32 thread_num;
extern uint32 buffer_size;
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
        crdt_map_.resize(buffer_size);
        // mutexes_local_txns_.resize(buffer_size);
        local_txns_.resize(buffer_size);
        remote_txns_.resize(buffer_size);
        commit_txns_.resize(buffer_size);
        abort_txns_.resize(buffer_size);
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

    void Server::Execute(PB::Txn* txn, uint64 epoch)
    {
        storage_->LockRead();
        txn->set_status(PB::TxnStatus::EXEC);
        for (size_t i = 0; i < txn->commands_size(); i++)
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
        // txn->set_end_ts(GetTime());
        txn->set_status(PB::TxnStatus::PRECOMMIT);
        {
            std::unique_lock<std::mutex> lk_epoch(add_epoch_mutex_);
            uint64 cur_epoch = epoch_manager_->GetPhysicalEpoch();
            txn->set_end_epoch(cur_epoch);
            uint64 epoch_mod = cur_epoch % buffer_size;
            std::unique_lock<std::mutex> lk_vector(mutexes_local_txns_[epoch_mod]);
            local_txns_[epoch_mod].push_back(*txn);
        }
        delete txn;
    }

    void Server::HeartbeatAllServers()
    {
        std::string channel = "Heartbeat";
        conn_->NewChannel(channel);
        PB::MessageProto sync_msg;
        sync_msg.set_type(PB::MessageProto_MessageType_HEARTBEAT);
        sync_msg.set_dest_channel(channel);
        sync_msg.set_src_channel(channel);
        sync_msg.set_src_node_id(server_id_);
        for (std::map<uint32, Node*>::iterator iter = config_->all_servers_.begin();
            iter != config_->all_servers_.end(); ++iter)
        {
            uint32 remote_server_id = iter->first;
            if (remote_server_id == server_id_)
                continue;
            sync_msg.set_dest_node_id(remote_server_id);
            conn_->Send(sync_msg);
        }
        
        // waiting for servers and proxy's replies
        int sync_server_cnt = 1;
        // sync_msg.Clear();
        while (sync_server_cnt < config_->all_servers_.size() + config_->all_proxies_.size())
        {
            if(conn_->GetMessage(channel, &sync_msg))
            {
                sync_server_cnt++;
            }
            {
                usleep(100);
            }
        }
        // sync complete
        conn_->DeleteChannel(channel);
    }

    bool Server::WriteIntent(const PB::Txn& txn, uint64 index)
    {
        for (const PB::Txn::KeyValue& kv : txn.write_set())
        {
            if (crdt_map_[index].find(kv.key()) != crdt_map_[index].end())
            {
                // exist earlier record
                const Metadata& prev_data = crdt_map_[index][kv.key()];
                const Metadata cur_data
                {
                    sen: txn.start_epoch(),
                    rna: (uint64)txn.related_nodes_size(),
                    csn: txn.txn_id()
                };
                if (prev_data.sen < txn.start_epoch())
                {
                    crdt_map_[index][kv.key()] = cur_data;
                }
                else if (prev_data.sen == txn.start_epoch())
                {
                    if (prev_data.rna > cur_data.rna)
                    {
                        crdt_map_[index][kv.key()] = cur_data;
                    }
                    else if (prev_data.rna == cur_data.rna)
                    {
                        if (prev_data.csn > cur_data.csn)
                        {
                            crdt_map_[index][kv.key()] = cur_data;
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
                crdt_map_[index][kv.key()] = cur_data;
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
        conn_->NewChannel("Proxy");
        while (!deconstructor_invoked_)
        {
            uint64 start_time = GetTime();
            uint64 cur_epoch = epoch_manager_->GetPhysicalEpoch();
            uint64 cur_epoch_mod = cur_epoch % buffer_size;
            
            // reach max running epoch, exit
            if (cur_epoch == limit_epoch_+1)
            {
                std::unique_lock<std::mutex> lk(cv_commit_mutex_);
                cv_commit_.wait(lk, [this]{return this->epoch_manager_->GetCommittedEpoch() == this->limit_epoch_; });
                lk.unlock();
                thread_pool_->shutdown();
                LOG(INFO) << "done!";
                break;
            }
            
            // start a epoch
            LOG(INFO) << "------ epoch "<< cur_epoch << " start ------";
            uint32 cnt = 0;
            while (GetTime() - start_time < epoch_manager_->GetEpochDuration())
            {
                if (local_txns_[cur_epoch_mod].size() > limit_txns_)
                {
                    // dont receive new txns anymore 
                    usleep(100);
                    continue;
                }
                PB::MessageProto received_txn;
                if (conn_->GetMessage("Proxy", &received_txn))
                {
                    cnt ++;
                    received_txn.mutable_single_txn()->set_status(PB::TxnStatus::PEND);
                    received_txn.mutable_single_txn()->set_start_epoch(cur_epoch);
                    received_txn.mutable_single_txn()->set_start_ts(GetTime());
                    // LOG(INFO) << "debug" << received_txn.single_txn().txn_id();
                    PB::Txn *txn = new PB::Txn(received_txn.single_txn());
                    std::thread t(&Server::Execute, this, txn, cur_epoch_mod);
                    t.detach(); // 独自执行
                }
            }
            LOG(INFO) << "epoch : " << cur_epoch << " " << "received " << cnt << " txns";
            {
                std::unique_lock<std::mutex> lk_epoch(add_epoch_mutex_);
                epoch_manager_->AddPhysicalEpoch();
            }
            {
                std::unique_lock<std::mutex> lk_vector(mutexes_local_txns_[cur_epoch_mod]);
                LOG(INFO) << "epoch : " << cur_epoch << " " << local_txns_[cur_epoch_mod].size() << " txns done";
            }
            // process with all other shard peer
            // worker
            usleep(1000);
            thread_pool_->submit(std::bind(&Server::Work, this, cur_epoch));
            LOG(INFO) << "------ epoch "<< cur_epoch << " end ------";
        }
        conn_->DeleteChannel("Proxy");
    }

    // send in-region subtxn to all other region's peer node
    void  Server::Replicate(uint64 epoch)
    {
        std::string replicate_channel = std::string("Replicate_") + std::to_string(epoch);
        conn_->NewChannel(replicate_channel);
        // replicate local txn to other replicas
        PB::MessageProto local_txns_mp;
        local_txns_mp.set_type(PB::MessageProto::BATCHTXNS);
        local_txns_mp.mutable_batch_txns()->set_commit_epoch(epoch);
        uint64 epoch_mod = epoch % buffer_size;
        for (PB::Txn txn : local_txns_[epoch_mod])
        {
            local_txns_mp.mutable_batch_txns()->mutable_txns()->Add(std::move(txn));
        }
        for (size_t i = 0; i < config_->replica_num_; i++)
        {
            if (i != replica_id_)
            {
                local_txns_mp.set_src_node_id(server_id_);
                local_txns_mp.set_dest_node_id(i * config_->replica_size_ + parition_id_);
                local_txns_mp.set_dest_channel(replicate_channel);
                conn_->Send(local_txns_mp);
            }
        }
        // wait for other replica's broadcast
        uint32 have_replicated = 1; // iteself
        PB::MessageProto msg;
        while (have_replicated < config_->replica_num_)
        {
            if (conn_->GetMessage(replicate_channel, &msg))
            {
                have_replicated++;
                for (const PB::Txn& txn : msg.batch_txns().txns())
                {
                    remote_txns_[epoch_mod].push_back(txn);
                }
            }
            else
            {
                usleep(100);
            }
        }
        conn_->DeleteChannel(replicate_channel);
    }

    // process crdt merge
    void Server::Merge(uint64 epoch)
    {
        std::vector<PB::Txn> all_txns;
        // write intent for local txns and remote txns
        uint64 epoch_mod = epoch % buffer_size;
        for (PB::Txn& txn : local_txns_[epoch_mod])
        {
            bool res = WriteIntent(txn, epoch_mod);
            if (!res)
                txn.set_status(PB::TxnStatus::ABORT);
            all_txns.push_back(txn);
        }
        for (PB::Txn& txn : remote_txns_[epoch_mod])
        {
            bool res = WriteIntent(txn, epoch_mod);
            if (!res)
                txn.set_status(PB::TxnStatus::ABORT);
            all_txns.push_back(txn);
        }

        // wait & write intent for WaitTxns if mul-replica
        if (config_->replica_num_ > 1)
        {
            // wait until last epoch's write completed
            {
                std::unique_lock<std::mutex> lk(cv_process_mutex_);
                cv_process_.wait(lk, [this, epoch]{return this->epoch_manager_->GetProcessedEpoch() == epoch-1; });
                lk.unlock();
            }
        }
        {
            std::shared_lock<std::shared_mutex> lk(mutex_wait_);
            while (!wait_txns_.empty())
            {
                PB::Txn txn = wait_txns_.front();
                wait_txns_.pop();
                bool res = WriteIntent(txn, epoch_mod);
                if (!res)
                    txn.set_status(PB::TxnStatus::ABORT);
                all_txns.push_back(txn);
            }
        }

        // validate write set
        // local txns
        for (PB::Txn& txn : all_txns)
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
        uint64 epoch_mod = epoch % buffer_size;
        std::vector<std::vector<TxnRes> > send_buf;
        send_buf.resize(config_->replica_size_);
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
        std::string channel = "Validate_" + std::to_string(epoch);
        conn_->NewChannel(channel);
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

        // LOG(INFO) << "epoch : " << epoch << " related nodes size = " << related_nodes_size;
        // receive related nodes's msg
        int received_nodes_size = 0;
        PB::MessageProto* msg = new PB::MessageProto();
        while (received_nodes_size < related_nodes_size - 1) // except itself
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
            else
            {
                usleep(100);
            }
        }
        
        conn_->DeleteChannel(channel);
    }

    void Server::EpochWrite(uint64 epoch)
    {
        // wait until last epoch's write completed
        {
            std::unique_lock<std::mutex> lk(cv_commit_mutex_);
            cv_commit_.wait(lk, [this, epoch]{return this->epoch_manager_->GetCommittedEpoch() == epoch-1; });
            lk.unlock();
        }
        uint64 epoch_mod = epoch % buffer_size;
        for (auto &kv : commit_txns_[epoch_mod])
        {
            PB::Txn& txn = kv.second;
            if (txn.status() == PB::TxnStatus::COMMIT && !txn.read_only())
            {
                storage_->LockWrite();
                for (size_t i = 0; i < txn.write_set_size(); i++)
                {
                    storage_->put(txn.write_set(i).key(), txn.write_set(i).value());
                }
                storage_->UnlockWrite();
                txn.set_end_ts(GetTime());
            }
        }
    }

    void Server::EnqueWaitTxns(uint64 epoch)
    {
        uint64 epoch_mod = epoch % buffer_size;
        for (auto &kv : commit_txns_[epoch_mod])
        {
            PB::Txn& txn = kv.second;
            if (txn.status() != PB::TxnStatus::ABORT)
            {
                if (txn.received_nodes_size() < txn.related_nodes_size())
                {
                    txn.set_status(PB::TxnStatus::WAIT);
                    std::unique_lock<std::shared_mutex> lk(mutex_wait_);
                    wait_txns_.push(txn);
                }
            }            
        }

        {
            std::shared_lock<std::shared_mutex> lk(mutex_wait_);
            LOG(INFO) << "epoch : " << epoch << " WaitTxns size = " << wait_txns_.size();
        }

        {
            std::unique_lock<std::mutex> lk(cv_process_mutex_);
            epoch_manager_->AddProcessedEpoch();
            cv_process_.notify_all();
        }
    }

    void Server::CleanBuffer(uint64 epoch)
    {
        uint64 epoch_mod = epoch % buffer_size;
        crdt_map_[epoch_mod].clear();
        local_txns_[epoch_mod].clear();
        commit_txns_[epoch_mod].clear();
        commit_txns_[epoch_mod].clear();
        if (config_->replica_num_ > 1)
            remote_txns_[epoch_mod].clear();
    }

    void Server::PrintStatistic(uint32 epoch)
    {
        uint64 epoch_mod = epoch % buffer_size;
        std::string filename = "./report." + UInt32ToString(server_id_) + "." + UInt64ToString(epoch);
        std::ofstream file(filename);
        std::string report;
        double abort_txn_cnt = 0.0;
        double commit_txn_cnt = 0.0;
        double latency = 0.0;
        for (const auto &kv : commit_txns_[epoch_mod])
        {
            const PB::Txn& txn = kv.second;
            if (txn.status() == PB::TxnStatus::COMMIT)
            {
                commit_txn_cnt += 1.0/double(txn.related_nodes_size());
                latency += double(txn.end_ts() - txn.start_ts());
                // LOG(INFO) << "single txn latency : " << txn.end_ts() - txn.start_ts() << " " << txn.end_epoch() << " " << txn.start_epoch();
            }
            else if(txn.status() == PB::TxnStatus::ABORT)
            {
                abort_txn_cnt += 1.0/double(txn.related_nodes_size());
            }
        }
        for (const auto &kv : abort_txns_[epoch_mod])
        {
            const PB::Txn& txn = kv.second;
            abort_txn_cnt += 1.0/double(txn.related_nodes_size());
        }
        
        LOG(ERROR) << "epoch: " << epoch << " commit_txn_cnt: " << commit_txn_cnt << " abort_txn_cnt: " << abort_txn_cnt;
        {
            std::lock_guard<std::mutex> lock(cnt_mutex_);
            total_commit_cnt_ += commit_txn_cnt;
            total_txn_cnt_ += commit_txn_cnt + abort_txn_cnt;
            total_latency_ += latency;
            // txns per second
            report.append("avg_throught   : " + DoubleToString(total_commit_cnt_ * 1000.0 / double(GetTime() - launch_ts_)) + "\n");
            report.append("avg_lantency   : " + DoubleToString(total_latency_ / total_commit_cnt_) + "\n");
            report.append("avg_abort_rate : " + DoubleToString(total_commit_cnt_ / total_txn_cnt_) + "\n");
        }
        file << report;
    }
    // worker
    void Server::Work(uint64 epoch)
    {
        if (config_->replica_num_ > 1)
        {
            // process replicate & collect all out-region subtxns
            LOG(INFO) << "epoch : " << epoch << " Start Replicate";
            // replicate subtxn and share 
            Replicate(epoch);
            LOG(INFO) << "epoch : " << epoch << " Replicate Finish";
        }
        // determinstic process merge
        // return value : kvs all will write in db 
        LOG(INFO) << "epoch : " << epoch << " Start Merge";
        Merge(epoch);
        LOG(INFO) << "epoch : " << epoch << " Merge Finish";
        // LOG(INFO) << "commitTxns " << commit_txns_[epoch % buffer_size].size();
        // LOG(INFO) << "abortTxns " << abort_txns_[epoch % buffer_size].size();
        // validate atomic
        if (config_->replica_size_ > 1)
        {
            LOG(INFO) << "epoch : " << epoch << " Start Validate";
            ValidateAtomic(epoch);
            LOG(INFO) << "epoch : " << epoch << " Validate Finish";
        }

        EnqueWaitTxns(epoch);

        EpochWrite(epoch);
        
        LOG(INFO) << "epoch : " << epoch << " Write Finish";
        PrintStatistic(epoch);
        CleanBuffer(epoch);
        // Check Correctness
        #ifdef CHECK_ATOMIC

        #endif
        {
            std::unique_lock<std::mutex> lk(cv_commit_mutex_);
            epoch_manager_->AddCommittedEpoch();
            cv_commit_.notify_all();
        }
    }

    void Server::Join()
    {
        worker_.join();
    }
} // namespace taas



