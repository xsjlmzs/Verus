#ifndef SERVER_H
#define SERVER_H

#include <vector>
#include <string>
#include <unordered_set>

#include "common.h"
#include "epoch.h"
#include "storage.h"
#include "thread_pool.h"

namespace taas
{
    const uint16 kMaxEpoch = 1000;
    enum Isolation
    {
        kReadCommit = 0,
        kRepeatableRead,
        kSnapshotIsolation,
        kSerilizable, // not supported
    };
    class Server
    {
    private:
        // list of servers
        Configuration* config_;
        Connection* conn_;
        EpochManager* epoch_manager_;
        Storage* storage_;
        ThreadPool* thread_pool_;

        bool deconstructor_invoked_;
        // for local merge <key, tid>
        struct Metadata
        {
            uint64 sen;
            uint64 rna;
            uint64 csn;
        };
        // for validate atomic
        struct TxnRes
        {
            uint64 txn_id;
            bool commit = false;
        };
        
        std::vector<std::map<std::string, Metadata> > crdt_map_;
        // local txn <epoch-id, txns>
        std::mutex mutexes_local_txns_[64];
        std::mutex add_epoch_mutex_;
        std::vector<std::vector<PB::Txn> > local_txns_;
        std::vector<std::vector<PB::Txn> > remote_txns_;
        // WaitTxns
        std::shared_mutex mutex_wait_;
        std::vector<PB::Txn> wait_txns_;
        // CommitTxns & AbortTxns
        // struct Hash
        // {
        //     uint64 operator()(const PB::Txn& txn) const
        //     {
        //         return txn.txn_id();
        //     }
        // };
        // struct Equal
        // {
        //     bool operator()(const PB::Txn& txn1,const PB::Txn& txn2) const
        //     {
        //         return txn1.txn_id() == txn2.txn_id();
        //     }
        // };
        
        
        std::vector<std::unordered_map<uint64, PB::Txn> > commit_txns_;
        std::vector<std::unordered_map<uint64, PB::Txn> > abort_txns_;

        uint32 server_id_;
        uint32 parition_id_;
        uint32 replica_id_;
        Isolation isolation;

        void HeartbeatAllServers();
        void Execute(PB::Txn* txn, uint64 epoch);
        bool WriteIntent(const PB::Txn& txn, uint64 epoch);
        bool ValidateWS(const PB::Txn& txn, uint64 epoch);
        void ValidateAtomic(uint64 epoch);
        void EpochWrite(uint64 epoch);
        void CleanBuffer(uint64 epoch);
        void EnqueWaitTxns(uint64 epoch);
        void PrintStatistic(uint32 epoch);

        std::thread worker_;

        std::mutex cnt_mutex_;
        double total_txn_cnt_ = 0;
        double total_latency_ = 0;
        uint64 launch_ts_ = 0;
        
        uint64 limit_epoch_;
        uint32 limit_txns_;

        std::mutex cv_commit_mutex_;
        std::condition_variable cv_commit_;

        std::mutex cv_process_mutex_;
        std::condition_variable cv_process_;

    public:
        Server(Configuration *config, Connection *conn, uint32 node_id);
        ~Server();
        void Run();
        // std::vector<PB::MessageProto>* Distribute(const std::vector<PB::Txn>& local_txns, uint64 epoch);
        void Replicate(uint64 epoch);
        bool ValidateReadSet(const PB::Txn& txn);
        void Merge(uint64 epoch);

        // worker
        void Work(uint64 epoch);
        void Join();
    };
} // namespace tass



#endif