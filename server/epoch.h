#ifndef EPOCH_H
#define EPOCH_H

#include <atomic>
#include "types.h"

namespace taas
{
    class EpochManager
    {
    private:
        static EpochManager* em_;

        uint32 epoch_duration_; // ms
        std::atomic<uint64> cur_epoch_;
        std::atomic<uint64> committed_epoch_;
        std::atomic<uint64> processed_epoch_;
        bool deconstructor_invoked_;

        EpochManager(uint32 epoch_duration);
        ~EpochManager();
        EpochManager(const EpochManager&) = delete;
        EpochManager& operator=(const EpochManager&);
        void Run();
    public:
        static EpochManager& GetInstance();
        void AddPhysicalEpoch();
        uint64 GetPhysicalEpoch();
        double GetEpochDuration();
        void AddCommittedEpoch();
        uint64 GetCommittedEpoch();
        void AddProcessedEpoch();
        uint64 GetProcessedEpoch();
    };
} // namespace taas



#endif