#include <chrono>
#include <thread>

#include "epoch.h"

extern uint32 epoch_length;
namespace taas
{

    EpochManager* EpochManager::em_ = nullptr;

    EpochManager::EpochManager(uint32 epoch_duration = epoch_length) : epoch_duration_(epoch_duration)
    {
        std::atomic_init(&cur_epoch_, 1);
        std::atomic_init(&committed_epoch_, 0);
        std::atomic_init(&processed_epoch_, 0);
        deconstructor_invoked_ = false;
    }

    EpochManager::~EpochManager()
    {
        deconstructor_invoked_ = true;
        delete em_;
    }

    EpochManager& EpochManager::GetInstance()
    {
        static EpochManager em;
        return em;
    }
    void EpochManager::Run()
    {

    }
    void EpochManager::AddPhysicalEpoch()
    {
        cur_epoch_++;
    }
    uint64 EpochManager::GetPhysicalEpoch() 
    {
        return cur_epoch_.load();
    }

    void EpochManager::AddCommittedEpoch()
    {
        committed_epoch_++;
    }
    uint64 EpochManager::GetCommittedEpoch() 
    {
        return committed_epoch_.load();
    }

    double EpochManager::GetEpochDuration()
    {
        return epoch_duration_;
    }
    void EpochManager::AddProcessedEpoch()
    {
        processed_epoch_++;
    }
    uint64 EpochManager::GetProcessedEpoch()
    {
        return processed_epoch_;
    }
} // namespace taas




