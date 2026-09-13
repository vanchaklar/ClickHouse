#pragma once

#include <Common/Exception.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool memory_reservation_protect_from_eviction;
    extern const SettingsBool memory_reservation_force_spill_before_eviction;
    extern const SettingsMilliseconds memory_reservation_suction_queue_timeout_ms;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 memory_reservation_max_allocation_before_suction_bytes;
    extern const ServerSettingsUInt64 memory_reservation_suction_max_allocation_bytes;
    extern const ServerSettingsUInt64 memory_reservation_suction_reserved_bytes;
    extern const ServerSettingsString memory_reservation_suction_queue_policy;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

inline MemoryReservation::Settings getMemoryReservationSettings(
    const Settings & settings,
    const ServerSettings & server_settings)
{
    MemoryReservation::Settings reservation_settings;
    reservation_settings.pressure_policy.protect_from_eviction
        = settings[Setting::memory_reservation_protect_from_eviction];
    reservation_settings.force_spill_before_eviction
        = settings[Setting::memory_reservation_force_spill_before_eviction];
    reservation_settings.suction_queue_timeout_ms
        = settings[Setting::memory_reservation_suction_queue_timeout_ms].totalMilliseconds();

    reservation_settings.pressure_policy.max_allocation_before_suction_bytes
        = server_settings[ServerSetting::memory_reservation_max_allocation_before_suction_bytes];
    reservation_settings.pressure_policy.suction_max_allocation_bytes
        = server_settings[ServerSetting::memory_reservation_suction_max_allocation_bytes];
    reservation_settings.pressure_policy.suction_reserved_bytes
        = server_settings[ServerSetting::memory_reservation_suction_reserved_bytes];

    const String suction_queue_policy = server_settings[ServerSetting::memory_reservation_suction_queue_policy];
    if (suction_queue_policy == "fifo")
    {
        reservation_settings.pressure_policy.suction_queue_policy
            = ResourceAllocation::SuctionQueuePolicy::Fifo;
    }
    else if (suction_queue_policy == "largest_memory_first")
    {
        reservation_settings.pressure_policy.suction_queue_policy
            = ResourceAllocation::SuctionQueuePolicy::LargestMemoryFirst;
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unknown `memory_reservation_suction_queue_policy`: '{}'. Expected `fifo` or `largest_memory_first`",
            suction_queue_policy);
    }

    return reservation_settings;
}

}
