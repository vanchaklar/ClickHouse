#pragma once

#include <Common/Scheduler/MemoryReservation.h>

namespace DB
{

struct ServerSettings;
struct Settings;

MemoryReservation::Settings getMemoryReservationSettings(
    const Settings & settings,
    const ServerSettings & server_settings);

}
