#pragma once

#include <Common/Scheduler/MemoryReservation.h>

namespace DB
{

class ServerSettings;
class Settings;

MemoryReservation::Settings getMemoryReservationSettings(
    const Settings & settings,
    const ServerSettings & server_settings);

}
