TIMESTAMP_NEVER = 0
WALK_AFTER_TELEPORT_SPEED = 11
FALLBACK_MITM_WAIT_TIMEOUT = 30
# Distance in meters that are to be allowed to consider a GMO as within a valid range
# Some modes calculate with extremely strict distances (0.0001m for example), thus not allowing
# direct use of routemanager radius as a distance (which would allow long distances for raid scans as well)
MINIMUM_DISTANCE_ALLOWANCE_FOR_GMO = 5

# Since GMOs may arrive during walks, we define sort of a buffer to use.
# That buffer can be subtracted in case a walk was longer than that buffer.
SECONDS_BEFORE_ARRIVAL_OF_WALK_BUFFER = 10

STOP_SPIN_DISTANCE = 80

# Parameters of routemanagers


# Redis caching time.
REDIS_CACHETIME_MON_LURE_IV = 180
REDIS_CACHETIME_IVS = 180
REDIS_CACHETIME_STOP_DETAILS = 900
REDIS_CACHETIME_GYMS = 900
REDIS_CACHETIME_RAIDS = 900
REDIS_CACHETIME_CELLS = 60
REDIS_CACHETIME_WEATHER = 900
REDIS_CACHETIME_POKESTOP_DATA = 900
REDIS_CACHETIME_ROUTE = 900
REDIS_CACHETIME_STATIONS = 900
