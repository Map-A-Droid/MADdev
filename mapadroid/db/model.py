# coding: utf-8
import enum

from sqlalchemy import Column, Double, Float, ForeignKey, Index, String, text
from sqlalchemy.dialects.mysql import (BIGINT, BOOLEAN, ENUM, INTEGER,
                                       LONGBLOB, LONGTEXT, MEDIUMBLOB,
                                       SMALLINT, TINYINT)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from mapadroid.db.GeometryColumnType import GeometryColumnType
from mapadroid.db.TZDateTime import TZDateTime

Base = declarative_base()
metadata = Base.metadata


class AuthLevel(enum.Enum):
    MADMIN_PUBLIC_PAGE = 1
    MITM_DATA = 2
    MADMIN_ADMIN = 4


class FilestoreMeta(Base):
    __tablename__ = 'filestore_meta'

    filestore_id = Column(INTEGER(11), primary_key=True)
    filename = Column(String(255, 'utf8mb4_unicode_ci'), nullable=False)
    size = Column(INTEGER(11), nullable=False)
    mimetype = Column(String(255, 'utf8mb4_unicode_ci'), nullable=False)


class MadApk(FilestoreMeta):
    __tablename__ = 'mad_apks'
    __table_args__ = (
        Index('usage', 'usage', 'arch', unique=True),
    )

    filestore_id = Column(ForeignKey('filestore_meta.filestore_id', ondelete='CASCADE'), primary_key=True)
    usage = Column(INTEGER(11), nullable=False)
    arch = Column(INTEGER(11), nullable=False)
    version = Column(String(32, 'utf8mb4_unicode_ci'), nullable=False)


class Gym(Base):
    __tablename__ = 'gym'
    __table_args__ = (
        Index('gym_latitude_longitude', 'latitude', 'longitude'),
    )

    gym_id = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True)
    team_id = Column(SMALLINT(6), nullable=False)
    guard_pokemon_id = Column(SMALLINT(6), nullable=False)
    slots_available = Column(SMALLINT(6), nullable=False)
    enabled = Column(BOOLEAN, nullable=False)
    latitude = Column(Double(asdecimal=True), nullable=False)
    longitude = Column(Double(asdecimal=True), nullable=False)
    total_cp = Column(SMALLINT(6), nullable=False)
    is_in_battle = Column(BOOLEAN, nullable=False)
    weather_boosted_condition = Column(SMALLINT(6))
    last_modified = Column(TZDateTime, nullable=False, index=True)
    last_scanned = Column(TZDateTime, nullable=False, index=True)
    is_ex_raid_eligible = Column(BOOLEAN)
    is_ar_scan_eligible = Column(BOOLEAN, nullable=False, server_default=text("'0'"))


class GymDetail(Base):
    __tablename__ = 'gymdetails'

    gym_id = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True)
    name = Column(String(191, 'utf8mb4_unicode_ci'), nullable=False)
    description = Column(LONGTEXT)
    url = Column(String(191, 'utf8mb4_unicode_ci'), nullable=False)
    last_scanned = Column(TZDateTime, nullable=False)


class MadApkAutosearch(Base):
    __tablename__ = 'mad_apk_autosearch'

    usage = Column(INTEGER(11), primary_key=True, nullable=False)
    arch = Column(INTEGER(11), primary_key=True, nullable=False)
    version = Column(String(32, 'utf8mb4_unicode_ci'))
    url = Column(String(256, 'utf8mb4_unicode_ci'))
    download_status = Column(TINYINT(1), nullable=False, server_default=text("'0'"))
    last_checked = Column(TZDateTime, nullable=False)


class MadminInstance(Base):
    __tablename__ = 'madmin_instance'

    instance_id = Column(INTEGER(10), primary_key=True)
    name = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False, unique=True)


class OriginHopper(Base):
    __tablename__ = 'origin_hopper'

    origin = Column(String(128, 'utf8mb4_unicode_ci'), primary_key=True)
    last_id = Column(INTEGER(10), nullable=False)


class Pokemon(Base):
    __tablename__ = 'pokemon'
    __table_args__ = (
        Index('pokemon_disappear_time_pokemon_id', 'disappear_time', 'pokemon_id'),
        Index('pokemon_iv', 'individual_attack', 'individual_defense', 'individual_stamina'),
        Index('pokemon_latitude_longitude', 'latitude', 'longitude')
    )

    encounter_id = Column(BIGINT(20), primary_key=True)
    spawnpoint_id = Column(BIGINT(20), nullable=False, index=True)
    pokemon_id = Column(SMALLINT(6), nullable=False, index=True)
    latitude = Column(Double(asdecimal=True), nullable=False)
    longitude = Column(Double(asdecimal=True), nullable=False)
    disappear_time = Column(TZDateTime, nullable=False)
    individual_attack = Column(SMALLINT(6), index=True)
    individual_defense = Column(SMALLINT(6))
    individual_stamina = Column(SMALLINT(6))
    move_1 = Column(SMALLINT(6))
    move_2 = Column(SMALLINT(6))
    cp = Column(SMALLINT(6))
    cp_multiplier = Column(Float)
    weight = Column(Float)
    height = Column(Float)
    gender = Column(SMALLINT(6))
    form = Column(SMALLINT(6))
    costume = Column(SMALLINT(6))
    catch_prob_1 = Column(Double(asdecimal=True))
    catch_prob_2 = Column(Double(asdecimal=True))
    catch_prob_3 = Column(Double(asdecimal=True))
    rating_attack = Column(String(2, 'utf8mb4_unicode_ci'))
    rating_defense = Column(String(2, 'utf8mb4_unicode_ci'))
    weather_boosted_condition = Column(SMALLINT(6))
    last_modified = Column(TZDateTime, index=True)
    fort_id = Column(String(50, 'utf8mb4_unicode_ci'), default=None)
    cell_id = Column(BIGINT(20), default=None)
    seen_type = Column(ENUM('wild', 'encounter', 'nearby_stop', 'nearby_cell', 'lure_wild',
                            'lure_encounter'), nullable=False)
    size = Column(SMALLINT(6), default=None)


class PokemonDisplay(Base):
    __tablename__ = 'pokemon_display'

    encounter_id = Column(BIGINT(20), primary_key=True)
    pokemon = Column(SMALLINT(6), nullable=False, index=True)
    gender = Column(SMALLINT(6), server_default=None, nullable=True)
    form = Column(SMALLINT(6), server_default=None, nullable=True)
    costume = Column(SMALLINT(6), server_default=None, nullable=True)


class Pokestop(Base):
    __tablename__ = 'pokestop'
    __table_args__ = (
        Index('pokestop_latitude_longitude', 'latitude', 'longitude'),
    )

    pokestop_id = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True)
    enabled = Column(BOOLEAN, nullable=False)
    latitude = Column(Double(asdecimal=True), nullable=False)
    longitude = Column(Double(asdecimal=True), nullable=False)
    last_modified = Column(TZDateTime, nullable=False, index=True)
    lure_expiration = Column(TZDateTime, index=True)
    active_fort_modifier = Column(SMALLINT(6), index=True)
    last_updated = Column(TZDateTime, index=True)
    name = Column(String(128, 'utf8mb4_unicode_ci'))
    image = Column(String(255, 'utf8mb4_unicode_ci'))
    is_ar_scan_eligible = Column(BOOLEAN, nullable=False, server_default=text("'0'"))


class PokestopIncident(Base):
    __tablename__ = 'pokestop_incident'
    __table_args__ = (
        Index('pokestop_incident_stop_expiration', 'pokestop_id', 'incident_expiration'),
        Index('pokestop_incident_expiration', 'incident_expiration'),
    )
    pokestop_id = Column(ForeignKey('pokestop.pokestop_id', ondelete='CASCADE'), primary_key=True,
                         nullable=False)
    incident_id = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True, unique=True)
    incident_start = Column(TZDateTime, nullable=True)
    incident_expiration = Column(TZDateTime, nullable=True, index=True)
    hide_incident = Column(BOOLEAN, nullable=False, server_default=text("'0'"))
    incident_display_type = Column(SMALLINT(3))
    incident_display_order_priority = Column(INTEGER(11))
    custom_display = Column(String(50))
    is_cross_stop_incident = Column(BOOLEAN, nullable=False, server_default=text("'0'"))
    character_display = Column(SMALLINT(4), server_default=None, nullable=True)


class Raid(Base):
    __tablename__ = 'raid'

    gym_id = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True)
    level = Column(INTEGER(11), nullable=False, index=True)
    spawn = Column(TZDateTime, nullable=False, index=True)
    start = Column(TZDateTime, nullable=False, index=True)
    end = Column(TZDateTime, nullable=False, index=True)
    pokemon_id = Column(SMALLINT(6))
    cp = Column(INTEGER(11))
    move_1 = Column(SMALLINT(6))
    move_2 = Column(SMALLINT(6))
    last_scanned = Column(TZDateTime, nullable=False, index=True)
    form = Column(SMALLINT(6))
    is_exclusive = Column(BOOLEAN)
    gender = Column(TINYINT(1))
    costume = Column(TINYINT(1))
    evolution = Column(SMALLINT(6))


class Scannedlocation(Base):
    __tablename__ = 'scannedlocation'
    __table_args__ = (
        Index('scannedlocation_latitude_longitude', 'latitude', 'longitude'),
    )

    cellid = Column(BIGINT(20), primary_key=True)
    latitude = Column(Double(asdecimal=True), nullable=False)
    longitude = Column(Double(asdecimal=True), nullable=False)
    last_modified = Column(TZDateTime, index=True)
    done = Column(BOOLEAN, nullable=False)
    band1 = Column(SMALLINT(6), nullable=False)
    band2 = Column(SMALLINT(6), nullable=False)
    band3 = Column(SMALLINT(6), nullable=False)
    band4 = Column(SMALLINT(6), nullable=False)
    band5 = Column(SMALLINT(6), nullable=False)
    midpoint = Column(SMALLINT(6), nullable=False)
    width = Column(SMALLINT(6), nullable=False)


class SettingsGeofence(Base):
    __tablename__ = 'settings_geofence'
    __table_args__ = (
        Index('name', 'name', 'instance_id', unique=True),
    )

    geofence_id = Column(INTEGER(10), primary_key=True)
    guid = Column(String(32, 'utf8mb4_unicode_ci'))
    instance_id = Column(INTEGER(10), nullable=False)
    name = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False)
    fence_type = Column(ENUM('polygon', 'geojson'), nullable=False, server_default=text("'polygon'"))
    fence_data = Column(LONGTEXT, nullable=False)

    def __str__(self):
        return self.name


class SettingsRoutecalc(Base):
    __tablename__ = 'settings_routecalc'

    routecalc_id = Column(INTEGER(10), primary_key=True)
    guid = Column(String(32, 'utf8mb4_unicode_ci'))
    instance_id = Column(INTEGER(10), nullable=False)
    recalc_status = Column(BOOLEAN, server_default=text("'0'"))
    last_updated = Column(TZDateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    routefile = Column(LONGTEXT)


class Spawnpoint(Base):
    __tablename__ = 'spawnpoint'
    __table_args__ = (
        Index('spawnpoint_latitude_longitude', 'latitude', 'longitude'),
    )

    id = Column(BIGINT(20), primary_key=True)
    latitude = Column(Double(asdecimal=True), nullable=False)
    longitude = Column(Double(asdecimal=True), nullable=False)
    last_scanned = Column(TZDateTime, nullable=False, index=True)
    kind = Column(String(4, 'utf8mb4_unicode_ci'), nullable=False)
    links = Column(String(4, 'utf8mb4_unicode_ci'), nullable=False)
    missed_count = Column(INTEGER(11), nullable=False)
    latest_seen = Column(SMALLINT(6), nullable=False)
    earliest_unseen = Column(SMALLINT(6), nullable=False)


class TrsEvent(Base):
    __tablename__ = 'trs_event'

    id = Column(INTEGER(11), primary_key=True)
    event_name = Column(String(100))
    event_start = Column(TZDateTime)
    event_end = Column(TZDateTime)
    event_lure_duration = Column(INTEGER(11), nullable=False, server_default=text("'30'"))


class TrsQuest(Base):
    __tablename__ = 'trs_quest'

    GUID = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True)
    # 0: AR layer, 1: layer when holding AR quest
    layer = Column(TINYINT(3), primary_key=True, default=1, nullable=False, autoincrement=False)
    quest_type = Column(TINYINT(3), nullable=False, index=True)
    quest_timestamp = Column(INTEGER(11), nullable=False)
    quest_stardust = Column(SMALLINT(4), nullable=False)
    quest_pokemon_id = Column(SMALLINT(4), nullable=False)
    quest_reward_type = Column(SMALLINT(3), nullable=False)
    quest_item_id = Column(SMALLINT(3), nullable=False)
    quest_item_amount = Column(TINYINT(2), nullable=False)
    quest_target = Column(TINYINT(3), nullable=False)
    quest_condition = Column(String(2500, 'utf8mb4_unicode_ci'))
    quest_reward = Column(String(2560, 'utf8mb4_unicode_ci'))
    quest_template = Column(String(100, 'utf8mb4_unicode_ci'))
    quest_task = Column(String(150, 'utf8mb4_unicode_ci'))
    quest_pokemon_form_id = Column(SMALLINT(6), nullable=False, server_default=text("'0'"))
    quest_pokemon_costume_id = Column(SMALLINT(6), nullable=False, server_default=text("'0'"))
    quest_title = Column(String(100, 'utf8mb4_unicode_ci'), nullable=True, server_default=None)


class TrsS2Cell(Base):
    __tablename__ = 'trs_s2cells'

    id = Column(BIGINT(20), primary_key=True)
    level = Column(INTEGER(11), nullable=False)
    center_latitude = Column(Double(asdecimal=True), nullable=False)
    center_longitude = Column(Double(asdecimal=True), nullable=False)
    updated = Column(INTEGER(11), nullable=False)


class TrsSpawn(Base):
    __tablename__ = 'trs_spawn'
    __table_args__ = (
        Index('event_lat_long', 'eventid', 'latitude', 'longitude'),
    )

    spawnpoint = Column(BIGINT(20), primary_key=True)
    latitude = Column(Double(asdecimal=True), nullable=False)
    longitude = Column(Double(asdecimal=True), nullable=False)
    spawndef = Column(INTEGER(11), nullable=False, server_default=text("'240'"))
    earliest_unseen = Column(INTEGER(6), nullable=False)
    last_scanned = Column(TZDateTime)
    first_detection = Column(TZDateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    last_non_scanned = Column(TZDateTime)
    calc_endminsec = Column(String(5, 'utf8mb4_unicode_ci'))
    eventid = Column(INTEGER(11), nullable=False, server_default=text("'1'"))


class TrsStatsDetect(Base):
    __tablename__ = 'trs_stats_detect'

    id = Column(INTEGER(100), primary_key=True)
    worker = Column(String(100, 'utf8mb4_unicode_ci'), nullable=False, index=True)
    timestamp_scan = Column(INTEGER(11), nullable=False)
    mon = Column(INTEGER(255))
    raid = Column(INTEGER(255))
    mon_iv = Column(INTEGER(11))
    quest = Column(INTEGER(100))


class TrsStatsDetectWildMonRaw(Base):
    __tablename__ = 'trs_stats_detect_wild_mon_raw'

    worker = Column(String(128, 'utf8mb4_unicode_ci'), primary_key=True)
    encounter_id = Column(BIGINT(20), primary_key=True)
    count = Column(INTEGER(), nullable=False)
    is_shiny = Column(BOOLEAN, server_default='0', nullable=False)
    first_scanned = Column(TZDateTime, nullable=False)
    last_scanned = Column(TZDateTime, nullable=False)


class TrsStatsDetectSeenType(Base):
    __tablename__ = 'trs_stats_detect_seen_type'

    encounter_id = Column(BIGINT(20), primary_key=True)
    encounter = Column(TZDateTime, default=None, nullable=True)
    wild = Column(TZDateTime, default=None, nullable=True)
    nearby_stop = Column(TZDateTime, default=None, nullable=True)
    nearby_cell = Column(TZDateTime, default=None, nullable=True)
    lure_encounter = Column(TZDateTime, default=None, nullable=True)
    lure_wild = Column(TZDateTime, default=None, nullable=True)


class TrsStatsLocation(Base):
    __tablename__ = 'trs_stats_location'

    id = Column(INTEGER(11), primary_key=True)
    worker = Column(String(100, 'utf8mb4_unicode_ci'), nullable=False, index=True)
    timestamp_scan = Column(INTEGER(11), nullable=False)
    location_ok = Column(INTEGER(11), nullable=False)
    location_nok = Column(INTEGER(11), nullable=False)


class TrsStatsLocationRaw(Base):
    __tablename__ = 'trs_stats_location_raw'
    __table_args__ = (
        Index('latlng', 'lat', 'lng'),
        Index('count_same_events', 'worker', 'lat', 'lng', 'type', 'period', unique=True)
    )

    id = Column(INTEGER(11), primary_key=True)
    worker = Column(String(100, 'utf8mb4_unicode_ci'), nullable=False)
    lat = Column(Double(asdecimal=True), nullable=False)
    lng = Column(Double(asdecimal=True), nullable=False)
    fix_ts = Column(INTEGER(11), nullable=False)
    data_ts = Column(INTEGER(11), nullable=False)
    type = Column(TINYINT(1), nullable=False)
    walker = Column(String(255, 'utf8mb4_unicode_ci'), nullable=False)
    success = Column(BOOLEAN, nullable=False)
    period = Column(INTEGER(11), nullable=False)
    transporttype = Column(TINYINT(1), nullable=False)


class TrsUsage(Base):
    __tablename__ = 'trs_usage'

    usage_id = Column(INTEGER(10), primary_key=True)
    instance = Column(String(100, 'utf8mb4_unicode_ci'))
    cpu = Column(Float)
    memory = Column(Float)
    garbage = Column(INTEGER(5))
    timestamp = Column(INTEGER(11))


class TrsVisited(Base):
    __tablename__ = 'trs_visited'

    pokestop_id = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True, nullable=False)
    username = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True, nullable=False)


class Version(Base):
    __tablename__ = 'versions'

    key = Column(String(191, 'utf8mb4_unicode_ci'), primary_key=True)
    val = Column(SMALLINT(6), nullable=False)


class Weather(Base):
    __tablename__ = 'weather'

    s2_cell_id = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True)
    latitude = Column(Double(asdecimal=True), nullable=False)
    longitude = Column(Double(asdecimal=True), nullable=False)
    cloud_level = Column(SMALLINT(6))
    rain_level = Column(SMALLINT(6))
    wind_level = Column(SMALLINT(6))
    snow_level = Column(SMALLINT(6))
    fog_level = Column(SMALLINT(6))
    wind_direction = Column(SMALLINT(6))
    gameplay_weather = Column(SMALLINT(6))
    severity = Column(SMALLINT(6))
    warn_weather = Column(SMALLINT(6))
    world_time = Column(SMALLINT(6))
    last_updated = Column(TZDateTime, index=True)


class AutoconfigFile(Base):
    __tablename__ = 'autoconfig_file'

    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), primary_key=True,
                         nullable=False)
    name = Column(String(128, 'utf8mb4_unicode_ci'), primary_key=True, nullable=False)
    data = Column(LONGBLOB, nullable=False)

    instance = relationship('MadminInstance')


class FilestoreChunk(Base):
    __tablename__ = 'filestore_chunks'
    __table_args__ = (
        Index('chunk_id', 'chunk_id', 'filestore_id', unique=True),
    )

    chunk_id = Column(INTEGER(11), primary_key=True)
    filestore_id = Column(ForeignKey('filestore_meta.filestore_id', ondelete='CASCADE'), nullable=False, index=True)
    size = Column(INTEGER(11), nullable=False)
    data = Column(LONGBLOB)

    filestore = relationship('FilestoreMeta')


class SettingsArea(Base):
    __tablename__ = 'settings_area'

    area_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
    guid = Column(String(32, 'utf8mb4_unicode_ci'))
    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    name = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False)
    mode = Column(ENUM('idle', 'iv_mitm', 'mon_mitm', 'pokestops', 'raids_mitm', 'init'), nullable=False)

    instance = relationship('MadminInstance')


class SettingsAreaIdle(SettingsArea):
    __tablename__ = 'settings_area_idle'

    area_id = Column(ForeignKey('settings_area.area_id', ondelete='CASCADE'), primary_key=True, autoincrement=True)
    geofence_included = Column(ForeignKey('settings_geofence.geofence_id'), nullable=False, index=True)
    routecalc = Column(ForeignKey('settings_routecalc.routecalc_id'), nullable=False, index=True)

    settings_geofence = relationship('SettingsGeofence')
    settings_routecalc = relationship('SettingsRoutecalc')


class SettingsAreaIvMitm(SettingsArea):
    __tablename__ = 'settings_area_iv_mitm'

    area_id = Column(ForeignKey('settings_area.area_id', ondelete='CASCADE'), primary_key=True, autoincrement=True)
    geofence_included = Column(ForeignKey('settings_geofence.geofence_id'), nullable=False, index=True)
    geofence_excluded = Column(String(256, 'utf8mb4_unicode_ci'))
    routecalc = Column(ForeignKey('settings_routecalc.routecalc_id'), nullable=False, index=True)
    speed = Column(Float)
    max_distance = Column(Float)
    delay_after_prio_event = Column(INTEGER(11))
    priority_queue_clustering_timedelta = Column(Float)
    remove_from_queue_backlog = Column(BOOLEAN)
    starve_route = Column(BOOLEAN)
    monlist_id = Column(ForeignKey('settings_monivlist.monlist_id'), index=True)
    all_mons = Column(BOOLEAN, nullable=False, server_default=text("'0'"))
    min_time_left_seconds = Column(INTEGER(11))
    encounter_all = Column(BOOLEAN)

    settings_geofence = relationship('SettingsGeofence')
    monlist = relationship('SettingsMonivlist')
    settings_routecalc = relationship('SettingsRoutecalc')


class SettingsAreaMonMitm(SettingsArea):
    __tablename__ = 'settings_area_mon_mitm'

    area_id = Column(ForeignKey('settings_area.area_id', ondelete='CASCADE'), primary_key=True, autoincrement=True)
    geofence_included = Column(ForeignKey('settings_geofence.geofence_id'), nullable=False, index=True)
    geofence_excluded = Column(String(256, 'utf8mb4_unicode_ci'))
    routecalc = Column(ForeignKey('settings_routecalc.routecalc_id'), nullable=False, index=True)
    coords_spawns_known = Column(BOOLEAN)
    speed = Column(Float)
    max_distance = Column(Float)
    delay_after_prio_event = Column(INTEGER(11))
    priority_queue_clustering_timedelta = Column(Float)
    remove_from_queue_backlog = Column(Float)
    starve_route = Column(BOOLEAN)
    monlist_id = Column(ForeignKey('settings_monivlist.monlist_id'), index=True)
    all_mons = Column(BOOLEAN, nullable=False, server_default=text("'0'"))
    min_time_left_seconds = Column(INTEGER(11))
    max_clustering = Column(INTEGER(11))
    include_event_id = Column(INTEGER(11))
    encounter_all = Column(BOOLEAN)

    settings_geofence = relationship('SettingsGeofence')
    monlist = relationship('SettingsMonivlist')
    settings_routecalc = relationship('SettingsRoutecalc')


class SettingsAreaPokestop(SettingsArea):
    __tablename__ = 'settings_area_pokestops'

    area_id = Column(ForeignKey('settings_area.area_id', ondelete='CASCADE'), primary_key=True, autoincrement=True)
    geofence_included = Column(ForeignKey('settings_geofence.geofence_id'), nullable=False, index=True)
    geofence_excluded = Column(String(256, 'utf8mb4_unicode_ci'))
    routecalc = Column(ForeignKey('settings_routecalc.routecalc_id'), nullable=False, index=True)
    level = Column(BOOLEAN)
    speed = Column(Float)
    max_distance = Column(Float)
    ignore_spinned_stops = Column(BOOLEAN)
    cleanup_every_spin = Column(BOOLEAN)
    layer = Column(TINYINT(3), default=1, nullable=False, autoincrement=False, index=True)

    settings_geofence = relationship('SettingsGeofence')
    settings_routecalc = relationship('SettingsRoutecalc')


class SettingsAreaRaidsMitm(SettingsArea):
    __tablename__ = 'settings_area_raids_mitm'

    area_id = Column(ForeignKey('settings_area.area_id', ondelete='CASCADE'), primary_key=True, autoincrement=True)
    geofence_included = Column(ForeignKey('settings_geofence.geofence_id'), nullable=False, index=True)
    geofence_excluded = Column(String(256, 'utf8mb4_unicode_ci'))
    routecalc = Column(ForeignKey('settings_routecalc.routecalc_id'), nullable=False, index=True)
    including_stops = Column(BOOLEAN)
    speed = Column(Float)
    max_distance = Column(Float)
    delay_after_prio_event = Column(INTEGER(11))
    priority_queue_clustering_timedelta = Column(Float)
    remove_from_queue_backlog = Column(Float)
    starve_route = Column(BOOLEAN)
    monlist_id = Column(ForeignKey('settings_monivlist.monlist_id'), index=True)
    all_mons = Column(BOOLEAN, nullable=False, server_default=text("'0'"))
    encounter_all = Column(BOOLEAN)

    settings_geofence = relationship('SettingsGeofence')
    monlist = relationship('SettingsMonivlist')
    settings_routecalc = relationship('SettingsRoutecalc')


class SettingsAreaInitMitm(SettingsArea):
    __tablename__ = 'settings_area_init_mitm'

    area_id = Column(ForeignKey('settings_area.area_id', ondelete='CASCADE'), primary_key=True, autoincrement=True)
    geofence_included = Column(ForeignKey('settings_geofence.geofence_id'), nullable=False, index=True)
    geofence_excluded = Column(String(256, 'utf8mb4_unicode_ci'))
    routecalc = Column(ForeignKey('settings_routecalc.routecalc_id'), nullable=False, index=True)
    init_type = Column(ENUM('forts', 'mons'), nullable=False)
    init_mode_rounds = Column(INTEGER(11))
    monlist_id = Column(ForeignKey('settings_monivlist.monlist_id'), index=True)
    all_mons = Column(BOOLEAN, nullable=False, server_default=text("'0'"))
    speed = Column(Float)
    max_distance = Column(Float)

    settings_geofence = relationship('SettingsGeofence')
    monlist = relationship('SettingsMonivlist')
    settings_routecalc = relationship('SettingsRoutecalc')


class SettingsAuth(Base):
    __tablename__ = 'settings_auth'

    auth_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
    guid = Column(String(32, 'utf8mb4_unicode_ci'))
    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    username = Column(String(32, 'utf8mb4_unicode_ci'), nullable=False)
    password = Column(String(32, 'utf8mb4_unicode_ci'), nullable=False)
    auth_level = Column(INTEGER(10), nullable=False, server_default=text("'0'"))

    instance = relationship('MadminInstance')


class SettingsDevicepool(Base):
    __tablename__ = 'settings_devicepool'

    pool_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
    guid = Column(String(32, 'utf8mb4_unicode_ci'))
    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    name = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False)
    post_walk_delay = Column(Float)
    post_teleport_delay = Column(Float)
    walk_after_teleport_distance = Column(Float)
    cool_down_sleep = Column(BOOLEAN)
    post_turn_screen_on_delay = Column(Float)
    post_pogo_start_delay = Column(Float)
    restart_pogo = Column(INTEGER(11))
    inventory_clear_rounds = Column(INTEGER(11))
    mitm_wait_timeout = Column(Float)
    vps_delay = Column(Float)
    reboot = Column(BOOLEAN)
    reboot_thresh = Column(INTEGER(11))
    restart_thresh = Column(INTEGER(11))
    post_screenshot_delay = Column(Float)
    screenshot_x_offset = Column(INTEGER(11))
    screenshot_y_offset = Column(INTEGER(11))
    screenshot_type = Column(ENUM('jpeg', 'png'))
    screenshot_quality = Column(INTEGER(11))
    startcoords_of_walker = Column(String(256, 'utf8mb4_unicode_ci'))
    injection_thresh_reboot = Column(INTEGER(11))
    screendetection = Column(BOOLEAN)
    enhanced_mode_quest_safe_items = Column(String(500, 'utf8mb4_unicode_ci'))
    extended_login = Column(BOOLEAN, server_default=text("'0'"))

    instance = relationship('MadminInstance')


class SettingsMonivlist(Base):
    __tablename__ = 'settings_monivlist'

    monlist_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
    guid = Column(String(32, 'utf8mb4_unicode_ci'))
    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    name = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False)

    instance = relationship('MadminInstance')

    def __str__(self):
        return self.name


class SettingsWalker(Base):
    __tablename__ = 'settings_walker'

    walker_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
    guid = Column(String(32, 'utf8mb4_unicode_ci'))
    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    name = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False)

    instance = relationship('MadminInstance')

    def __str__(self):
        return self.name


class SettingsDevice(Base):
    __tablename__ = 'settings_device'

    device_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
    guid = Column(String(32, 'utf8mb4_unicode_ci'))
    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    name = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False)
    walker_id = Column(ForeignKey('settings_walker.walker_id'), nullable=False, index=True)
    pool_id = Column(ForeignKey('settings_devicepool.pool_id'), index=True)
    adbname = Column(String(128, 'utf8mb4_unicode_ci'))
    post_walk_delay = Column(Float)
    post_teleport_delay = Column(Float)
    walk_after_teleport_distance = Column(Float)
    cool_down_sleep = Column(BOOLEAN)
    post_turn_screen_on_delay = Column(Float)
    post_pogo_start_delay = Column(Float)
    restart_pogo = Column(INTEGER(11))
    inventory_clear_rounds = Column(INTEGER(11))
    mitm_wait_timeout = Column(Float)
    vps_delay = Column(Float)
    reboot = Column(BOOLEAN)
    reboot_thresh = Column(INTEGER(11))
    restart_thresh = Column(INTEGER(11))
    post_screenshot_delay = Column(Float)
    screenshot_x_offset = Column(INTEGER(11))
    screenshot_y_offset = Column(INTEGER(11))
    screenshot_type = Column(ENUM('jpeg', 'png'))
    screenshot_quality = Column(INTEGER(11))
    startcoords_of_walker = Column(String(256, 'utf8mb4_unicode_ci'))
    screendetection = Column(BOOLEAN)
    # Google login mail set in device to be used with prio (optional).
    # Accountswitching will attempt google login first. If it fails, fallback to any free PTC
    ggl_login_mail = Column(String(256, 'utf8mb4_unicode_ci'))
    clear_game_data = Column(BOOLEAN)
    account_rotation = Column(BOOLEAN)
    rotation_waittime = Column(Float)
    rotate_on_lvl_30 = Column(BOOLEAN)
    injection_thresh_reboot = Column(INTEGER(11))
    enhanced_mode_quest_safe_items = Column(String(500, 'utf8mb4_unicode_ci'))
    mac_address = Column(String(17, 'utf8mb4_unicode_ci'))
    interface_type = Column(ENUM('lan', 'wlan'), server_default=text("'lan'"))
    softbar_enabled = Column(BOOLEAN, server_default=text("'0'"))
    extended_permission_toggling = Column(BOOLEAN, server_default=text("'0'"))
    extended_login = Column(BOOLEAN, server_default=text("'0'"))

    instance = relationship('MadminInstance')
    pool = relationship('SettingsDevicepool')
    walker = relationship('SettingsWalker')


class TrsStatus(Base):
    # class TrsStatus(Base):
    __tablename__ = 'trs_status'

    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    device_id = Column(ForeignKey('settings_device.device_id', ondelete='CASCADE'), primary_key=True)
    currentPos = Column(GeometryColumnType)
    lastPos = Column(GeometryColumnType)
    routePos = Column(INTEGER(11))
    routeMax = Column(INTEGER(11))
    area_id = Column(ForeignKey('settings_area.area_id', ondelete='CASCADE'), index=True)
    idle = Column(TINYINT(4), server_default=text("'0'"))
    rebootCounter = Column(INTEGER(11))
    lastProtoDateTime = Column(TZDateTime)
    lastPogoRestart = Column(TZDateTime)
    rebootingOption = Column(BOOLEAN)
    restartCounter = Column(INTEGER(11))
    lastPogoReboot = Column(TZDateTime)
    globalrebootcount = Column(INTEGER(11), server_default=text("'0'"))
    globalrestartcount = Column(INTEGER(11), server_default=text("'0'"))
    currentSleepTime = Column(INTEGER(11), nullable=False, server_default=text("'0'"))
    area = relationship('SettingsArea')
    instance = relationship('MadminInstance')


class SettingsMonivlistToMon(Base):
    __tablename__ = 'settings_monivlist_to_mon'

    monlist_id = Column(ForeignKey('settings_monivlist.monlist_id', ondelete='CASCADE'), primary_key=True,
                        nullable=False, index=True)
    mon_id = Column(INTEGER(11), primary_key=True, nullable=False, index=True)
    mon_order = Column(INTEGER(11), nullable=False)

    monlist = relationship('SettingsMonivlist')


class SettingsWalkerarea(Base):
    __tablename__ = 'settings_walkerarea'

    walkerarea_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
    guid = Column(String(32, 'utf8mb4_unicode_ci'))
    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    name = Column(String(128, 'utf8mb4_unicode_ci'))
    area_id = Column(ForeignKey('settings_area.area_id'), nullable=False, index=True)
    algo_type = Column(ENUM('countdown', 'timer', 'round', 'period', 'coords', 'idle'), nullable=False)
    algo_value = Column(String(256, 'utf8mb4_unicode_ci'))
    max_walkers = Column(INTEGER(11))
    eventid = Column(INTEGER(11))

    area = relationship('SettingsArea')
    instance = relationship('MadminInstance')


class AutoconfigRegistration(Base):
    __tablename__ = 'autoconfig_registration'

    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    session_id = Column(INTEGER(10), primary_key=True)
    device_id = Column(ForeignKey('settings_device.device_id', ondelete='CASCADE'), index=True)
    ip = Column(String(39, 'utf8mb4_unicode_ci'), nullable=False)
    status = Column(INTEGER(10))

    device = relationship('SettingsDevice')
    instance = relationship('MadminInstance')


class SettingsPogoauth(Base):
    __tablename__ = 'settings_pogoauth'
    __table_args__ = (
        Index('settings_pogoauth_u1', 'login_type', 'username', unique=True),
    )

    instance_id = Column(ForeignKey('madmin_instance.instance_id', ondelete='CASCADE'), nullable=False, index=True)
    account_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
    device_id = Column(ForeignKey('settings_device.device_id', ondelete='CASCADE'), index=True, nullable=True)
    login_type = Column(ENUM('google', 'ptc'), nullable=False)
    username = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False)
    password = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False)
    key_blob = Column(MEDIUMBLOB, nullable=True)
    level = Column(SMALLINT(2), nullable=False, server_default=str(0))
    last_burn = Column(TZDateTime, nullable=True)
    last_burn_type = Column(ENUM('ban', 'suspended', 'maintenance'), nullable=True)
    last_softban_action = Column(TZDateTime, nullable=True)
    last_softban_action_location = Column(GeometryColumnType, nullable=True)

    device = relationship('SettingsDevice')
    instance = relationship('MadminInstance')


class SettingsWalkerToWalkerarea(Base):
    __tablename__ = 'settings_walker_to_walkerarea'

    walker_id = Column(ForeignKey('settings_walker.walker_id', ondelete='CASCADE'), primary_key=True, nullable=False,
                       index=True)
    walkerarea_id = Column(ForeignKey('settings_walkerarea.walkerarea_id'), primary_key=True, nullable=False,
                           index=True)
    area_order = Column(INTEGER(11), primary_key=True, nullable=False)

    walker = relationship('SettingsWalker')
    walkerarea = relationship('SettingsWalkerarea')


class AutoconfigLog(Base):
    __tablename__ = 'autoconfig_logs'
    __table_args__ = (
        Index('k_acl', 'instance_id', 'session_id'),
    )

    log_id = Column(INTEGER(10), primary_key=True)
    instance_id = Column(INTEGER(10), nullable=False)
    session_id = Column(ForeignKey('autoconfig_registration.session_id', ondelete='CASCADE'), nullable=False,
                        index=True)
    log_time = Column(TZDateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    level = Column(INTEGER(10), nullable=False, server_default=text("'2'"))
    msg = Column(String(1024, 'utf8mb4_unicode_ci'), nullable=False)

    session = relationship('AutoconfigRegistration')


class Route(Base):
    __tablename__ = 'route'

    route_id = Column(String(50), primary_key=True)
    waypoints = Column(LONGTEXT, nullable=False)
    type = Column(TINYINT(2), nullable=False, server_default=text("'0'"))
    path_type = Column(TINYINT(2), nullable=False, server_default=text("'0'"))
    name = Column(String(255, 'utf8mb4_unicode_ci'), nullable=False)
    description = Column(String(255, 'utf8mb4_unicode_ci'), nullable=False)
    version = Column(INTEGER(11), nullable=False)
    reversible = Column(BOOLEAN, nullable=False)
    submission_time = Column(TZDateTime, nullable=False)
    route_distance_meters = Column(INTEGER(11), nullable=False)
    route_duration_seconds = Column(INTEGER(11), nullable=False)
    pins = Column(LONGTEXT, nullable=True)
    tags = Column(LONGTEXT, nullable=True)
    image = Column(String(255, 'utf8mb4_unicode_ci'), nullable=True)
    image_border_color_hex = Column(String(8, 'utf8mb4_unicode_ci'), nullable=True)
    route_submission_status = Column(TINYINT(2), nullable=False, server_default=text("'0'"))
    route_submission_update_time = Column(TZDateTime, nullable=False)
    start_poi_fort_id = Column(String(50, 'utf8mb4_unicode_ci'), nullable=False)
    start_poi_latitude = Column(Double(asdecimal=True), nullable=False)
    start_poi_longitude = Column(Double(asdecimal=True), nullable=False)
    start_poi_image_url = Column(String(255, 'utf8mb4_unicode_ci'), nullable=True)
    end_poi_fort_id = Column(String(50, 'utf8mb4_unicode_ci'), nullable=False)
    end_poi_latitude = Column(Double(asdecimal=True), nullable=False)
    end_poi_longitude = Column(Double(asdecimal=True), nullable=False)
    end_poi_image_url = Column(String(255, 'utf8mb4_unicode_ci'), nullable=True)
    last_updated = Column(TZDateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))

class Station(Base):
    __tablename__ = 'wr_station'

    station_id = Column(String(50, 'utf8mb4_unicode_ci'), primary_key=True)
    latitude = Column(Double(asdecimal=True), nullable=False)
    longitude = Column(Double(asdecimal=True), nullable=False)
    name = Column(String(128, 'utf8mb4_unicode_ci'), nullable=False)
    battle_spawn = Column(TZDateTime)
    battle_window_start = Column(TZDateTime)
    battle_window_end = Column(TZDateTime, index=True)
    battle_pokemon_id = Column(SMALLINT(6), index=True)
    battle_pokemon_form = Column(SMALLINT(6))
    battle_pokemon_costume = Column(SMALLINT(6))
    battle_pokemon_gender = Column(TINYINT(1))
    battle_pokemon_alignment = Column(SMALLINT(6))
    battle_pokemon_move_1 = Column(SMALLINT(6))
    battle_pokemon_move_2 = Column(SMALLINT(6))
    battle_level = Column(TINYINT(1))
    reward_pokemon_id = Column(SMALLINT(6))
    reward_pokemon_form = Column(SMALLINT(6))
    reward_pokemon_costume = Column(SMALLINT(6))
    reward_pokemon_gender = Column(TINYINT(1))
    reward_pokemon_alignment = Column(SMALLINT(6))
    start_time = Column(TZDateTime, nullable=False)
    end_time = Column(TZDateTime, index=True, nullable=False)
    bread_battle_available = Column(BOOLEAN, nullable=False)
    inactive = Column(BOOLEAN, nullable=False)
    last_updated = Column(TZDateTime, index=True, nullable=False)
