import argparse
import os
import sys
from time import strftime

import configargparse

import mapadroid
from mapadroid.data_handler.mitm_data.MitmMapperType import MitmMapperType


def memoize(function):
    memo = {}

    def wrapper(*args):
        if args in memo:
            return memo[args]
        else:
            rv = function(*args)
            memo[args] = rv
            return rv

    return wrapper


@memoize
def parse_args():
    defaultconfigfiles = []
    default_tokenfile = None
    if '-cf' not in sys.argv and '--config' not in sys.argv:
        defaultconfigfiles = [os.getenv('MAD_CONFIG', os.path.join(
            mapadroid.MAD_ROOT, 'configs/config.ini'))]
    if '-td' not in sys.argv and '--token_dispenser' not in sys.argv:
        default_tokenfile = os.getenv('MAD_CONFIG', os.path.join(
            mapadroid.MAD_ROOT, 'configs/token-dispensers.ini'))
    parser = configargparse.ArgParser(
        default_config_files=defaultconfigfiles,
        auto_env_var_prefix='THERAIDMAPPER_')
    parser.add_argument('-cf', '--config',
                        is_config_file=True, help='Set configuration file')
    parser.add_argument('-asi', '--apk_storage_interface', required=True, help='APK Storage Interface')

    # MySQL
    parser.add_argument('-dbip', '--dbip', required=False,
                        help='IP or hostname of MySql Server')
    parser.add_argument('-dbsock', '--dbsocket', required=False,
                        help='Unix socket of MySQL Server - replaces dbip')
    parser.add_argument('-dbport', '--dbport', type=int, default=3306,
                        help='Port for MySQL Server')
    parser.add_argument('-dbuser', '--dbusername', required=False,
                        help='Username for MySQL login')
    parser.add_argument('-dbpassw', '--dbpassword', required=False,
                        help='Password for MySQL login')
    parser.add_argument('-dbname', '--dbname', required=False,
                        help='Name of MySQL Database')
    parser.add_argument('-dbps', '--db_poolsize', type=int, default=5,
                        help='Size of MySQL pool (open connections to DB). Default: 5')
    parser.add_argument('-nrati', '--no_restrict_accounts_to_instance', default=False, type=bool,
                        action=argparse.BooleanOptionalAction,
                        help='Configure whether the settings_pogoauth entries (PTC or google accounts) should be '
                             'fetched only for the active instance or globally. Default: False')

    # DB Cleanup
    parser.add_argument('-ci', '--cleanup_interval', type=int, default=300,
                        help='Interval between database cleanup routines in seconds. Default: 300.')
    parser.add_argument('-delmonshours', '--delete_mons_n_hours', type=int, default=None,
                        help='Remove mons from DB N hours after despawn. Only use positive values. '
                             'None if no cleanup is to be run. Default: None')
    parser.add_argument('-delmonslimit', '--delete_mons_limit', type=int, default=5000,
                        help='Limit the number of mon records to be deleted in each run. '
                             'Default: 5000. 0 represents infinity - may cause long locks on tables.')
    parser.add_argument('--delete_incidents_n_hours', type=int, default=None,
                        help='Remove incidents from DB N hours after expiration. Only use positive values. '
                             'None if no cleanup is to be run. Default: None')

    # Websocket Settings (RGC receiver)
    parser.add_argument('-wsip', '--ws_ip', required=False, default="0.0.0.0", type=str,
                        help='IP for websocket to listen on. Default: 0.0.0.0')
    parser.add_argument('-wsport', '--ws_port', required=False, type=int, default=8080,
                        help='Port of the websocket to listen on. Default: 8080')
    parser.add_argument('-wsct', '--websocket_command_timeout', required=False, type=int, default=30,
                        help='The max time to wait for a command to return (in seconds). Default: 30 seconds')

    # MITM Receiver (PD receiver)
    parser.add_argument('-mrip', '--mitmreceiver_ip', required=False, default="0.0.0.0", type=str,
                        help='IP to listen on for proto data (MITM data). Default: 0.0.0.0')
    parser.add_argument('-mrport', '--mitmreceiver_port', required=False, default=8000,
                        help='Port to listen on for proto data (MITM data). Default: 8000')
    parser.add_argument('-mrdw', '--mitmreceiver_data_workers', type=int, default=2,
                        help='Amount of workers to work off the data that queues up. Default: 2')
    parser.add_argument('-miptt', '--mitm_ignore_proc_time_thresh', type=int, default=0,
                        help='Ignore MITM data having a timestamp too far in the past.'
                             'Specify in seconds. Default: 0 (off)')
    parser.add_argument('-mipb', '--mitm_ignore_pre_boot', type=bool,
                        action=argparse.BooleanOptionalAction,
                        help='Ignore MITM data having a timestamp pre MAD\'s startup time')
    parser.add_argument('-mspass', '--mitm_status_password', default='',
                        help='Header Authorization password for MITM /status/ page')
    parser.add_argument('-mitmus', '--mitm_unix_socket', required=False, default=None, type=str,
                        help="Path to unix socket file to use if TCP is not to be used for MITMReceiver...")
    parser.add_argument('-xfwdpmitmr', '--enable_x_forwarded_path_mitm_receiver', type=bool,
                        action=argparse.BooleanOptionalAction,
                        help='Enable X-Fordward-Path allowance for reverse proxy usage for MITMReceiver. Default: False')
    parser.add_argument('-insecure', '--insecure_auth', type=bool,
                        action=argparse.BooleanOptionalAction,
                        help='Remove enforcing of auth for, e.g., MITMReceiver. Default: False')

    # MappingManager gRPC
    parser.add_argument('-mmgrip', '--mappingmanager_ip', required=False, default="127.0.0.1", type=str,
                        help='IP to listen on for the MappingManager gRPC API or connect to (main MAD component. Default: 127.0.0.1')
    parser.add_argument('-mmgrport', '--mappingmanager_port', required=False, default=50052, type=int,
                        help='Port to listen on for the MappingManager gRPC API or connect to (main MAD component). Default: 50052')
    parser.add_argument('-mmgrtlspriv', '--mappingmanager_tls_private_key_file', required=False, default=None, type=str,
                        help='Path to file of private key file (MappingManager). Default: None')
    parser.add_argument('-mmgrtlscert', '--mappingmanager_tls_cert_file', required=False, default=None, type=str,
                        help='Path to file of certificate chain (MappingManager). Default: None')
    parser.add_argument('-mmgrcomp', '--mappingmanager_compression', type=bool,
                        action=argparse.BooleanOptionalAction,
                        help='Enable compression of data of the MappingManager gRPC communication. Default: False')

    parser.add_argument('-mitmtype', '--mitmmapper_type', type=MitmMapperType, choices=list(MitmMapperType),
                        help='Pick the MitmMapper implementation to be used. gRPC or Redis are intended for bigger setups (multi process/tenant)')
    # MitmMapper gRPC
    parser.add_argument('-mitmmip', '--mitmmapper_ip', required=False, default="127.0.0.1", type=str,
                        help='IP to listen on for the MitmMapper gRPC API or connect to (separate MAD component. Default: 127.0.0.1')
    parser.add_argument('-mitmmport', '--mitmmapper_port', required=False, default=50051, type=int,
                        help='Port to listen on for the MitmMapper gRPC API or connect to (separate MAD component). Default: 50051')
    parser.add_argument('-mitmtlspriv', '--mitmmapper_tls_private_key_file', required=False, default=None, type=str,
                        help='Path to file of private key file. Default: None')
    parser.add_argument('-mitmtlscert', '--mitmmapper_tls_cert_file', required=False, default=None, type=str,
                        help='Path to file of certificate chain. Default: None')
    parser.add_argument('-mitmcomp', '--mitmmapper_compression', type=bool,
                        action=argparse.BooleanOptionalAction,
                        help='Enable compression of data of the MitmMapper gRPC communication. Default: False')

    # StatsHandler gRPC
    parser.add_argument('-statship', '--statshandler_ip', required=False, default="127.0.0.1", type=str,
                        help='IP to listen on for the StatsHandler gRPC API or connect to (separate MAD component. Default: 127.0.0.1')
    parser.add_argument('-statshport', '--statshandler_port', required=False, default=50053, type=int,
                        help='Port to listen on for the StatsHandler gRPC API or connect to (separate MAD component). Default: 50053')
    parser.add_argument('-statshtlspriv', '--statshandler_tls_private_key_file', required=False, default=None, type=str,
                        help='Path to file of private key file. Default: None')
    parser.add_argument('-statshtlscert', '--statshandler_tls_cert_file', required=False, default=None, type=str,
                        help='Path to file of certificate chain. Default: None')
    parser.add_argument('-statshcomp', '--statshandler_compression', type=bool,
                        action=argparse.BooleanOptionalAction,
                        help='Enable compression of data of the StatsHandler gRPC communication. Default: False')

    # Walk Settings
    parser.add_argument('--enable_worker_specific_extra_start_stop_handling', default=False,
                        help='Option to enable/disable extra handling for the start/stop routine of workers. Default: '
                             'False')
    parser.add_argument('-mvd', '--maximum_valid_distance', required=False, type=int, default=5,
                        help='The maximum distance for a scan of a location to be considered a valid/correct scan of'
                             ' that location in meters. Default: 5m')

    # Job Processor
    parser.add_argument('-jobdtwh', '--job_dt_wh', action='store_true', default=False,
                        help='Send job status to discord. Default: False')
    parser.add_argument('-jobdtwhurl', '--job_dt_wh_url', required=False, default="", type=str,
                        help='Discord Webhook URL for job messages')
    parser.add_argument('-jobdtsdtyp', '--job_dt_send_type', required=False,
                        default="SUCCESS|FAILURE|NOCONNECT|TERMINATED",
                        type=str, help='Kind of Job Messages to send - separated by pipe | '
                                       '(Default: SUCCESS|FAILURE|NOCONNECT|TERMINATED)')
    parser.add_argument('-jobrtnc', '--job_restart_notconnect', required=False, type=int, default=0,
                        help='Restart job if device is not connected (in minutes). Default: 0 (Off)')
    parser.add_argument('-jtc', '--job_thread_count', type=int, default=1,
                        help='Amount of threads to work off the device jobs. Default: 1')

    # Runtypes
    parser.add_argument('-os', '--only_scan', action='store_true', default=True,
                        help='Use this instance only for scanning')
    parser.add_argument('-otc', '--ocr_thread_count', type=int, default=2,
                        help='Amount of threads/processes to be used for screenshot-analysis. Default: 2')
    parser.add_argument('-otl', '--omp_thread_limit', type=int, default=None,
                        help='Set the environment variable OMP_THREAD_LIMIT. This does not default, i.e., '
                             'it is not set at all. Some environments apparently require limitation to 1')
    parser.add_argument('-or', '--only_routes', action='store_true', default=False,
                        help='Only calculate routes, then exit the program. No scanning.')
    parser.add_argument('-cm', '--config_mode', action='store_true', default=False,
                        help='Run in ConfigMode')
    parser.add_argument('-nm', '--scan_nearby_mons', action='store_true', default=False,
                        help='Enable scanning of nearby mons')
    parser.add_argument('-dnc', '--disable_nearby_cell', action='store_true', default=False,
                        help='Disables nearby_cell scans if scan_nearby_mons is enabled')
    parser.add_argument('-lm', '--scan_lured_mons', action='store_true', default=False,
                        help='Enable scanning of lured mons')
    parser.add_argument('-dnt', '--default_nearby_timeleft', type=int, default=15,
                        help='The default despawn time left in minutes for Nearby Mons. Default: 15')
    parser.add_argument('-dut', '--default_unknown_timeleft', type=int, default=3,
                        help='The default despawn time left in minutes for Mons at unknown Spawnpoints. Default: 3')
    parser.add_argument("-sn", "--status-name", default="mad",
                        help=("Enable status page database update using"
                              " STATUS_NAME as main worker name."))
    parser.add_argument('-nec', '--no_event_checker', action="store_true",
                        help='Disable event checker task')
    parser.add_argument('-ut', '--unit_tests', action='store_true', default=False,
                        help='Run unit tests then quit', dest='unit_tests')

    # Path Settings
    parser.add_argument('-tmp', '--temp_path', default='temp',
                        help='Temp Folder for OCR Scanning. Default: temp')
    parser.add_argument('-upload', '--upload_path', default=os.path.join(mapadroid.MAD_ROOT, 'upload'),
                        help='Path for uploaded Files via madmin and for device installation. Default: '
                             '/absolute/path/to/upload')
    parser.add_argument('--file_path',
                        help='Defines directory to save worker stats- and position files and calculated routes',
                        default='files')
    # Should be deprecated but may be required for *REALLY* old systems
    parser.add_argument('-mf', '--mappings',
                        default=os.getenv('MAD_CONFIG', os.path.join(mapadroid.MAD_ROOT, 'configs/mappings.json')),
                        help='Set mappings file')

    # other settings
    parser.add_argument('-hlat', '--home_lat', default='0.0', type=float,
                        help=('Set Lat from the center of your scan location.'
                              'Especially for using MADBOT (User submitted Raidscreens). Default: 0.0'))
    parser.add_argument('-hlng', '--home_lng', default='0.0', type=float,
                        help=('Set Lng from the center of your scan location.'
                              'Especially for using MADBOT (User submitted Raidscreens). Default: 0.0'))
    parser.add_argument('-L', '--language', default='en',
                        help=('Set Language for Madmin / Quests. Default: en'))
    parser.add_argument('--no_quest_titles', default=False, action='store_true',
                        help='Do not download quest title resources')
    parser.add_argument('-wfdsd', '--wait_for_data_sleep_duration', default='1.0', type=float,
                        help=('Time in seconds (floating point) to sleep inbetween checks of data in workers. '
                              'Default: 1.0'))

    # MADmin
    parser.add_argument('-dm', '--disable_madmin', action='store_true', default=False,
                        help='Disable Madmin on the instance')
    parser.add_argument('-mmbp', '--madmin_base_path', default='/',
                        help='Base path for madmin')
    parser.add_argument('-mmip', '--madmin_ip', default='0.0.0.0',
                        help='MADmin listening interface (Default: 0.0.0.0)')
    parser.add_argument('-mmprt', '--madmin_port', default='5000',
                        help='MADmin web port (Default: 5000)')
    parser.add_argument('-mus', '--madmin_unix_socket', required=False, default=None, type=str,
                        help="Path to unix socket file to use if TCP is not to be used for MADmin...")
    parser.add_argument('-mmauth', '--madmin_enable_auth', type=bool,
                        action=argparse.BooleanOptionalAction,
                        help='Enable demanding of credentials based on settings_auth.')
    parser.add_argument('-mmt', '--madmin_time', default='24',
                        help='MADmin clock format (12/24) (Default: 24)')
    parser.add_argument('-mmnrsp', '--madmin_noresponsive', action='store_false', default=True,
                        help='MADmin deactivate responsive tables')
    parser.add_argument('-qpub', '--quests_public', action='store_true', default=False,
                        help='Enables MADmin /quests_pub, /get_quests, and pushassets endpoints for public quests'
                             'overview')
    parser.add_argument('-ods', '--outdated_spawnpoints', type=int, default=3,
                        help='Define when a spawnpoint is out of date (in days). Default: 3.')
    parser.add_argument('--quest_stats_fences', default="",
                        help="Comma separated list of geofences for stop/quest statistics (Empty: all)")
    parser.add_argument('-xfwdpmadmin', '--enable_x_forwarded_path_madmin', type=bool,
                        action=argparse.BooleanOptionalAction,
                        help='Enable X-Fordward-Path allowance for reverse proxy usage for MADmin. Default: False')

    # Statistics
    parser.add_argument('-stat', '--statistic', action='store_true', default=False,
                        help='Activate system statistics (Default: False)')
    parser.add_argument('-stco', '--stat_gc', action='store_true', default=False,
                        help='Store collected objects (garbage collector) (Default: False)')
    parser.add_argument('-stiv', '--statistic_interval', default=60, type=int,
                        help='Store new local stats every N seconds (Default: 60)')
    parser.add_argument('-trace', '--trace', action='store_true', default=False,
                        help='Enable tracing of system stats, e.g. tracemalloc. '
                             'ONLY TO BE USED FOR DEBUGGING ISSUES. (Default: False)')

    # Game Stats
    parser.add_argument('-ggs', '--game_stats', action='store_true', default=False,
                        help='Generate worker stats')
    parser.add_argument('-ggrs', '--game_stats_raw', action='store_true', default=False,
                        help='Generate worker raw stats (only with --game_stats)')
    parser.add_argument('-gsms', '--game_stats_mon_seen', action='store_true', default=False,
                        help='Generate mon seen stats (only with --game_stats)')
    parser.add_argument('-gsst', '--game_stats_save_time', default=300, type=int,
                        help='Number of seconds until worker information is saved to database')
    parser.add_argument('-rds', '--raw_delete_shiny', default=0, type=int,
                        help='Delete shiny mon in raw stats older then x days (0 =  Disable (Default))')

    # ADB
    parser.add_argument('-adb', '--use_adb', action='store_true', default=False,
                        help='Use ADB for "device control" (Default: False)')
    parser.add_argument('-adbservip', '--adb_server_ip', default='127.0.0.1',
                        help='IP address of ADB server (Default: 127.0.0.1)')
    parser.add_argument('-adpservprt', '--adb_server_port', type=int, default=5037,
                        help='Port of ADB server (Default: 5037)')

    # Webhook
    parser.add_argument('-wh', '--webhook', action='store_true', default=False,
                        help='Activate webhook support')
    parser.add_argument('-whurl', '--webhook_url', default='',
                        help='URL endpoint/s for webhooks (seperated by commas) with [<type>] '
                             'for restriction like [mon|weather|raid]http://example.org/foo/bar '
                             '- urls have to start with http*')
    parser.add_argument('-whser', '--webhook_submit_exraids', action='store_true', default=False,
                        help='Send Ex-raids to the webhook if detected')
    parser.add_argument('-whea', '--webhook_excluded_areas', default="",
                        help='Comma-separated list of area names to exclude elements from within to be sent to a '
                             'webhook')
    parser.add_argument('-qwhf', '--quest_webhook_flavor', choices=['default', 'poracle'], default='default',
                        help='Webhook format for Quests: default or poracle compatible')
    parser.add_argument('-whst', '--webhook_start_time', default=0,
                        help='Debug: Set initial timestamp to fetch changed elements from the DB to send via WH.')
    parser.add_argument('-whmps', '--webhook_max_payload_size', default=0, type=int,
                        help='Split up the payload into chunks and send multiple requests. Default: 0 (unlimited)')
    parser.add_argument('-whwi', '--webhook_worker_interval', default=10, type=int,
                        help='Send webhook every X seconds (Default: 10 [seconds])')

    # Dynamic Rarity
    parser.add_argument('-rh', '--rarity_hours', type=int, default=72,
                        help='Set the number of hours for the calculation of pokemon rarity (Default: 72)')
    parser.add_argument('-ruf', '--rarity_update_frequency', type=int, default=60,
                        help='Update frequency for dynamic rarity in minutes (Default: 60)')

    # Logging
    parser.add_argument('--no_file_logs', action='store_true', default=False,
                        help="Disable file logging (Default: file logging is enabled by default)")
    parser.add_argument('--log_path', default="logs/",
                        help="Defines directory to save log files to.")
    parser.add_argument('--log_filename', default='%Y%m%d_%H%M_<SN>.log',
                        help=("Defines the log filename to be saved."
                              " Allows date formatting, and replaces <SN>"
                              " with the instance's status name. Read the"
                              " python time module docs for details."
                              " Default: %%Y%%m%%d_%%H%%M_<SN>.log."))
    parser.add_argument("--log_file_rotation", default="50 MB",
                        help=("This parameter expects a human-readable value like"
                              " '18:00', 'sunday', 'weekly', 'monday at 12:00' or"
                              " a maximum file size like '100 MB' or '0.5 GB'."
                              " Set to '0' to disable completely. (Default: 50 MB)"))
    parser.add_argument('--log_level',
                        help=("Forces a certain log level. By default"
                              " it's set to INFO while being modified"
                              " by the -v command to show DEBUG logs."
                              " Custom log levels like DEBUG[1-5] can"
                              " be used too."))
    verbose = parser.add_mutually_exclusive_group()
    verbose.add_argument('-v', action='count', default=0, dest='verbose',
                         help=("Show debug messages. Has no effect, if"
                               "--log_level has been set."))
    parser.add_argument("--log_file_level",
                        help="File logging level. See description for --log_level.")
    parser.add_argument("--log_file_retention", default="10",
                        help=("Amount of days to keep file logs. Set to 0 to"
                              " keep them forever (Default: 10)"))
    parser.add_argument('--no_log_colors', action="store_true", default=False,
                        help=("Disable colored logs."))
    parser.set_defaults(DEBUG=False)

    # MADAPKs
    parser.add_argument('-td', '--token_dispenser', default=default_tokenfile,
                        help='Token dispenser config (MAD)')
    parser.add_argument('-tdu', '--token_dispenser_user', default='',
                        help='Token dispenser config (User)')
    parser.add_argument('-gu', '--gmail_user', default='',
                        help='Google Mail User for interacting with the Google Play Store')
    parser.add_argument('-gp', '--gmail_passwd', default='',
                        help='Google Mail Password for interacting with the Google Play Store.  Must be an app'
                             ' password or 2fa will be triggered (this should be enabled on your account anyways')
    parser.add_argument('-mat', '--maddev_api_token', default=None,
                        help='MADdev API token used for querying supported versions')

    # Auto-Configuration
    parser.add_argument('-acna', '--autoconfig_no_auth', action='store_true', default=False,
                        help='MAD PoGo auth is not required during autoconfiguration',
                        dest='autoconfig_no_auth')

    # PTC Login tracking
    parser.add_argument('-ips', '--ip_service', default='https://checkip.amazonaws.com',
                        help=('Host to use to request the external IPv4 address of the device. '
                              'MAD will search for the first IPv4 address via regex.'))
    parser.add_argument('-elt', '--enable_login_tracking', action='store_true', default=False,
                        help='Enable tracking of login attempts to PTC')
    parser.add_argument('-lth', '--login_tracking_host', type=str, default=None,
                        help='Redis cache host for login tracking. If not specified, it will use the cache_host set '
                             'for general redis caching. (Default: None).')
    parser.add_argument('-ltp', '--login_tracking_port', default=6379,
                        help='Redis cache port. If not specified along with login_tracking_host, it will use the cache_'
                             'port and cache_host set for general redis caching. (Default: 6379).')
    parser.add_argument('-ltu', '--login_tracking_username', default='',
                        help='Redis username for login tracking')
    parser.add_argument('-ltpwd', '--login_tracking_password', default='',
                        help='Redis password for login tracking')
    parser.add_argument('-ltdb', '--login_tracking_database', default=0,
                        help=('Redis database used by login tracking. Use different numbers (0-15) '
                              'if you are running multiple instances'))
    parser.add_argument('-ltt', '--login_tracking_timeout', default=360,
                        help=('Duration in seconds during which a login attempt is counting towards '
                              '\'login_tracking_limit\'.'))
    parser.add_argument('-ltl', '--login_tracking_limit', default=15,
                        help='Max number of login attempts during \'login_tracking_timeout\'.')

    # Redis cache
    parser.add_argument('-ch', '--cache_host', default='127.0.0.1',
                        help='Redis host used by caching')
    parser.add_argument('-csock', '--cache_socket', required=False,
                        help='Unix Socket to connect to redis - replaces cache_host')
    parser.add_argument('-cp', '--cache_port', default=6379,
                        help='Redis port used by caching')
    parser.add_argument('-cu', '--cache_username', default=None,
                        help='Redis username')
    parser.add_argument('-cpwd', '--cache_password', default=None,
                        help='Redis password')
    parser.add_argument('-cdb', '--cache_database', default=0,
                        help='Redis database. Use different numbers (0-15) if you are running multiple instances')

    parser.add_argument('-eemd', '--enable_early_maintenance_detection', action='store_true', default=False,
                        help='Enable early maintenance screen detection - could be inaccurate, but will save on login time')
    parser.add_argument('-rrqk', '--redis_report_queue_key', default=None,
                        help='Redis key used to store reported value')
    parser.add_argument('-rrqi', '--redis_report_queue_interval', default=30, type=int,
                        help='Report queue size from mitmreciver to redis every every N seconds (Default: 30)')


    if "MODE" in os.environ and os.environ["MODE"] == "DEV":
        args = parser.parse_known_args()[0]
    else:
        args = parser.parse_args()
    # Allow status name and date formatting in log filename.
    args.log_filename = strftime(args.log_filename)
    args.log_filename = args.log_filename.replace('<sn>', '<SN>')
    args.log_filename = args.log_filename.replace('<SN>', args.status_name)

    return args
