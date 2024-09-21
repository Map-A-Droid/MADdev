from enum import Enum


class ScreenType(Enum):
    UNDEFINED = -1
    BIRTHDATE = 1  # birthday selection screen, set by PogoWindows.__screendetection_get_type_internal
    RETURNING = 2  # returning player screen
    LOGINSELECT = 3  # login selection regarding OAUTH
    PTC = 4  # PTC login
    FAILURE = 5  # Unable to authenticate. Please try again. One green button: OK
    RETRY = 6  # Failed to log in. Green button on top: RETRY - no background button below: TRY A DIFFERENT ACCOUNT
    WRONG = 7  # incorrect credentials?
    GAMEDATA = 8  # game data could not be fetched from server
    HARDWARE_UNITY_UNSUPPORTED = 9 # Your device does not match hardware requirements for this application CONTINUE
    GGL = 10  # Google account picker
    PERMISSION = 11  # permission grant overlay (Android)
    MARKETING = 12  # marketing notification request (pogo)
    CONSENT = 13  # consent activity
    SN = 14  # OS not compatible message (pogo)
    UPDATE = 15  # Force update modal (pogo)
    STRIKE = 16  # Strike / red warning modal (pogo)
    SUSPENDED = 17  # account suspended modal / temporarily banned (pogo)
    TERMINATED = 18  # account terminated modal (pogo)
    MAINTENANCE = 19  # blue maintenance screen (pogo)
    QUEST = 20  # research menu / quest listing (pogo)
    GPS = 21  # GPS signal not found error message (pogo)
    CREDENTIALS = 22  # new GrantCredentials dialog
    NOTRESPONDING = 24  # system popup about an app not responding, blocking most actions
    NOGGL = 23  # loginselect, but without the google button (probably failed to set a correct birthdate)
    LOGINTIMEOUT = 25  # login taking too long, sign out button is visible
    WELCOME = 30 # Welcome screen for new account. One green button: CONTINUE
    TOS = 31 # Terms of service. One green button: ACCEPT
    PRIVACY = 32 # Privacy Policy notification. One green button: OK
    WILLOWCHAR = 33 # Professor Blabla screen in tutorial before character is selected.
    WILLOWCATCH = 34 # Professor Blabla screen in catch tutorial.
    WILLOWNAME = 35 # Professor Blabla asking for name.
    ADVENTURESYNC = 36 # Adventure Sync overlay after tutorial. One blue button: Turn it On!
    WILLOWGO = 37 # Professor Blabla at end of tutorial.
    LIMITATIONS = 38 # Most likely new MAINTENANCE/24h cooldown
    POGO = 99  # uhm, whatever... At least pogo is topmost, no idea where we are yet tho (in the process of switching)
    ERROR = 100  # some issue occurred while handling screentypes or not able to determine screen
    BLACK = 110  # screen is black, likely loading up game
    CLOSE = 500  # pogo is not topmost app (whatever app is topmost, it's not pogo)
    DISABLED = 999  # screendetection disabled
