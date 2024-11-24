import time
from datetime import timedelta
from typing import List, Optional

from loguru import logger

from mapadroid.utils.DatetimeWrapper import DatetimeWrapper
from mapadroid.utils.madGlobals import QuestLayer
import mapadroid.mitm_receiver.protos.Rpc_pb2 as pogoprotos


def calculate_mon_level(cp_multiplier):
    if cp_multiplier < 0.734:
        pokemon_level = 58.35178527 * cp_multiplier * cp_multiplier - 2.838007664 * cp_multiplier + 0.8539209906
    else:
        pokemon_level = 171.0112688 * cp_multiplier - 95.20425243
    return round(pokemon_level) * 2 / 2


def determine_current_quest_layer(quests_held: Optional[List[int]]) -> QuestLayer:
    if quests_held is None:
        raise ValueError("Unknown quest layer of worker")
    if 46 in quests_held:
        # 46 is the quest ID for AR quests, thus we are scanning the NON AR layer since we will only receive
        # quests without AR quests
        return QuestLayer.NON_AR
    else:
        return QuestLayer.AR


def gen_despawn_timestamp(known_despawn, timestamp, default_time_left_minutes=3) -> int:
    if not known_despawn:
        # just set despawn time to now + 3 minutes
        # after that round down to full minutes to fix
        # possible re-scan issue updating the seconds
        # causing the timestmap to change and thus causing
        # a resend via webhook. This kinde fixes that. Kinda.
        return int(int(time.time() + default_time_left_minutes * 60) // 60 * 60)

    hrmi = known_despawn.split(":")
    datatime = DatetimeWrapper.fromtimestamp(timestamp)
    despawn = datatime.replace(minute=int(hrmi[0]), second=int(hrmi[1]), microsecond=0)
    if datatime.minute > despawn.minute:
        despawn = despawn + timedelta(hours=1)

    despawn_ts = int(despawn.timestamp())
    return despawn_ts


def calculate_iv(ind_atk, ind_def, ind_stm):
    iv = 100.0 * (ind_atk + ind_def + ind_stm) / 45
    return iv


def form_mapper(mon_id, form_id):
    forms = {
        "19": {
            "45": 0,  # normal
            "46": 61  # alola
        },
        "20": {
            "47": 0,  # normal
            "48": 61  # alola
        },
        "26": {
            "49": 0,  # normal
            "50": 61  # alola
        },
        "27": {
            "51": 0,  # normal
            "52": 61  # alola
        },
        "28": {
            "53": 0,  # normal
            "54": 61  # alola
        },
        "37": {
            "55": 0,  # normal
            "56": 61  # alola
        },
        "38": {
            "57": 0,  # normal
            "58": 61  # alola
        },
        "50": {
            "59": 0,  # normal
            "60": 61  # alola
        },
        "51": {
            "61": 0,  # normal
            "62": 61  # alola
        },
        "52": {
            "63": 0,  # normal
            "64": 61  # alola
        },
        "53": {
            "65": 0,  # normal
            "66": 61  # alola
        },
        "74": {
            "67": 0,  # normal
            "68": 61  # alola
        },
        "75": {
            "69": 0,  # normal
            "70": 61  # alola
        },
        "76": {
            "71": 0,  # normal
            "72": 61  # alola
        },
        "88": {
            "73": 0,  # normal
            "74": 61  # alola
        },
        "89": {
            "75": 0,  # normal
            "76": 61  # alola
        },
        "103": {
            "77": 0,  # normal
            "78": 61  # alola
        },
        "105": {
            "79": 0,  # normal
            "80": 61  # alola
        },
        "150": {  # TODO: missing assets
            "133": 0,
            "134": 0,
            "135": 0
        },
        "201": {
            "1": 11,  # a
            "2": 12,  # b
            "3": 13,  # c
            "4": 14,  # d
            "5": 15,  # e
            "6": 16,  # f
            "7": 17,  # g
            "8": 18,  # h
            "9": 19,  # i
            "10": 20,  # j
            "11": 21,  # k
            "12": 22,  # l
            "13": 23,  # m
            "14": 24,  # n
            "15": 25,  # o
            "16": 26,  # p
            "17": 27,  # q
            "18": 28,  # r
            "19": 29,  # s
            "20": 30,  # t
            "21": 31,  # u
            "22": 32,  # v
            "23": 33,  # w
            "24": 34,  # x
            "25": 35,  # y
            "26": 36,  # z
            "27": 37,  # !
            "28": 38  # ?
        },
        "327": {  # TODO mapping is not really clear
            "121": 0,  # 08
            "122": 11,  # 09
            "123": 12,  # 19
            "124": 13,  # 11
            "125": 14,  # 12
            "126": 15,  # 13
            "127": 16,  # 14
            "128": 17,  # 15
            "129": 18,  # 16
            "130": 19,  # 17
            "131": 0,  # 18
            "132": 0,  # 19
        },
        "351": {
            "29": 11,  # normal
            "30": 12,  # sunny
            "31": 13,  # rainy
            "32": 14,  # snowy
        },
        "386": {
            "33": 11,  # normal
            "34": 12,  # attack
            "35": 13,  # defense
            "36": 14  # speed
        },
        "412": {
            "118": 11,  # plant
            "119": 12,  # sandy
            "120": 13  # trash
        },
        "413": {
            "87": 11,  # plant
            "88": 12,  # sandy
            "89": 13  # trash
        },
        "421": {
            "94": 11,  # overcast
            "95": 12,  # sunny
        },
        "422": {
            "96": 11,  # west sea
            "97": 12,  # east sea
        },
        "423": {
            "98": 11,  # west sea
            "99": 12,  # east sea
        },
        "479": {
            "81": 0,  # normal
            "82": 14,  # frost
            "83": 15,  # fan
            "84": 16,  # mow
            "85": 13,  # wash
            "86": 12,  # heat
        },
        "487": {
            "90": 11,  # altered
            "91": 12,  # origin
        },
        "492": {
            "92": 11,  # sky
            "93": 12,  # land
        },
        "493": {
            "100": 0,  # normal
            "101": 12,  # fighting
            "102": 13,  # flying
            "103": 14,  # poison
            "104": 15,  # ground
            "105": 16,  # rock
            "106": 17,  # bug
            "107": 18,  # ghost
            "108": 19,  # steel
            "109": 20,  # fire
            "110": 21,  # water
            "111": 22,  # grass
            "112": 23,  # electric
            "113": 24,  # psychic
            "114": 25,  # ice
            "115": 26,  # dragon
            "116": 27,  # dark
            "117": 28,  # fairy
        },

    }

    mon = forms.get(str(mon_id), None)

    # we don't have any form IDs
    if mon is None:
        return 0

    mon_form = mon.get(str(form_id), 0)

    return mon_form


def is_mon_ditto_raw(pokemon_data: pogoprotos.PokemonProto):
    logger.debug3('Determining if mon is a ditto')
    logger.debug4(pokemon_data)
    if pokemon_data.pokemon_display.is_strong_pokemon:
        logger.debug3('Strong/Mighty mon, skipping ditto check')
        return False
    weather_boost = pokemon_data.pokemon_display.weather_boosted_condition
    valid_atk = pokemon_data.individual_attack < 4
    valid_def = pokemon_data.individual_defense < 4
    valid_sta = pokemon_data.individual_stamina < 4
    cp_multi = pokemon_data.cp_multiplier
    valid_boost_attrs = valid_atk or valid_def or valid_sta or cp_multi < .3
    if weather_boost is None:
        return False
    elif weather_boost > 0 and valid_boost_attrs:
        # Weather boosted mon, but the iv is lower than threshold
        return True
    elif weather_boost == 0 and cp_multi > 0.733:
        # Not weather boosted, but the level is > 30
        return True
    else:
        return False


def calculate_cooldown(distance, speed):
    if distance >= 1335000:
        speed = 180.43  # Speed can be abt 650 km/h
    elif distance >= 1100000:
        speed = 176.2820513
    elif distance >= 1020000:
        speed = 168.3168317
    elif distance >= 1007000:
        speed = 171.2585034
    elif distance >= 948000:
        speed = 166.3157895
    elif distance >= 900000:
        speed = 164.8351648
    elif distance >= 897000:
        speed = 166.1111111
    elif distance >= 839000:
        speed = 158.9015152
    elif distance >= 802000:
        speed = 159.1269841
    elif distance >= 751000:
        speed = 152.6422764
    elif distance >= 700000:
        speed = 151.5151515
    elif distance >= 650000:
        speed = 146.3963964
    elif distance >= 600000:
        speed = 142.8571429
    elif distance >= 550000:
        speed = 138.8888889
    elif distance >= 500000:
        speed = 134.4086022
    elif distance >= 450000:
        speed = 129.3103448
    elif distance >= 400000:
        speed = 123.4567901
    elif distance >= 350000:
        speed = 116.6666667
    elif distance >= 328000:
        speed = 113.8888889
    elif distance >= 300000:
        speed = 108.6956522
    elif distance >= 250000:
        speed = 101.6260163
    elif distance >= 201000:
        speed = 90.54054054
    elif distance >= 175000:
        speed = 85.78431373
    elif distance >= 150000:
        speed = 78.125
    elif distance >= 125000:
        speed = 71.83908046
    elif distance >= 100000:
        speed = 64.1025641
    elif distance >= 90000:
        speed = 60
    elif distance >= 80000:
        speed = 55.55555556
    elif distance >= 70000:
        speed = 50.72463768
    elif distance >= 60000:
        speed = 47.61904762
    elif distance >= 45000:
        speed = 39.47368421
    elif distance >= 40000:
        speed = 35.0877193
    elif distance >= 35000:
        speed = 32.40740741
    elif distance >= 30000:
        speed = 29.41176471
    elif distance >= 25000:
        speed = 27.77777778
    elif distance >= 20000:
        speed = 27.77777778
    elif distance >= 15000:
        speed = 27.77777778
    elif distance >= 10000:
        speed = 23.80952381
    elif distance >= 8000:
        speed = 26.66666667
    elif distance >= 5000:
        speed = 22.34137623
    elif distance >= 4000:
        speed = 22.22222222
    delay_used = distance / speed
    if delay_used > 7200:  # There's a maximum of 2 hours wait time
        delay_used = 7200
    return delay_used
