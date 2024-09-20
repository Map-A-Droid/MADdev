import asyncio
import json
import time
from asyncio import Task
from typing import Any, Dict, List, Set, Tuple

from mapadroid.db.DbWebhookReader import DbWebhookReader
from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.db.model import Pokestop, TrsQuest
from mapadroid.geofence.geofenceHelper import GeofenceHelper
from mapadroid.mapping_manager import MappingManager
from mapadroid.utils.gamemechanicutil import calculate_mon_level
from mapadroid.utils.json_encoder import mad_json_dumps
from mapadroid.utils.logging import LoggerEnums, get_logger
from mapadroid.utils.madGlobals import MonSeenTypes, terminate_mad
from mapadroid.utils.questGen import QuestGen
from mapadroid.utils.RestHelper import RestApiResult, RestHelper
from mapadroid.utils.s2Helper import S2Helper

logger = get_logger(LoggerEnums.webhook)


class WebhookWorker:
    __excluded_areas = {}

    def __init__(self, args, db_wrapper: DbWrapper, mapping_manager: MappingManager, rarity, quest_gen: QuestGen):
        self.__quest_gen: QuestGen = quest_gen
        self.__worker_interval_sec = args.webhook_worker_interval
        self.__args = args
        self.__db_wrapper: DbWrapper = db_wrapper
        self.__rarity = rarity
        self.__last_check = int(time.time())
        self.__webhook_receivers = []
        self.__webhook_types: Set[str] = set()
        self.__pokemon_types: Set[MonSeenTypes] = set()
        self.__mapping_manager: MappingManager = mapping_manager
        self.__valid_types: Set[str] = {
            'pokemon', 'raid', 'weather', 'quest', 'gym', 'pokestop'
        }
        self.__valid_mon_types: Set[MonSeenTypes] = {
            MonSeenTypes.encounter, MonSeenTypes.wild, MonSeenTypes.nearby_stop, MonSeenTypes.nearby_cell,
            MonSeenTypes.lure_wild, MonSeenTypes.lure_encounter
        }

    def __payload_type_count(self, payload):
        count = {}

        for elem in payload:
            count[elem["type"]] = count.get(elem["type"], 0) + 1

        return count

    def __payload_chunk(self, payload, size):
        if size == 0:
            return [payload]

        return [payload[x: x + size] for x in range(0, len(payload), size)]

    def __is_in_excluded_area(self, coordinate):
        for gfh in self.__excluded_areas:
            if gfh.is_coord_inside_include_geofence(coordinate):
                return True

        return False

    async def __send_webhook(self, payloads):
        if len(payloads) == 0:
            logger.debug2("Payload empty. Skip sending to webhook.")
            return

        current_wh_num = 1
        for webhook in self.__webhook_receivers:
            payload_to_send = []
            sub_types = webhook.get('types')

            if sub_types is not None:
                for payload in payloads:
                    if payload["type"] in sub_types or \
                            (payload["message"].get("seen_type", None) in sub_types):
                        payload_to_send.append(payload)
            else:
                payload_to_send = payloads

            if len(payload_to_send) == 0:
                logger.debug2("Payload empty. Skip sending to: {} (Filter: {})", webhook.get('url'), sub_types)
                continue
            else:
                logger.debug2("Sending to webhook: {} (Filter: {})", webhook.get('url'), sub_types)

            payload_list = self.__payload_chunk(
                payload_to_send, self.__args.webhook_max_payload_size
            )

            current_pl_num = 1
            for payload_chunk in payload_list:
                logger.debug4("Python data for payload: {}", payload_chunk)
                logger.debug4("Payload: {}", await mad_json_dumps(payload_chunk))

                try:
                    response: RestApiResult = await RestHelper.send_post(webhook.get('url'),
                                                                         data=payload_chunk,
                                                                         headers={"Content-Type": "application/json"},
                                                                         params=None,
                                                                         timeout=5)
                    if response.status_code != 200:
                        logger.warning("Webhook destination {} returned status code other than 200 OK: {}",
                                       webhook.get('url'), response.status_code)
                    else:
                        if len(self.__webhook_receivers) > 1:
                            whcount_text = " [wh {}/{}]".format(current_wh_num, len(self.__webhook_receivers))
                        else:
                            whcount_text = ""

                        if len(payload_list) > 1:
                            whchunk_text = " [pl {}/{}]".format(current_pl_num, len(payload_list))
                        else:
                            whchunk_text = ""

                        logger.success("Successfully sent payload to webhook{}{}. Stats: {}", whchunk_text,
                                       whcount_text, await mad_json_dumps(self.__payload_type_count(payload_chunk)))
                except Exception as e:
                    logger.warning("Exception occured while sending webhook: {}", e)
                    logger.exception(e)

                current_pl_num += 1
            current_wh_num += 1

    async def __prepare_quest_data(self, quest_data: Dict[int, Tuple[Pokestop, Dict[int, TrsQuest]]]):
        ret = []
        for stop, quests in quest_data.values():
            if self.__is_in_excluded_area([stop.latitude, stop.longitude]):
                continue
            for layer, quest in quests.items():
                try:
                    transformed_quest = await self.__quest_gen.generate_quest(stop, quest)
                    quest_payload = self.__construct_quest_payload(transformed_quest)

                    entire_payload = {"type": "quest", "message": quest_payload}
                    ret.append(entire_payload)
                except Exception as e:
                    logger.error("Exception occured while generating quest webhook: {}", e)
                    logger.exception(e)

        return ret

    def __construct_quest_payload(self, transformed_quest: Dict) -> Dict:
        if self.__args.quest_webhook_flavor == "default":
            # used by PokeAlarm
            return {
                "pokestop_id": transformed_quest["pokestop_id"],
                "latitude": transformed_quest["latitude"],
                "longitude": transformed_quest["longitude"],
                "quest_type": transformed_quest["quest_type"],
                "quest_type_raw": transformed_quest["quest_type_raw"],
                "item_type": transformed_quest["item_type"],
                "name": transformed_quest["name"].replace('"', '\\"').replace("\n", "\\n") if transformed_quest.get("name") else None,
                "url": transformed_quest["url"],
                "timestamp": transformed_quest["timestamp"],
                "quest_reward_type": transformed_quest["quest_reward_type"],
                "quest_reward_type_raw": transformed_quest["quest_reward_type_raw"],
                "quest_reward_raw": transformed_quest['quest_reward_raw'].replace("'", '"').lower(),
                "quest_target": transformed_quest["quest_target"],
                "pokemon_id": int(transformed_quest["pokemon_id"]),
                "pokemon_form": int(transformed_quest.get("pokemon_form", '0')),
                "pokemon_costume": int(transformed_quest.get("pokemon_costume", '0')),
                "item_amount": transformed_quest["item_amount"],
                "item_id": transformed_quest["item_id"],
                "quest_task": transformed_quest["quest_task"],
                "quest_condition": transformed_quest["quest_condition"].replace("'", '"').lower(),
                "quest_template": transformed_quest["quest_template"],
                "is_ar_scan_eligible": transformed_quest["is_ar_scan_eligible"],
                "quest_title": transformed_quest["quest_title"],
                "with_ar": bool(transformed_quest["quest_layer"])
            }

        # Other known type is Poracle/RDM compatible.

        # For some reason we aren't saving JSON in our databse, so we gotta replace ' with ".
        # Pray that we never save strings that contain ' inside them.
        quest_conditions = json.loads(transformed_quest["quest_condition"].replace("'", '"'))
        quest_condition = []
        quest_rewards = []
        a_quest_reward_type = transformed_quest["quest_reward_type_raw"]
        a_quest_reward = {}
        quest_rewards.append(a_quest_reward)
        a_quest_reward["info"] = {}
        a_quest_reward["type"] = a_quest_reward_type

        if a_quest_reward_type == 2:
            a_quest_reward["info"]["item_id"] = transformed_quest["item_id"]
            a_quest_reward["info"]["amount"] = int(transformed_quest["item_amount"])
        elif a_quest_reward_type == 3:
            a_quest_reward["info"]["amount"] = int(transformed_quest["item_amount"])
        elif a_quest_reward_type == 4:
            a_quest_reward["info"]["amount"] = int(transformed_quest["item_amount"])
            a_quest_reward["info"]["pokemon_id"] = int(transformed_quest["pokemon_id"])
        elif a_quest_reward_type == 7:
            a_quest_reward["info"]["pokemon_id"] = int(transformed_quest["pokemon_id"])
            a_quest_reward["info"]["form_id"] = int(transformed_quest["pokemon_form"])
            a_quest_reward["info"]["costume_id"] = int(transformed_quest.get("pokemon_costume", '0'))
            a_quest_reward["info"]["shiny"] = 0
            a_quest_reward["info"]["form"] = int(transformed_quest["pokemon_form"])
        elif a_quest_reward_type == 12:
            a_quest_reward["info"]["pokemon_id"] = int(transformed_quest["pokemon_id"])
            a_quest_reward["info"]["amount"] = int(transformed_quest["item_amount"])

        for a_quest_condition in quest_conditions:
            # condition for special type of pokemon (type = 1)
            if "with_pokemon_type" in a_quest_condition:
                a_quest_condition["info"] = a_quest_condition.pop("with_pokemon_type")
                a_quest_condition["info"]["pokemon_type_ids"] = a_quest_condition[
                    "info"
                ].pop("pokemon_type")
            if "with_pokemon_category" in a_quest_condition:
                a_quest_condition["info"] = a_quest_condition.pop("with_pokemon_category")

            # Condition for mons with weather boost (type = 3) holds no additional info.
            # Condition for being first catch of the day (type = 4) holds no additional info.
            # Condition for being first spin of the day (type = 5) holds no additional info.
            # Condition for raid status (type= 6) holds no addition info, (means we have to win the raid)

            # Quest condition for raid level(s). (type = 7)
            if "with_raid_level" in a_quest_condition:
                a_quest_condition["info"] = a_quest_condition.pop("with_raid_level")
                a_quest_condition["info"]["raid_levels"] = a_quest_condition[
                    "info"
                ].pop("raid_level")
            # Quest condition for throw type. (type = 8 and type = 14)
            if "with_throw_type" in a_quest_condition:
                a_quest_condition["info"] = a_quest_condition.pop("with_throw_type")
                a_quest_condition["info"]["throw_type_id"] = a_quest_condition[
                    "info"
                ].pop("throw_type")

            # Quest condition for Winning the gym battle (type = 9) has no additional info
            # Quest condition for using a super effective charge attack (type = 10) has no additional info

            # Quest condition for use of items (type = 11)
            if "with_item" in a_quest_condition:
                a_quest_condition["info"] = a_quest_condition.pop("with_item")
                a_quest_condition["info"]["item_id"] = a_quest_condition["info"].pop(
                    "item"
                )

            # Quest condition that pokestop has to be new (type = 12) has no additional info

            # Quest condition for quest context (type 13) is only used for flagging story/challenge quests

            # Quest condition for throws in a row (type = 14) is the same as for type 8 handled above

            # Quest condition for curveballs (type = 15) has no additional info

            # Quest condition for badge types (type = 16)

            # Quest condition for player level (type = 17)

            # Quest condition for battle status (type = 18)

            # Quest condition for new friends (type = 19)

            # Quest condition for number of days in a row (type = 20) (so far only used in special research)

            # Quest condition for unique pokemons (type = 21)

            # Quest condition for npc combat (type = 22) has no additional info (that we care of)

            # Quest condition for battling another trainer (type = 23)
            if "with_pvp_combat" in a_quest_condition:
                a_quest_condition['info'] = a_quest_condition['with_pvp_combat']

            # Quest condition for location (type = 24) is unused

            # Quest condition for distance (type = 25)
            if "with_distance" in a_quest_condition:
                a_quest_condition['info'] = a_quest_condition['with_distance']
                a_quest_condition['info']['distance'] = a_quest_condition['info'].pop('distance_km')

            # Quest condition for pokemon alignment (shadow/purified) (type = 26)
            if "with_pokemon_alignment" in a_quest_condition:
                a_quest_condition['info'] = a_quest_condition['with_pokemon_alignment']

            # Quest condition for grunts, rocket leaders and other friends (type = 27)
            if "with_invasion_character" in a_quest_condition:
                a_quest_condition['info'] = a_quest_condition['with_invasion_character']
                a_quest_condition['info']['character_category_ids'] = a_quest_condition['info'].pop('category')

            # Quest condition for snapshots with buddy (type = 28)
            if "with_buddy" in a_quest_condition:
                a_quest_condition['info'] = a_quest_condition['with_buddy']

            quest_condition.append(a_quest_condition)

        return {
            "pokestop_id": transformed_quest["pokestop_id"],
            "template": transformed_quest["quest_template"],
            "pokestop_name": transformed_quest["name"].replace("\n", "\\n"),
            "pokestop_url": transformed_quest["url"],
            "conditions": quest_condition,
            "type": transformed_quest["quest_type_raw"],
            "latitude": transformed_quest["latitude"],
            "longitude": transformed_quest["longitude"],
            "rewards": quest_rewards,
            "target": transformed_quest["quest_target"],
            "updated": transformed_quest["timestamp"],
            "quest_task": transformed_quest["quest_task"],
            "quest_type": transformed_quest["quest_type"],
            "quest_type_raw": transformed_quest["quest_type_raw"],
            "item_type": transformed_quest["item_type"],
            "name": transformed_quest["name"].replace('"', '\\"').replace("\n", "\\n") if transformed_quest.get(
                "name") else None,
            "url": transformed_quest["url"],
            "timestamp": transformed_quest["timestamp"],
            "quest_reward_type": transformed_quest["quest_reward_type"],
            "quest_reward_type_raw": transformed_quest["quest_reward_type_raw"],
            "quest_reward_raw": transformed_quest['quest_reward_raw'].replace("'", '"').lower(),
            "quest_target": transformed_quest["quest_target"],
            "pokemon_id": int(transformed_quest["pokemon_id"]),
            "pokemon_form": int(transformed_quest.get("pokemon_form", '0')),
            "pokemon_costume": int(transformed_quest.get("pokemon_costume", '0')),
            "item_amount": transformed_quest["item_amount"],
            "item_id": transformed_quest["item_id"],
            "quest_condition": transformed_quest["quest_condition"].replace("'", '"').lower(),
            "quest_template": transformed_quest["quest_template"],
            "is_ar_scan_eligible": transformed_quest["is_ar_scan_eligible"],
            "with_ar": bool(transformed_quest["quest_layer"])
        }

    def __prepare_weather_data(self, weather_data):
        ret = []

        for weather in weather_data:
            weather_payload = {
                "s2_cell_id": weather["s2_cell_id"],
                "condition": weather["gameplay_weather"],
                "alert_severity": weather["severity"],
                "day": weather["world_time"],
                "time_changed": weather["last_updated"],
            }

            if weather.get("latitude", None) is None:
                weather_payload["latitude"] = S2Helper.middle_of_cell(
                    weather["s2_cell_id"]
                )[0]
            else:
                weather_payload["latitude"] = weather["latitude"]

            if weather.get("longitude", None) is None:
                weather_payload["longitude"] = S2Helper.middle_of_cell(
                    weather["s2_cell_id"]
                )[1]
            else:
                weather_payload["longitude"] = weather["longitude"]

            if weather.get("coords", None) is None:
                weather_payload["coords"] = S2Helper.coords_of_cell(
                    weather["s2_cell_id"]
                )
            else:
                weather_payload["coords"] = weather["coords"]

            entire_payload = {"type": "weather", "message": weather_payload}
            ret.append(entire_payload)

        return ret

    def __prepare_raid_data(self, raid_data):
        ret = []

        for raid in raid_data:
            if self.__is_in_excluded_area([raid["latitude"], raid["longitude"]]):
                continue

            # skip ex raid mon if disabled
            is_exclusive = raid["is_exclusive"] is not None and raid["is_exclusive"] != 0
            if not self.__args.webhook_submit_exraids and is_exclusive:
                continue

            raid_payload = {
                "latitude": raid["latitude"],
                "longitude": raid["longitude"],
                "level": raid["level"],
                "pokemon_id": raid["pokemon_id"],
                "team_id": raid["team_id"],
                "cp": raid["cp"],
                "start": raid["start"],
                "end": raid["end"],
                "name": raid["name"],
                "evolution": raid["evolution"],
                "spawn": raid["spawn"],
            }

            if raid["move_1"] is not None:
                raid_payload["move_1"] = raid["move_1"]

            if raid["move_2"] is not None:
                raid_payload["move_2"] = raid["move_2"]

            if raid["pokemon_id"] is None:
                raid_payload["pokemon_id"] = 0

            if raid["gym_id"] is not None:
                raid_payload["gym_id"] = raid["gym_id"]

            if raid["url"] is not None and raid["url"]:
                raid_payload["url"] = raid["url"]

            if raid["weather_boosted_condition"] is not None:
                raid_payload["weather"] = raid["weather_boosted_condition"]

            if raid["form"] is not None:
                raid_payload["form"] = raid["form"]

            if raid["is_ex_raid_eligible"] is not None:
                raid_payload["is_ex_raid_eligible"] = raid["is_ex_raid_eligible"] != 0

            if raid["is_exclusive"] is not None:
                raid_payload["is_exclusive"] = raid["is_exclusive"] != 0

            if raid["gender"] is not None:
                raid_payload["gender"] = raid["gender"]

            if raid["costume"] is not None:
                raid_payload["costume"] = raid["costume"]

            # create final message
            entire_payload = {"type": "raid", "message": raid_payload}

            # add to payload
            ret.append(entire_payload)

        return ret

    def __prepare_mon_data(self, mon_data: List[Dict]):
        ret = []

        for mon in mon_data:
            if self.__is_in_excluded_area([mon["latitude"], mon["longitude"]]):
                logger.debug3("Webhook ignoring (excluded area) mon ID {} with encounter ID {}. Stats: {}/{}/{}",
                              mon["pokemon_id"],
                              mon["encounter_id"],
                              mon.get("individual_attack"),
                              mon.get("individual_defense"),
                              mon.get("individual_stamina"))
                continue

            mon_payload = {
                "encounter_id": str(mon["encounter_id"]),
                "pokemon_id": mon["pokemon_id"],
                "display_pokemon_id": mon['display_pokemon'],
                "spawnpoint_id": mon["spawnpoint_id"],
                "latitude": mon["latitude"],
                "longitude": mon["longitude"],
                "disappear_time": mon["disappear_time"],
                "verified": mon["spawn_verified"],
                "seen_type": str(mon["seen_type"])
            }

            # get rarity
            pokemon_rarity = self.__rarity.rarity_by_id(pokemonid=mon["pokemon_id"])

            if mon.get("cp_multiplier", None) is not None:
                mon_payload["cp_multiplier"] = mon["cp_multiplier"]
                mon_payload["pokemon_level"] = calculate_mon_level(mon["cp_multiplier"])

            if mon["form"] is not None and mon["form"] > 0:
                mon_payload["form"] = mon["form"]

            if mon["display_form"] is not None and mon["display_form"] > 0:
                mon_payload["display_form"] = mon["display_form"]

            if mon["costume"] is not None:
                mon_payload["costume"] = mon["costume"]

            if mon["display_costume"] is not None and mon["display_costume"] > 0:
                mon_payload["display_costume"] = mon["display_costume"]

            if mon["cp"] is not None:
                mon_payload["cp"] = mon["cp"]

            if mon["individual_attack"] is not None:
                mon_payload["individual_attack"] = mon["individual_attack"]

            if mon["individual_defense"] is not None:
                mon_payload["individual_defense"] = mon["individual_defense"]

            if mon["individual_stamina"] is not None:
                mon_payload["individual_stamina"] = mon["individual_stamina"]
            logger.debug3("Webhook sending mon ID {} with encounter ID {}. Stats: {}/{}/{}", mon["pokemon_id"],
                          mon["encounter_id"],
                          mon.get("individual_attack"),
                          mon.get("individual_defense"),
                          mon.get("individual_stamina"))
            if mon["move_1"] is not None:
                mon_payload["move_1"] = mon["move_1"]

            if mon["move_2"] is not None:
                mon_payload["move_2"] = mon["move_2"]

            if mon.get("height", None) is not None:
                mon_payload["height"] = mon["height"]

            if mon["weight"] is not None:
                mon_payload["weight"] = mon["weight"]

            if mon["gender"] is not None:
                mon_payload["gender"] = mon["gender"]

            if mon["display_gender"] is not None:
                mon_payload["display_gender"] = mon["display_gender"]

            if mon.get("size", None) is not None:
                mon_payload["size"] = mon.get("size")

            if pokemon_rarity is not None:
                mon_payload["rarity"] = pokemon_rarity

            if mon["base_catch"] is not None:
                mon_payload["base_catch"] = mon["base_catch"]
                mon_payload["great_catch"] = mon["great_catch"]
                mon_payload["ultra_catch"] = mon["ultra_catch"]

            if mon["weather_boosted_condition"] is not None \
                    and mon["weather_boosted_condition"] > 0:
                mon_payload["weather"] = mon["weather_boosted_condition"]

            if mon["seen_type"] in (MonSeenTypes.nearby_stop.name, MonSeenTypes.lure_wild.name,
                                    MonSeenTypes.lure_encounter.name):
                mon_payload["pokestop_id"] = mon["fort_id"]
                mon_payload["pokestop_name"] = mon.get("stop_name")
                mon_payload["pokestop_url"] = mon.get("stop_url")

                if mon["seen_type"] == MonSeenTypes.nearby_stop.name:
                    mon_payload["verified"] = False
                else:
                    mon_payload["verified"] = True

            if mon["seen_type"] == MonSeenTypes.nearby_cell.name:
                mon_payload["cell_coords"] = S2Helper.coords_of_cell(
                    mon["cell_id"]
                )
                mon_payload["cell_id"] = mon["cell_id"]
                mon_payload["verified"] = False

            entire_payload = {"type": "pokemon", "message": mon_payload}
            ret.append(entire_payload)

        return ret

    def __prepare_gyms_data(self, gym_data):
        ret = []

        for gym in gym_data:
            if self.__is_in_excluded_area([gym["latitude"], gym["longitude"]]):
                continue

            gym_payload = {
                "gym_id": gym["gym_id"],
                "latitude": gym["latitude"],
                "longitude": gym["longitude"],
                "team_id": gym["team_id"],
                "name": gym["name"],
                "slots_available": gym["slots_available"],
                "is_ar_scan_eligible": gym["is_ar_scan_eligible"],
                "is_in_battle": gym["is_in_battle"]
            }

            if gym.get("description", None) is not None:
                gym_payload["description"] = gym.get("description")

            if gym["url"] is not None:
                gym_payload["url"] = gym["url"]

            if gym["is_ex_raid_eligible"] is not None:
                gym_payload["is_ex_raid_eligible"] = gym["is_ex_raid_eligible"]

            entire_payload = {"type": "gym", "message": gym_payload}
            ret.append(entire_payload)

        return ret

    def __prepare_stops_data(self, pokestop_data: List[Dict[str, Any]]):
        ret = []

        for pokestop in pokestop_data:
            if self.__is_in_excluded_area([pokestop["latitude"], pokestop["longitude"]]):
                continue

            pokestop_payload = {
                "name": pokestop["name"],
                "pokestop_id": pokestop["pokestop_id"],
                "latitude": pokestop["latitude"],
                "longitude": pokestop["longitude"],
                "updated": pokestop["last_updated"],
                "last_modified": pokestop["last_modified"]
            }

            if 'active_fort_modifier' in pokestop and pokestop["active_fort_modifier"]:
                pokestop_payload["lure_expiration"] = pokestop["lure_expiration"]
                pokestop_payload["lure_id"] = pokestop["active_fort_modifier"]

            if pokestop["image"]:
                pokestop_payload["url"] = pokestop["image"]

            if pokestop.get("incident_start"):
                pokestop_payload["incident_start"] = pokestop["incident_start"]

            if pokestop.get("incident_expiration"):
                pokestop_payload["incident_expiration"] = pokestop["incident_expiration"]

            if pokestop.get("incident_grunt_type"):
                pokestop_payload["incident_grunt_type"] = pokestop["incident_grunt_type"]

            if pokestop.get("incident_display_type"):
                pokestop_payload["incident_display_type"] = pokestop["incident_display_type"]

            pokestop_payload["incidents"] = pokestop["incidents"]

            entire_payload = {"type": "pokestop", "message": pokestop_payload}
            ret.append(entire_payload)

        return ret

    def __build_webhook_receivers(self):
        webhooks = self.__args.webhook_url.split(",")

        for webhook in webhooks:
            sub_types = None
            url = webhook.strip()

            if url.startswith("["):
                end_pos = url.index("]")
                raw_sub_types = url[1:end_pos].strip()
                url = url[end_pos + 1:]
                sub_types = raw_sub_types.split(" ")
                sub_types = [t.replace(" ", "") for t in sub_types]

                if "pokemon" in sub_types:
                    sub_types.append("encounter")

                for vtype in self.__valid_types:
                    if vtype in sub_types:
                        self.__webhook_types.add(vtype)
                    for vmtype in self.__valid_mon_types:
                        if vmtype.name in sub_types:
                            self.__pokemon_types.add(vmtype)
            else:
                for valid_mon_type in self.__valid_mon_types:
                    self.__pokemon_types.add(valid_mon_type)
                for valid_type in self.__valid_types:
                    self.__webhook_types.add(valid_type)

            self.__webhook_receivers.append({
                "url": url.replace(" ", ""),
                "types": sub_types
            })

    async def __build_excluded_areas(self):
        self.__excluded_areas: List[GeofenceHelper] = []

        if self.__args.webhook_excluded_areas == "":
            pass

        tmp_excluded_areas = {}
        for rm in await self.__mapping_manager.get_all_routemanager_ids():
            name = await self.__mapping_manager.routemanager_get_name(rm)
            gfh = await self.__mapping_manager.routemanager_get_geofence_helper(rm)
            tmp_excluded_areas[name] = gfh

        area_names = self.__args.webhook_excluded_areas.split(",")
        for area_name in area_names:
            area_name = area_name.strip()
            for name, gf in tmp_excluded_areas.items():
                if (area_name.endswith("*") and name.startswith(area_name[:-1])) or area_name == name:
                    self.__excluded_areas.append(gf)

        tmp_excluded_areas = None

        if len(self.__excluded_areas) > 0:
            logger.info("Excluding {} areas from webhooks", len(self.__excluded_areas))

    async def __create_payload(self):
        logger.debug("Fetching data changed since {}", self.__last_check)

        # the payload that is about to be sent
        full_payload = []
        async with self.__db_wrapper as session, session:
            # TODO: Single transaction...
            try:
                # raids
                if 'raid' in self.__webhook_types:
                    raids = self.__prepare_raid_data(
                        await DbWebhookReader.get_raids_changed_since(session, self.__last_check)
                    )
                    full_payload += raids

                # quests
                if 'quest' in self.__webhook_types:
                    quest = await self.__prepare_quest_data(
                        await DbWebhookReader.get_quests_changed_since(session, self.__last_check)
                    )
                    full_payload += quest

                # weather
                if 'weather' in self.__webhook_types:
                    weather = self.__prepare_weather_data(
                        await DbWebhookReader.get_weather_changed_since(session, self.__last_check)
                    )
                    full_payload += weather

                # gyms
                if 'gym' in self.__webhook_types:
                    gyms = self.__prepare_gyms_data(
                        await DbWebhookReader.get_gyms_changed_since(session, self.__last_check)
                    )
                    full_payload += gyms

                # stops
                if 'pokestop' in self.__webhook_types:
                    pokestops = self.__prepare_stops_data(
                        await DbWebhookReader.get_stops_changed_since(session, self.__last_check)
                    )
                    full_payload += pokestops

                # mon
                if self.__pokemon_types:
                    mon = self.__prepare_mon_data(
                        await DbWebhookReader.get_mon_changed_since(session, self.__last_check, self.__pokemon_types)
                    )
                    full_payload += mon
            except Exception as e:
                logger.exception(e)
                logger.exception("Error while creating webhook payload")

        logger.debug("Done fetching data + building payload")

        return full_payload

    async def start(self) -> Task:
        loop = asyncio.get_running_loop()
        return loop.create_task(self.__run_worker())

    async def __run_worker(self):
        logger.info("Starting webhook worker thread, sending payload every {} seconds", self.__worker_interval_sec)

        self.__build_webhook_receivers()
        await self.__build_excluded_areas()

        if self.__args.webhook_start_time != 0:
            self.__last_check = int(self.__args.webhook_start_time)

        while not terminate_mad.is_set():
            # Always check modifications of intervals N - 6 to NOW given processing of queues may take some time...
            preparing_timestamp = int(time.time()) - 6 * self.__worker_interval_sec

            # fetch data and create payload
            full_payload = await self.__create_payload()

            # send our payload
            await self.__send_webhook(full_payload)

            self.__last_check = preparing_timestamp
            await asyncio.sleep(self.__worker_interval_sec)

        logger.info("Stopping webhook worker thread")
