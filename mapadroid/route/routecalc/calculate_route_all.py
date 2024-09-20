import asyncio
import math
import platform
from concurrent.futures import ProcessPoolExecutor
from typing import List

import numpy as np

from mapadroid.route.routecalc.calculate_route_quick import route_calc_impl
from mapadroid.utils.collections import Location
from mapadroid.utils.logging import LoggerEnums, get_logger, init_logging
from mapadroid.utils.madGlobals import MadGlobals, RoutecalculationTypes

logger = get_logger(LoggerEnums.routecalc)


def is_or_tools_available() -> bool:
    or_tools_available: bool = False
    if platform.architecture()[0] == "64bit":
        try:
            from ortools.constraint_solver import pywrapcp, routing_enums_pb2
            pywrapcp
            routing_enums_pb2
        except Exception:
            pass
        else:
            or_tools_available = True
    else:
        logger.info("OR Tools not available since the system is running {}", platform.architecture()[0])
    return or_tools_available


def create_data_model(less_coordinates):
    """Stores the data for the problem."""

    data = {'locations': []}

    # ortools requires x,y data to be integers
    # we will scale lat,lng to large numbers so that rounding won't adversely affect the path calculation
    for coord in less_coordinates:
        data['locations'].append((int(float(coord[0]) * 1e9), int(float(coord[1]) * 1e9)))

    data['num_vehicles'] = 1  # calculate as if only one walker on route
    data['depot'] = 0  # route will start at the first lat,lng
    return data


def compute_euclidean_distance_matrix(locations):
    """Creates callback to return distance between points."""
    distances = {}
    for from_counter, from_node in enumerate(locations):
        distances[from_counter] = {}
        for to_counter, to_node in enumerate(locations):
            if from_counter == to_counter:
                distances[from_counter][to_counter] = 0
            else:
                # Euclidean distance
                distances[from_counter][to_counter] = (int(
                    math.hypot((from_node[0] - to_node[0]),
                               (from_node[1] - to_node[1]))))
    return distances


def format_solution(manager, routing, solution):
    """Format the solution for MAD."""
    route_through_nodes = []
    index = routing.Start(0)
    while not routing.IsEnd(index):
        route_through_nodes.append(manager.IndexToNode(index))
        index = solution.Value(routing.NextVar(index))
    logger.debug("Done formatting solution.")
    return route_through_nodes


def _run_in_process_executor(method, less_coordinates, route_name):
    # Utility method to init logging in process executor...

    if not MadGlobals.application_args:
        MadGlobals.load_args()
    init_logging(MadGlobals.application_args, print_info=False)
    try:
        return method(less_coordinates, route_name)
    except Exception as e:
        logger.critical("Failed calculating route: {}", e)
        logger.exception(e)


def route_calc_ortools(less_coordinates, route_name):
    from ortools.constraint_solver import pywrapcp, routing_enums_pb2
    data = create_data_model(less_coordinates)

    # Create the routing index manager.
    manager = pywrapcp.RoutingIndexManager(len(data['locations']),
                                           data['num_vehicles'], data['depot'])

    # Create Routing Model.
    routing = pywrapcp.RoutingModel(manager)

    distance_matrix = compute_euclidean_distance_matrix(data['locations'])

    def distance_callback(from_index, to_index):
        """Returns the distance between the two nodes."""
        # Convert from routing variable Index to distance matrix NodeIndex.
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)

    # Define cost of each arc.
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Setting first solution heuristic.
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)

    # Solve the problem.
    logger.debug("OR-Tools routecalc starting for route: {}", route_name)
    solution = routing.SolveWithParameters(search_parameters)
    logger.debug("OR-Tools routecalc finished for route: {}", route_name)

    return format_solution(manager, routing, solution)


async def route_calc_all(coords: List[Location], route_name, algorithm: RoutecalculationTypes):
    # check to see if we can use OR-Tools to perform our routecalc
    coords_for_calc = np.zeros(shape=(len(coords), 2))
    for i in range(len(coords)):
        coords_for_calc[i][0] = coords[i].lat
        coords_for_calc[i][1] = coords[i].lng
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor() as executor:
        if is_or_tools_available() and algorithm.OR_TOOLS:
            logger.debug("Using OR-Tools for routecalc")
            sol_best = await loop.run_in_executor(
                executor, _run_in_process_executor, route_calc_ortools, coords_for_calc, route_name)
        else:
            logger.debug("Using MAD quick routecalc")
            sol_best = await loop.run_in_executor(
                executor, _run_in_process_executor, route_calc_impl, coords_for_calc, route_name)
    logger.debug("Solution has {} coordinates", len(sol_best))
    return sol_best
