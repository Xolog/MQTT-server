import sys
import socket

from typing import Tuple
from loguru import logger
from itertools import groupby

from .utils import get_node_id_from_addr
from .utils import NeighbourChecker as checker

logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="DEBUG")


class NodeMonitor:
    def __init__(self, node_id: int, ping_port: int, host_ip: str):
        self.node_id = node_id
        self.ping_port = ping_port
        self.host_ip = host_ip
        self.neighbors: list = []

    # --------------------поиск соседей--------------------
    def init_neighbors(self) -> list:
        logger.info('Поиск соседей...')
        neighbors = self.get_neighbors()
        self.set_neighbors(neighbors)

        return self.neighbors

    def get_neighbors(self) -> list:
        neighbors = [el for el, _ in groupby(self.__search_neighbors())]

        return neighbors

    def __search_neighbors(self) -> Tuple[str, str]:
        left_neighbour, right_neighbour = False, False
        while not left_neighbour and not right_neighbour:
            left_neighbour, right_neighbour = self.__start_search_neighbors()
            if not left_neighbour and not right_neighbour:
                logger.warning('Соседи не найдены...')

        return left_neighbour, right_neighbour

    def __start_search_neighbors(self) -> Tuple[str, str]:
        """Функция принимает номер текущей подсети node_number
        и в зависимости от него задаёт параметры для старта поиска соседей
        right_search_neighbour() и left_search_neighbour()
        """

        if self.node_id == 1:
            right_neighbour = self.__right_search_neighbour(self.node_id + 1, 256)
            left_neighbour = self.__left_search_neighbour(255, 1)

        elif self.node_id == 255:
            right_neighbour = self.__right_search_neighbour(1, 255)
            left_neighbour = self.__left_search_neighbour(self.node_id - 1, 0)

        else:
            right_neighbour = self.__right_search_neighbour(self.node_id + 1, 256)
            left_neighbour = self.__left_search_neighbour(self.node_id - 1, 0)

        return left_neighbour, right_neighbour

    def __right_search_neighbour(self, start_node: int, end_node: int, step: int = 1) -> str:
        """Запускает поиск соседей search_neighbour() справа от подсети текущей машины.
        Если до последней возможной подсети сосед не найден,
        начинает новый поиск с первого возможного узла до текущего, выводит найден сосед или нет
        """

        neighbour_found = self.__search_neighbour(start_node, end_node, step)

        if not neighbour_found:
            neighbour_found = self.__search_neighbour(1, start_node - 1, step)

        return neighbour_found

    def __left_search_neighbour(self, start_node: int, end_node: int, step: int = -1) -> str:
        """Запускает поиск соседей search_neighbour() слева от подсети текущей машины.
        Если до последней возможной подсети сосед не найден,
        начинает новый поиск от последнего возможного узла до текущего, выводит найден сосед или нет
        """

        neighbour_found = self.__search_neighbour(start_node, end_node, step)

        if not neighbour_found:
            neighbour_found = self.__search_neighbour(255, start_node, step)

        return neighbour_found

    def __search_neighbour(self, start_node: int, end_node: int, step: int) -> str:
        """Запускает попытку подключения try_connect_to_node() к узлу node,
        возвращает флаг-значение neighbour_found обозначающее найден сосед или нет
        """

        neighbour_found = False
        for node in range(start_node, end_node, step):
            neighbour_found = self.__get_address_or_false(node)
            if neighbour_found:
                return neighbour_found

        return neighbour_found

    def __get_address_or_false(self, target_node: int) -> str or bool:
        """Формирует целевой ip target_ip для подключения, создаёт сокет tcp_client_socket
         и пробует подключиться connect_node(), после чего передаёт результат
         """

        target_ip = f'10.20.{target_node}.1'

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_client_socket:
            try:
                result = self.__connect_node(target_ip, tcp_client_socket)
                if result == 0 and self.host_ip != target_ip:
                    return target_ip
                else:
                    return False
            except BrokenPipeError:
                print('Node undefined')
            except OSError:
                print('Network not find')

    def __connect_node(self, target_ip: str, tcp_client_socket: socket) -> int:
        """Задаёт время на попытку подключения и пробует его провести, возвращает 0 в случае успеха
        """

        tcp_client_socket.settimeout(0.15)

        return tcp_client_socket.connect_ex((target_ip, self.ping_port))

    # --------------------обработка соседей--------------------
    def set_neighbors(self, neighbors: list) -> None:
        self.neighbors = checker.clear_neighbors(*neighbors)

    # --------------------проверка доступности соседей--------------------
    def check_gone_neighbors(self) -> tuple:
        old_neighbors_status = self.__get_neighbors_status()
        gone_neighbors = ()
        if False in old_neighbors_status:
            gone_neighbors = self.__identify_gone_neighbors(old_neighbors_status)

        return gone_neighbors

    def __get_neighbors_status(self) -> list:
        statuses = [self.__get_address_or_false(get_node_id_from_addr(neighbour)) for neighbour in self.neighbors]

        return statuses

    def __identify_gone_neighbors(self, neighbors_statuses: list) -> tuple:
        deleted_neighbors = []
        for neighbour_status, old_neighbour in zip(neighbors_statuses, self.neighbors):
            if neighbour_status is False:
                deleted_neighbors.append(old_neighbour)

        return tuple(deleted_neighbors)

    def check_back_neighbors(self, new_neighbors: list, nodes: dict) -> tuple:
        back_neighbors = []
        nodes = tuple(self.__get_nodes_with_new_and_old_neighbors(new_neighbors, nodes))

        distances_new_neighbors = []
        for neighbour in new_neighbors:
            distances_new_neighbors.append(self.__get_distance_to_node(nodes, get_node_id_from_addr(neighbour)))

        distances_old_neighbors = []
        for neighbour in self.neighbors:
            distances_old_neighbors.append(self.__get_distance_to_node(nodes, get_node_id_from_addr(neighbour)))

        if len(self.neighbors) == 2:
            if len(new_neighbors) == 1:
                is_new_neighbour_closer = False
                is_neighbour_not_old = False
                for i, distance in enumerate(distances_old_neighbors):
                    if distances_new_neighbors[0] < distance:
                        is_new_neighbour_closer = True
                    if distances_new_neighbors[0] == distance and new_neighbors[0] != self.neighbors[i]:
                        is_neighbour_not_old = True

                if is_new_neighbour_closer and is_neighbour_not_old:
                    back_neighbors.append(new_neighbors[0])

            elif len(new_neighbors) == 2:
                for i, (distance_new_neighbour, distance_old_neighbour) in enumerate(zip(distances_new_neighbors,
                                                                                         distances_old_neighbors)):
                    if distance_new_neighbour < distance_old_neighbour:
                        back_neighbors.append(new_neighbors[i])

        elif len(self.neighbors) == 1:
            if len(new_neighbors) == 1 and self.neighbors != new_neighbors:
                back_neighbors.append(new_neighbors[0])
            elif len(new_neighbors) == 2:
                for i, distance in enumerate(distances_new_neighbors):
                    if distances_old_neighbors[0] < distance:
                        back_neighbors.append(new_neighbors[i])
                    elif distances_old_neighbors[0] == distance and new_neighbors[i] != self.neighbors[0]:
                        back_neighbors.append(new_neighbors[i])

        return tuple(back_neighbors)

    def __get_nodes_with_new_and_old_neighbors(self, new_neighbors: list, nodes: dict) -> list:
        nodes = set(nodes.keys())

        for neighbour in new_neighbors:
            new_node = get_node_id_from_addr(neighbour)
            if new_node not in nodes:
                nodes.add(new_node)

        for neighbour in self.neighbors:
            old_node = get_node_id_from_addr(neighbour)
            if old_node not in nodes:
                nodes.add(old_node)

        return sorted(list(nodes))

    def __get_distance_to_node(self, nodes: tuple, node_to_search: int) -> int:
        right_distance = self.__counting_distance(node_to_search, nodes, step=1)
        left_distance = self.__counting_distance(node_to_search, nodes, step=-1)

        return min(right_distance, left_distance)

    def __counting_distance(self, node_to_search: int, all_nodes: tuple, step: int) -> int:
        my_index = all_nodes.index(self.node_id)
        nodes = all_nodes[my_index::step]
        distance = -1
        for node in nodes:
            distance += 1
            if node == node_to_search:
                return distance

        nodes = all_nodes[0 if step == 1 else -1: my_index: step]
        for node in nodes:
            distance += 1
            if node == node_to_search:
                return distance

        return distance
