import sys
import time
import socket
import psutil

from loguru import logger
from socket import AddressFamily

logger.remove()
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="DEBUG")


def exploded_part(string: str, index: int, delimiter: str = ".") -> int:
    """Разделяет строку string по разделителю delimiter и
    возвращает часть строки из списка splited по индексу index в виде целого числа
    """
    splited = string.split(delimiter)
    return int(splited[index])


def get_ip_addresses(family: AddressFamily = socket.AF_INET, network='10.20') -> str:
    """Получает список ip адресов текущей машины и возвращает строку адреса если она начинается с префикса network"""

    for interface, snics in psutil.net_if_addrs().items():
        for snic in snics:
            if snic.family == family and snic.address.startswith(network):
                return snic.address


def get_node_id_from_addr(addr: str = get_ip_addresses()) -> int:
    return exploded_part(string=addr, index=2)


def sleeping(switch: bool, seconds: int = 3) -> None:
    if switch:
        time.sleep(seconds)


def get_unix_time() -> int:
    return int(time.time())


def get_my_ip() -> str:
    my_ip = None
    print('Wait ip')
    while my_ip is None:
        my_ip = get_ip_addresses()
        time.sleep(1)

    print(my_ip)
    return my_ip


class NeighbourChecker:
    """
    Класс для работы с проверкой расположения соседей
    """

    @staticmethod
    def clear_neighbors(left_neighbour: str or bool = False, right_neighbour: str or bool = False) -> list:
        if left_neighbour and right_neighbour:
            neighbors = [left_neighbour, right_neighbour]
        else:
            if left_neighbour:
                neighbors = [left_neighbour]
            else:
                neighbors = [right_neighbour]

        return neighbors

    @classmethod
    def is_node_outermost(cls, node_id: int, nodes: tuple) -> bool:
        if cls.is_node_first(node_id, nodes) or cls.is_node_last(node_id, nodes):
            return True
        else:
            return False

    @staticmethod
    def is_node_first(node_id: int, nodes: tuple) -> bool:
        if nodes[0] == node_id:
            return True
        else:
            return False

    @staticmethod
    def is_node_last(node_id: int, nodes: tuple) -> bool:
        if nodes[-1] == node_id:
            return True
        else:
            return False
