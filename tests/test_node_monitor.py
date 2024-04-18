from src.node_monitor import NodeMonitor


class TestNodeMonitor:

    def test_set_closest_neighbors(self):
        assert NodeMonitor(node_number=1,
                           ping_port=80,
                           host_ip='10.20.1.1'
                           )._NodeMonitor__set_closest_neighbors() == ['10.20.255.1', '10.20.2.1']
