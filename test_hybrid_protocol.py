import unittest
import threading
import time
from hybrid_protocol import HybridProtocol
from message_handling import MessageFragmenter

class TestHybridProtocol(unittest.TestCase):
    def setUp(self):
        # Initialize the protocol with localhost and arbitrary ports
        self.host = '127.0.0.1'
        self.tcp_port = 12345
        self.udp_port = 12346
        self.protocol = HybridProtocol(self.host, self.tcp_port, self.udp_port)
        self.mtu = 1400  # Set the MTU value for message fragmentation

    def tearDown(self):
        # Stop the protocol after each test
        self.protocol.stop()

    def test_server_start(self):
        # Test if the server starts successfully
        self.assertTrue(self.protocol.start_server())

    def test_client_connection(self):
        # Start the server in a separate thread
        server_thread = threading.Thread(target=self.protocol.start_server)
        server_thread.start()
        time.sleep(1)  # Give the server time to start

        # Create a client protocol instance
        client_protocol = HybridProtocol(self.host, self.tcp_port, self.udp_port)
        self.assertTrue(client_protocol.connect())
        client_protocol.stop()

    def test_tcp_communication(self):
        # Start the server
        self.protocol.start_server()

        # Register a simple echo handler
        def echo_handler(data, addr):
            self.protocol.send_tcp(data, client_addr=addr)

        self.protocol.register_handler('echo', echo_handler)

        # Create a client protocol instance
        client_protocol = HybridProtocol(self.host, self.tcp_port, self.udp_port)
        client_protocol.connect()

        # Send a message and check the response
        message = {'type': 'echo', 'data': 'Hello, TCP!'}
        client_protocol.send_tcp(message)

        # Add a delay to allow message processing
        time.sleep(1)

        # Stop the client
        client_protocol.stop()

    def test_udp_communication(self):
        # Start the server
        self.protocol.start_server()

        # Register a simple echo handler
        def echo_handler(data, addr):
            self.protocol.send_udp(data, addr=addr)

        self.protocol.register_handler('echo', echo_handler)

        # Create a client protocol instance
        client_protocol = HybridProtocol(self.host, self.tcp_port, self.udp_port)
        client_protocol.connect()

        # Send a message and check the response
        message = {'type': 'echo', 'data': 'Hello, UDP!'}
        client_protocol.send_udp(message, addr=(self.host, self.udp_port))

        # Add a delay to allow message processing
        time.sleep(1)

        # Stop the client
        client_protocol.stop()

    def test_compression(self):
        # Enable compression
        self.protocol.configure_compression(enable=True)
        self.assertTrue(self.protocol.compression_enabled)

        # Test sending a compressed message
        self.protocol.start_server()
        client_protocol = HybridProtocol(self.host, self.tcp_port, self.udp_port)
        client_protocol.connect()
        message = {'type': 'echo', 'data': 'Hello, Compressed!'}
        client_protocol.send_tcp(message, compress=True)
        time.sleep(1)
        client_protocol.stop()

    def test_encryption(self):
        # Enable encryption with a valid base64-encoded key
        valid_base64_key = 'dGVzdGtleQ=='  # 'testkey' in base64
        self.protocol.configure_encryption(enable=True, encryption_key=valid_base64_key)
        self.assertTrue(self.protocol.encryption_enabled)

        # Test sending an encrypted message
        self.protocol.start_server()
        client_protocol = HybridProtocol(self.host, self.tcp_port, self.udp_port)
        client_protocol.connect()
        message = {'type': 'echo', 'data': 'Hello, Encrypted!'}
        client_protocol.send_tcp(message, encrypt=True)
        time.sleep(1)
        client_protocol.stop()

    def test_connection_pooling(self):
        # Enable connection pooling
        self.protocol.configure_connection_pooling(enable=True)
        self.assertTrue(self.protocol.connection_pooling_enabled)

    def test_heartbeat(self):
        # Enable heartbeat
        self.protocol.configure_heartbeat(enable=True)
        self.assertTrue(self.protocol.heartbeat_enabled)

    def test_qos(self):
        # Enable QoS
        self.protocol.qos_enabled = True
        self.assertTrue(self.protocol.qos_enabled)

    def test_versioning(self):
        # Enable versioning
        self.protocol.versioning_enabled = True
        self.assertTrue(self.protocol.versioning_enabled)

    def test_transport_selection(self):
        # Enable transport selection
        self.protocol.configure_transport_selection(enable=True)
        self.assertTrue(self.protocol.transport_selection_enabled)

    def test_pubsub(self):
        # Enable PubSub
        self.protocol.pubsub_enabled = True
        self.assertTrue(self.protocol.pubsub_enabled)

    def test_streaming(self):
        # Enable streaming
        self.protocol.streaming_enabled = True
        self.assertTrue(self.protocol.streaming_enabled)

    def test_metrics(self):
        # Enable metrics
        self.protocol.metrics_enabled = True
        self.assertTrue(self.protocol.metrics_enabled)

    def test_message_fragmentation(self):
        # Enable message fragmentation
        self.protocol.message_handling_enabled = True
        self.protocol.message_fragmenter = MessageFragmenter(self.mtu)

        # Start the server
        self.protocol.start_server()

        # Register a simple echo handler
        def echo_handler(data, addr):
            self.protocol.send_tcp(data, client_addr=addr)

        self.protocol.register_handler('echo', echo_handler)

        # Create a client protocol instance
        client_protocol = HybridProtocol(self.host, self.tcp_port, self.udp_port)
        client_protocol.connect()

        # Send a large message that requires fragmentation
        large_message = {'type': 'echo', 'data': 'A' * (self.protocol.mtu + 100)}
        client_protocol.send_tcp(large_message)

        # Add a delay to allow message processing
        time.sleep(1)

        # Stop the client
        client_protocol.stop()

    def test_multiple_clients(self):
        # Start the server
        self.protocol.start_server()

        # Register a simple echo handler
        def echo_handler(data, addr):
            self.protocol.send_tcp(data, client_addr=addr)

        self.protocol.register_handler('echo', echo_handler)

        # Create multiple client protocol instances
        clients = [HybridProtocol(self.host, self.tcp_port, self.udp_port) for _ in range(5)]
        for client in clients:
            client.connect()

        # Send a message from each client
        for i, client in enumerate(clients):
            message = {'type': 'echo', 'data': f'Hello from client {i}!'}
            client.send_tcp(message)

        # Add a delay to allow message processing
        time.sleep(1)

        # Stop all clients
        for client in clients:
            client.stop()

    def test_encryption_integrity(self):
        # Enable encryption with a valid base64-encoded key
        valid_base64_key = 'dGVzdGtleQ=='  # 'testkey' in base64
        self.protocol.configure_encryption(enable=True, encryption_key=valid_base64_key)
        self.assertTrue(self.protocol.encryption_enabled)

        # Start the server
        self.protocol.start_server()

        # Register a simple echo handler
        def echo_handler(data, addr):
            self.protocol.send_tcp(data, client_addr=addr)

        self.protocol.register_handler('echo', echo_handler)

        # Create a client protocol instance
        client_protocol = HybridProtocol(self.host, self.tcp_port, self.udp_port)
        client_protocol.connect()

        # Send an encrypted message
        message = {'type': 'echo', 'data': 'Hello, Encrypted!'}
        client_protocol.send_tcp(message, encrypt=True)

        # Add a delay to allow message processing
        time.sleep(1)

        # Stop the client
        client_protocol.stop()

if __name__ == '__main__':
    unittest.main() 