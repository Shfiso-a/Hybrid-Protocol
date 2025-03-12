#!/usr/bin/env python3
"""
Hybrid network protocol combining TCP and UDP with enhanced features.

This protocol provides a flexible communications layer with:
- Dual transport protocols (TCP for reliability, UDP for speed)
- Built-in message compression with multiple algorithms
- End-to-end encryption with AES-GCM
- SSL/TLS support for secure connections
- TCP connection pooling for efficient connection reuse
- Heartbeat monitoring for connection health tracking
- Reliable UDP messaging with acknowledgments and retries
- Message type handling system
- Message batching for reducing overhead with small messages
- Message fragmentation for handling large messages
- Metrics collection for monitoring performance and usage
- Quality of Service (QoS) for prioritization and flow control
- Publish-Subscribe messaging for topic-based communication
- Streaming data transfer for continuous data flow
- Transport selection for automated protocol decisions
- Protocol versioning for backward compatibility
"""

import socket
import threading
import json
import time
import logging
import uuid
from enum import Enum, auto
from typing import Dict, Any, Optional, Callable, Tuple, Union, List

# Import the compression and encryption functionality from separate modules
from compression import Compression, CompressionType
from encryption import EncryptionManager, SSLWrapper, EncryptionType
from connection_pool import ConnectionPool, Connection, ConnectionPoolError
from heartbeat import HeartbeatManager
from message_handling import MessageBatcher, MessageFragmenter
from metrics import MetricsManager
from qos import QoSManager, PriorityLevel, FlowControlStrategy
from pubsub import PubSubManager, SubscriptionType
from streaming import StreamManager, StreamDirection, BackpressureStrategy, StreamState
from transport_selection import TransportSelector, TransportSelectionStrategy
from protocol_version import ProtocolVersion

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HybridProtocol")

class TransportType(Enum):
    """Enum for the transport protocols available."""
    TCP = auto()
    UDP = auto()

class HybridProtocol:
    """
    HybridProtocol combines TCP and UDP to create a custom network protocol.
    - TCP is used for reliable, ordered, connection-oriented communication
    - UDP is used for fast, connectionless communication with lower overhead
    - Optional encryption for secure communication
    - Optional compression for efficient bandwidth usage
    - Connection pooling for TCP connections
    - Heartbeat monitoring for connection health
    - Message batching for reducing overhead with small messages
    - Message fragmentation for handling large messages
    - Metrics collection for monitoring performance and usage
    - Quality of Service (QoS) for prioritization and flow control
    - Publish-Subscribe messaging for topic-based communication
    - Streaming data transfer for continuous data flow
    - Transport selection for automated protocol decisions
    - Protocol versioning for backward compatibility
    """
    
    def __init__(self, host, tcp_port, udp_port):
        """
        Initialize the hybrid protocol.
        
        Args:
            host (str): Host to bind to
            tcp_port (int): TCP port to use
            udp_port (int): UDP port to use
        """
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        
        # Initialize sockets
        self.tcp_server_socket = None
        self.tcp_client_socket = None
        self.udp_socket = None
        
        # Operation mode flag
        self.is_server = False
        
        # Client connections
        self.tcp_clients = {}  # addr -> socket
        self.udp_clients = set()  # Set of (host, port) tuples
        
        # Server state
        self.server_running = False
        self.client_connected = False
        
        # Message handlers
        self.message_handlers = {}  # message_type -> handler_func
        
        # Default server address for client mode
        self.server_addr = None
        
        # Threads
        self.tcp_server_thread = None
        self.tcp_client_thread = None
        self.udp_thread = None
        
        # Reliable UDP tracking
        self.reliable_udp_messages = {}  # message_id -> (data, addr, attempts)
        self.reliable_udp_acks = set()  # Set of received message IDs
        
        # Compression
        self.compression_enabled = False
        self.compression = Compression()
        
        # Encryption
        self.encryption_enabled = False
        self.encryption_manager = EncryptionManager()
        self.ssl_wrapper = SSLWrapper()
        self.ssl_enabled = False
        
        # Connection pooling
        self.connection_pooling_enabled = False
        self.connection_pool = None
        
        # Heartbeat management
        self.heartbeat_enabled = False
        self.heartbeat_manager = None

        # Message handling
        self.message_handling_enabled = False
        self.message_batcher = None
        self.message_fragmenter = None
        
        # Metrics
        self.metrics_enabled = False
        self.metrics_manager = None
        
        # QoS
        self.qos_enabled = False
        self.qos_manager = None
        
        # PubSub
        self.pubsub_enabled = False
        self.pubsub_manager = None
        
        # Streaming
        self.streaming_enabled = False
        self.stream_manager = None
        
        # Transport selection
        self.transport_selection_enabled = False
        self.transport_selector = None
        
        # Protocol versioning
        self.versioning_enabled = False
        self.protocol_version = None
        
        # For reliable UDP
        self.udp_message_buffer = {}
        self.udp_retry_interval = 1.0  # seconds
        self.udp_max_retries = 5
        
        # Compression settings (configurable)
        self.compression_level = 6
        self.compression_threshold = 1024  # bytes
        self.default_compression_type = Compression.get_recommended_type()
        self.available_compression_types = Compression.get_available_types()
        
        # Encryption settings (disabled by default)
        # No longer needed since we use encryption_enabled
        
        # SSL/TLS settings (disabled by default)
        self.ssl_context = None
        self.ssl_certificate = None
        self.ssl_private_key = None
        self.ssl_ca_certificate = None
        
        # Connection pooling settings (disabled by default)
        self.connection_pool_size = 10
        self.connection_pool_timeout = 5.0
        self.connection_pool_max_idle = 300
        self.connection_pool_max_lifetime = 3600
        
        # Heartbeat settings (disabled by default)
        self.heartbeat_interval = 5.0
        self.heartbeat_timeout = 15.0
        
        # Start with default threads as None
        self.tcp_thread = None
        self.udp_thread = None
        
        # Initialize message handling components
        self.max_batch_size = 1024 * 1024  # 1MB default
        self.max_batch_delay = 0.1  # 100ms default
        self.min_batch_messages = 2
        self.mtu = 1400  # Default MTU size for fragmentation
        self.fragment_timeout = 30.0  # Default timeout for reassembling fragments
        
        # Initialize metrics manager
        self.metrics_dir = None
        self.metrics_csv_export = False
        self.metrics_collection_interval = 60.0
        self.metrics_retention_period = 86400
        
        # Initialize QoS manager
        self.qos_flow_control_strategy = FlowControlStrategy.RATE_LIMITING
        self.qos_flow_control_params = {
            'rate': 1000,         # Default to 1000 messages per second
            'capacity': 2000,     # Token bucket capacity
            'window_size': 100,   # Initial window size for window-based flow control
            'max_window_size': 500  # Maximum window size
        }
        
        # Initialize PubSub manager
        self.pubsub_enabled = False
    
    def start_server(self):
        """
        Start the protocol server.
        
        Returns:
            bool: True if the server was started, False otherwise
        """
        if self.server_running:
            logger.warning("Server is already running")
            return False
            
        try:
            # Create TCP socket
            self.tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tcp_server_socket.bind((self.host, self.tcp_port))
            self.tcp_server_socket.listen(5)
            
            # Create UDP socket
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.bind((self.host, self.udp_port))
            
            # Set operating mode
            self.is_server = True
            
            # Initialize Stream manager if enabled
            if self.streaming_enabled and self.stream_manager:
                self.stream_manager.set_send_callback(self._stream_send_callback)
                self.stream_manager.set_data_callback(self._stream_data_callback)
                
            # Initialize Transport selector if enabled
            if self.transport_selection_enabled and self.transport_selector:
                self.transport_selector.set_callbacks(
                    tcp_available=self._is_tcp_available,
                    udp_available=self._is_udp_available
                )
                
            self.server_running = True
            
            # Start listener threads
            self.tcp_server_thread = threading.Thread(target=self._tcp_server_listener)
            self.udp_thread = threading.Thread(target=self._udp_listener)
            self.tcp_server_thread.daemon = True
            self.udp_thread.daemon = True
            self.tcp_server_thread.start()
            self.udp_thread.start()

            # Start connection pool if enabled
            if self.connection_pooling_enabled and self.connection_pool is None:
                self.connection_pool = ConnectionPool(
                    max_connections=self.connection_pool_size,
                    connection_timeout=self.connection_pool_timeout,
                    max_idle_time=self.connection_pool_max_idle,
                    max_lifetime=self.connection_pool_max_lifetime
                )
                self.connection_pool.start()
                
            # Initialize the heartbeat manager if enabled
            if self.heartbeat_enabled and self.heartbeat_manager:
                self.heartbeat_manager.start()
                self.heartbeat_manager.set_callbacks(
                    send_heartbeat=self._send_heartbeat_callback,
                    connection_lost=self._connection_lost_callback,
                    connection_restored=self._connection_restored_callback
                )
                
            # Initialize the metrics manager if enabled
            if self.metrics_enabled and self.metrics_manager:
                self.metrics_manager.start()
            
            # Initialize QoS manager if enabled
            if self.qos_enabled and self.qos_manager:
                self.qos_manager.set_send_callback(self._qos_send_callback)
                logger.info("QoS manager started")
            
            # Initialize PubSub manager if enabled
            if self.pubsub_enabled and self.pubsub_manager:
                self.pubsub_manager.set_send_callback(self._pubsub_send_callback)
                logger.info("PubSub manager started")
            
            logger.info(f"Server started on {self.host}:{self.tcp_port} (TCP) and {self.host}:{self.udp_port} (UDP)")
            return True
            
        except Exception as e:
            logger.error(f"Server start error: {e}")
            self.stop()  # Clean up on error
            return False
    
    def connect(self):
        """
        Connect to a server.
        
        Returns:
            bool: True if connected, False otherwise
        """
        if self.client_connected:
            logger.warning("Already connected to a server")
            return False
            
        try:
            # Create sockets
            self.tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            
            # Set operating mode
            self.is_server = False
            
            # Wrap TCP socket with SSL if enabled
            if self.ssl_enabled and self.ssl_context:
                self.tcp_client_socket = SSLWrapper.wrap_socket(
                    self.tcp_client_socket,
                    self.ssl_context,
                    server_side=False,
                    verify_peer=True,
                    ca_file=self.ssl_ca_certificate
                )
            
            # Connect TCP socket
            self.tcp_client_socket.connect((self.host, self.tcp_port))
            
            # UDP doesn't have a connection, but we'll bind to a random port
            self.udp_socket.bind(('0.0.0.0', 0))
            
            # Save server address for future use
            self.server_addr = (self.host, self.tcp_port)
            
            # Initialize Stream manager if enabled
            if self.streaming_enabled and self.stream_manager:
                self.stream_manager.set_send_callback(self._stream_send_callback)
                self.stream_manager.set_data_callback(self._stream_data_callback)
                
            # Initialize Transport selector if enabled
            if self.transport_selection_enabled and self.transport_selector:
                self.transport_selector.set_callbacks(
                    tcp_available=self._is_tcp_available,
                    udp_available=self._is_udp_available
                )
                
            # Start listener threads
            self.tcp_client_thread = threading.Thread(target=self._tcp_client_listener)
            self.udp_thread = threading.Thread(target=self._udp_listener)
            self.tcp_client_thread.daemon = True
            self.udp_thread.daemon = True
            self.tcp_client_thread.start()
            self.udp_thread.start()
            
            # Start connection pool if enabled
            if self.connection_pooling_enabled and self.connection_pool is None:
                self.connection_pool = ConnectionPool(
                    max_connections=self.connection_pool_size,
                    connection_timeout=self.connection_pool_timeout,
                    max_idle_time=self.connection_pool_max_idle,
                    max_lifetime=self.connection_pool_max_lifetime
                )
                self.connection_pool.start()
                
            # Initialize the heartbeat manager if enabled
            if self.heartbeat_enabled and self.heartbeat_manager:
                self.heartbeat_manager.start()
                self.heartbeat_manager.set_callbacks(
                    send_heartbeat=self._send_heartbeat_callback,
                    connection_lost=self._connection_lost_callback,
                    connection_restored=self._connection_restored_callback
                )
                # Register the server for heartbeat monitoring
                self.heartbeat_manager.register_connection(self.server_addr)
                
            # Initialize the metrics manager if enabled
            if self.metrics_enabled and self.metrics_manager:
                self.metrics_manager.start()
                
            # Initialize QoS manager if enabled
            if self.qos_enabled and self.qos_manager:
                self.qos_manager.set_send_callback(self._qos_send_callback)
                
            # Initialize PubSub manager if enabled
            if self.pubsub_enabled and self.pubsub_manager:
                self.pubsub_manager.set_send_callback(self._pubsub_send_callback)
                
            # Send version negotiation message if enabled
            if self.versioning_enabled and self.protocol_version:
                version_message = self.protocol_version.generate_version_message()
                self.send_tcp(version_message, self.server_addr)
            
            self.client_connected = True
            
            logger.info(f"Connected to server at {self.server_addr}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to server: {e}")
            
            # Clean up on error
            if self.tcp_client_socket:
                self.tcp_client_socket.close()
                self.tcp_client_socket = None
                
            if self.udp_socket:
                self.udp_socket.close()
                self.udp_socket = None
                
            return False
    
    def stop(self):
        """
        Stop the protocol server or client.
        
        Returns:
            bool: True if stopped, False otherwise
        """
        # Only do something if we're running
        if not self.server_running and not self.client_connected:
            logger.warning("Not running")
            return False
            
        try:
            # Set flags to signal threads to exit
            self.server_running = False
            self.client_connected = False
            
            # Wait for threads to finish
            if self.is_server:
                # Server mode clean up
                if hasattr(self, 'tcp_server_thread') and self.tcp_server_thread and self.tcp_server_thread.is_alive():
                    logger.debug("Waiting for TCP server thread to stop...")
                    self.tcp_server_thread.join(timeout=2.0)
                    
                # Close all client connections
                for client_addr, client_socket in list(self.tcp_clients.items()):
                    try:
                        client_socket.close()
                    except Exception as e:
                        logger.debug(f"Error closing client socket {client_addr}: {e}")
                        
                self.tcp_clients.clear()
                
                # Close server socket
                if self.tcp_server_socket:
                    try:
                        self.tcp_server_socket.close()
                    except Exception as e:
                        logger.debug(f"Error closing TCP server socket: {e}")
                    self.tcp_server_socket = None
            else:
                # Client mode clean up
                if hasattr(self, 'tcp_client_thread') and self.tcp_client_thread and self.tcp_client_thread.is_alive():
                    logger.debug("Waiting for TCP client thread to stop...")
                    self.tcp_client_thread.join(timeout=2.0)
                    
                # Close client socket
                if self.tcp_client_socket:
                    try:
                        self.tcp_client_socket.close()
                    except Exception as e:
                        logger.debug(f"Error closing TCP client socket: {e}")
                    self.tcp_client_socket = None
                    
            # Close UDP socket
            if hasattr(self, 'udp_thread') and self.udp_thread and self.udp_thread.is_alive():
                logger.debug("Waiting for UDP thread to stop...")
                self.udp_thread.join(timeout=2.0)
                
            if self.udp_socket:
                try:
                    self.udp_socket.close()
                except Exception as e:
                    logger.debug(f"Error closing UDP socket: {e}")
                self.udp_socket = None
                
            # Stop heartbeat manager
            if self.heartbeat_enabled and self.heartbeat_manager:
                try:
                    self.heartbeat_manager.stop()
                except Exception as e:
                    logger.error(f"Error stopping heartbeat manager: {e}")
                    
            # Close connection pool
            if self.connection_pooling_enabled and self.connection_pool is not None:
                try:
                    self.connection_pool.stop()
                except Exception as e:
                    logger.error(f"Error stopping connection pool: {e}")
                self.connection_pool = None
                    
            # Stop metrics manager
            if self.metrics_enabled and self.metrics_manager:
                try:
                    self.metrics_manager.stop()
                except Exception as e:
                    logger.error(f"Error stopping metrics manager: {e}")
                    
            # Stop QoS manager
            if self.qos_enabled and self.qos_manager:
                try:
                    self.qos_manager.stop()
                except Exception as e:
                    logger.error(f"Error stopping QoS manager: {e}")
                    
            # Stop PubSub manager
            if self.pubsub_enabled and self.pubsub_manager:
                try:
                    self.pubsub_manager.stop()
                except Exception as e:
                    logger.error(f"Error stopping PubSub manager: {e}")
                    
            # Stop Stream manager
            if self.streaming_enabled and self.stream_manager:
                try:
                    self.stream_manager.stop()
                except Exception as e:
                    logger.error(f"Error stopping Stream manager: {e}")
                    
            logger.info("Protocol stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            return False
    
    def register_handler(self, message_type, handler_func):
        """
        Register a function to handle a specific message type.
        
        Args:
            message_type (str): The type of message to handle
            handler_func (callable): Function that takes (data, address) as parameters
        """
        self.message_handlers[message_type] = handler_func
        
    def configure_compression(self, enable=True, compression_type=None, 
                             compression_level=6, threshold=1024):
        """
        Configure the compression options.
        
        Args:
            enable (bool): Whether to enable compression
            compression_type (str): Compression algorithm to use (from CompressionType)
            compression_level (int): Compression level (1-9, with 9 being highest)
            threshold (int): Minimum size in bytes before compression is applied
            
        Returns:
            bool: True if configuration was successful
        """
        # Store the old state
        old_enabled = self.compression_enabled
        
        # Update the configuration
        self.compression_enabled = enable
        
        if compression_type:
            self.default_compression_type = compression_type
        
        self.compression_level = compression_level
        self.compression_threshold = threshold
        
        # Log the change
        if old_enabled != enable:
            if enable:
                logger.info(f"Compression enabled with {self.default_compression_type}")
            else:
                logger.info("Compression disabled")
            
        return True
    
    def get_compression_stats(self):
        """
        Get current compression configuration and statistics.
        
        Returns:
            dict: Dictionary with compression settings and statistics
        """
        config = {
            'enabled': self.compression_enabled,
            'default_type': self.default_compression_type,
            'level': self.compression_level,
            'threshold': self.compression_threshold,
            'available_types': self.available_compression_types
        }
        
        # Add statistics if we have a compression instance
        if hasattr(self, 'compression') and hasattr(self.compression, 'get_stats'):
            stats = self.compression.get_stats()
            config.update(stats)
        
        return config
    
    def configure_encryption(self, enable=True, encryption_key=None, 
                            encryption_type=EncryptionType.AES_GCM):
        """
        Configure encryption settings.
        
        Args:
            enable (bool): Whether to enable encryption
            encryption_key (str, optional): Base64-encoded encryption key
            encryption_type (EncryptionType): Encryption algorithm to use
            
        Returns:
            bool: True if configuration was successful
        """
        # Store the old state
        old_enabled = self.encryption_enabled
        
        # Update configuration
        self.encryption_enabled = enable
        
        # Create a new encryption manager with the provided key if one was specified
        if enable:
            self.encryption_manager = EncryptionManager(
                encryption_key=encryption_key,
                encryption_type=encryption_type
            )
            
        # Log the change
        if old_enabled != enable:
            if enable:
                logger.info(f"Encryption enabled with {encryption_type}")
            else:
                logger.info("Encryption disabled")
            
        return True
    
    def configure_ssl(self, enable=True, cert_file=None, key_file=None, 
                     ca_file=None, verify_peer=True):
        """
        Configure SSL/TLS settings.
        
        Args:
            enable (bool): Whether to enable SSL
            cert_file (str, optional): Path to certificate file
            key_file (str, optional): Path to private key file
            ca_file (str, optional): Path to CA certificate file
            verify_peer (bool): Whether to verify peer certificates
            
        Returns:
            bool: True if configuration was successful
        """
        # Store old state
        old_enabled = self.ssl_enabled
        
        # Update configuration
        self.ssl_enabled = enable
        
        if enable:
            try:
                # Create SSL context
                self.ssl_context = SSLWrapper.create_ssl_context(
                    cert_file=cert_file,
                    key_file=key_file,
                    ca_file=ca_file,
                    verify_peer=verify_peer
                )
                
                # Store the certificate paths
                self.ssl_certificate = cert_file
                self.ssl_private_key = key_file
                self.ssl_ca_certificate = ca_file
                
                logger.info("SSL configuration completed successfully")
                
            except Exception as e:
                logger.error(f"SSL configuration error: {e}")
                self.ssl_enabled = False
                return False
            
        # Log the change
        if old_enabled != enable:
            if enable:
                logger.info("SSL/TLS enabled")
            else:
                logger.info("SSL/TLS disabled")
            
        return True
    
    def get_encryption_stats(self):
        """
        Get current encryption configuration and statistics.
        
        Returns:
            dict: Dictionary with encryption settings and statistics
        """
        if not self.encryption_enabled or not self.encryption_manager:
            return {'enabled': False}
            
        # Get stats from encryption manager
        stats = self.encryption_manager.get_stats()
        stats['enabled'] = self.encryption_enabled
        
        return stats
    
    def configure_connection_pooling(self, enable=True, max_connections=10, 
                                   connection_timeout=5.0, max_idle_time=300, 
                                   max_lifetime=3600):
        """
        Configure connection pooling settings.
        
        Args:
            enable (bool): Whether to enable connection pooling
            max_connections (int): Maximum number of connections in the pool
            connection_timeout (float): Timeout in seconds for creating new connections
            max_idle_time (float): Maximum time in seconds a connection can be idle
            max_lifetime (float): Maximum lifetime in seconds for a connection
            
        Returns:
            bool: True if configuration was successful
        """
        # Store old state
        old_enabled = self.connection_pooling_enabled
        
        # Update configuration
        self.connection_pooling_enabled = enable
        self.connection_pool_size = max_connections
        self.connection_pool_timeout = connection_timeout
        self.connection_pool_max_idle = max_idle_time
        self.connection_pool_max_lifetime = max_lifetime
        
        # Create or reconfigure the connection pool if enabled
        if enable:
            if self.connection_pool:
                # If the pool exists, update its configuration
                self.connection_pool.max_connections = max_connections
                self.connection_pool.connection_timeout = connection_timeout
                self.connection_pool.max_idle_time = max_idle_time
                self.connection_pool.max_lifetime = max_lifetime
            elif self.server_running or self.client_connected:
                # Only create the pool if the protocol is already running
                self.connection_pool = ConnectionPool(
                    max_connections=max_connections,
                    connection_timeout=connection_timeout,
                    max_idle_time=max_idle_time,
                    max_lifetime=max_lifetime
                )
                self.connection_pool.start()
            
        # Stop the pool if being disabled
        elif old_enabled and not enable and self.connection_pool:
            self.connection_pool.stop()
            self.connection_pool = None
        
        # Log the change
        if old_enabled != enable:
            if enable:
                logger.info(f"Connection pooling enabled (max: {max_connections})")
            else:
                logger.info("Connection pooling disabled")
            
        return True

    def get_connection_pool_stats(self):
        """
        Get statistics about the connection pool.
        
        Returns:
            dict: Connection pool statistics or None if pooling is disabled
        """
        if not self.connection_pooling_enabled or self.connection_pool is None:
            return {'enabled': False}
            
        # Get stats from pool
        stats = self.connection_pool.get_pool_stats()
        stats['enabled'] = True
        stats['max_connections'] = self.connection_pool_size
        stats['connection_timeout'] = self.connection_pool_timeout
        stats['max_idle_time'] = self.connection_pool_max_idle
        stats['max_lifetime'] = self.connection_pool_max_lifetime
        
        return stats
    
    def configure_heartbeat(self, enable=True, heartbeat_interval=5.0, 
                           connection_timeout=15.0):
        """
        Configure heartbeat monitoring for connections.
        
        Heartbeats are used to monitor connection health and detect disconnections.
        They calculate connection health scores and round-trip times (RTT).
        
        Args:
            enable (bool): Whether to enable heartbeat monitoring
            heartbeat_interval (float): Interval between heartbeats in seconds
            connection_timeout (float): Time after which a connection is considered dead
            
        Returns:
            bool: True if heartbeat was configured successfully
        """
        self.heartbeat_enabled = enable
        
        if enable:
            # Create or reconfigure heartbeat manager
            if self.heartbeat_manager is None:
                self.heartbeat_manager = HeartbeatManager(
                    heartbeat_interval=heartbeat_interval,
                    connection_timeout=connection_timeout
                )
                
                # Set up callbacks
                self.heartbeat_manager.set_callbacks(
                    send_heartbeat=self._send_heartbeat_callback,
                    connection_lost=self._connection_lost_callback,
                    connection_restored=self._connection_restored_callback
                )
                
                self.heartbeat_manager.start()
            else:
                # Stop and restart with new settings
                self.heartbeat_manager.stop()
                self.heartbeat_manager = HeartbeatManager(
                    heartbeat_interval=heartbeat_interval,
                    connection_timeout=connection_timeout
                )
                
                # Set up callbacks
                self.heartbeat_manager.set_callbacks(
                    send_heartbeat=self._send_heartbeat_callback,
                    connection_lost=self._connection_lost_callback,
                    connection_restored=self._connection_restored_callback
                )
                
                self.heartbeat_manager.start()
                
            # Store settings
            self.heartbeat_interval = heartbeat_interval
            self.heartbeat_timeout = connection_timeout
            
            logger.info(f"Heartbeat monitoring enabled with interval {heartbeat_interval}s")
            return True
        else:
            # Disable heartbeat monitoring
            if self.heartbeat_manager is not None:
                self.heartbeat_manager.stop()
                self.heartbeat_manager = None
                
            logger.info("Heartbeat monitoring disabled")
            return True

    def get_heartbeat_status(self):
        """
        Get the status of all monitored connections.
        
        Returns:
            dict: Connection status information or None if heartbeat is disabled
        """
        if not self.heartbeat_enabled or self.heartbeat_manager is None:
            return {'enabled': False}
            
        # Get status from heartbeat manager
        status = {
            'enabled': True,
            'interval': self.heartbeat_interval,
            'timeout': self.heartbeat_timeout,
            'connections': {}
        }
        
        # Get connection status
        conn_status = self.heartbeat_manager.get_connection_status()
        for addr, (is_alive, health, rtt) in conn_status.items():
            addr_str = f"{addr[0]}:{addr[1]}"
            status['connections'][addr_str] = {
                'alive': is_alive,
                'health': health,
                'rtt': rtt
            }
            
        return status
    
    def _send_heartbeat_callback(self, heartbeat_data, addr):
        """Callback to send heartbeat messages via UDP."""
        try:
            # Always send heartbeats over UDP for efficiency
            self.send_udp(heartbeat_data, addr=addr, compress=False)
        except Exception as e:
            logger.error(f"Error sending heartbeat to {addr}: {e}")
            
    def _connection_lost_callback(self, addr):
        """Callback when a connection is lost."""
        logger.warning(f"Connection lost: {addr}")
        
        # If this is a TCP client, remove it
        if self.is_server and addr in self.tcp_clients:
            try:
                self.tcp_clients[addr].close()
            except:
                pass
            del self.tcp_clients[addr]
            
    def _connection_restored_callback(self, addr):
        """Callback when a connection is restored."""
        logger.info(f"Connection restored: {addr}")
        
        # Connection was restored, nothing specific to do here
        # The client will need to re-establish a TCP connection if needed
    
    def send_tcp(self, data, client_addr=None, compress=None, encrypt=None, 
                reliable=None, _bypass_message_handling=False, _bypass_qos=False, 
                priority=PriorityLevel.NORMAL):
        """
        Send data via TCP.
        
        Args:
            data: Data to send
            client_addr: Client address to send to (only required for server mode)
            compress: Whether to compress the data (overrides global setting)
            encrypt: Whether to encrypt the data (overrides global setting)
            reliable: TCP is always reliable, this is for API consistency with send_udp
            _bypass_message_handling: Whether to bypass message handling
            _bypass_qos: Whether to bypass QoS
            priority: Message priority level
            
        Returns:
            bool: True if the data was sent
        """
        # Verify we're connected or running a server
        if not self.server_running and not self.client_connected:
            logger.error("Cannot send TCP data - not connected to a server and not running as a server")
            return False
        
        # In server mode, we need a client address
        if self.is_server and client_addr is None:
            logger.error("Cannot send TCP data in server mode without a client address")
            return False
        
        # In client mode, use the server address
        if not self.is_server and client_addr is None:
            client_addr = self.server_addr
        
        # Initialize transport_type for metrics
        transport_type = TransportType.TCP
        
        # Make sure the data is a dictionary
        if not isinstance(data, dict):
            data = {'type': 'raw', 'data': data}
            
        # Check if we're using a feature that the peer doesn't support
        if self.versioning_enabled and self.protocol_version and client_addr:
            # Add feature requirements for specific message types
            if compress and not '_requires_feature' in data:
                data['_requires_feature'] = 'compression'
            elif encrypt and not '_requires_feature' in data:
                data['_requires_feature'] = 'encryption'
                
            # Check compatibility
            if not self.is_message_compatible(data, client_addr):
                logger.warning(f"Message not compatible with peer {client_addr}, skipping send")
                return False
        
        # Record the start time for metrics
        start_time = time.time() if self.metrics_enabled else None
        
        # Convert to JSON and encode
        try:
            json_data = json.dumps(data).encode('utf-8')
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to encode message to JSON: {e}")
            return False
        
        # Apply message handling if enabled and not bypassed
        if not _bypass_message_handling and self.message_handling_enabled:
            if self.message_batcher and len(json_data) <= self.max_batch_size:
                self.message_batcher.add_message(json_data, client_addr)
                return True
            
            elif self.message_fragmenter and len(json_data) > self.mtu:
                return self.message_fragmenter.send_fragmented(json_data, client_addr, 
                                                             transport_type=TransportType.TCP)
        
        # Apply QoS if enabled and not bypassed
        if not _bypass_qos and self.qos_enabled and self.qos_manager:
            if not self.qos_manager.process_outgoing(json_data, client_addr, priority):
                logger.debug(f"Message rate-limited by QoS: {len(json_data)} bytes to {client_addr}")
                return False
        
        # Apply compression if enabled and size is above threshold
        if (compress is True) or (compress is None and self.compression_enabled and 
                 len(json_data) > self.compression_threshold):
            try:
                json_data, comp_type = self.compression.compress_data(
                    json_data, 
                    self.default_compression_type, 
                    self.compression_level
                )
                compressed = True
            except Exception as e:
                logger.error(f"Compression error: {e}")
                compressed = False
        else:
            compressed = False
        
        # Apply encryption if enabled
        if (encrypt is True) or (encrypt is None and self.encryption_enabled and 
                 self.encryption_manager):
            try:
                json_data = self.encryption_manager.encrypt(json_data)
                encrypted = True
            except Exception as e:
                logger.error(f"Encryption error: {e}")
                return False
        else:
            encrypted = False
        
        # Add metadata for the receiver
        metadata = {
            'compressed': compressed,
            'encrypted': encrypted,
            'timestamp': time.time(),
            'format': 'json',
            'type': data.get('type', 'generic')
        }
        
        # Combine metadata and data
        try:
            # Convert header to bytes
            header = json.dumps(metadata).encode('utf-8')
            
            # Calculate sizes
            header_size = len(header)
            
            # Limit header size to a reasonable value
            if header_size > 8192:  # 8KB max header size
                logger.warning(f"Header too large ({header_size} bytes), truncating metadata")
                metadata = {
                    'compressed': compressed,
                    'encrypted': encrypted,
                    'type': data.get('type', 'generic')
                }
                header = json.dumps(metadata).encode('utf-8')
                header_size = len(header)
                
            # Header size as 32-bit integer (4 bytes)
            header_size_bytes = header_size.to_bytes(4, byteorder='big')
            
            # Assemble the packet
            message_size = len(header_size_bytes) + header_size + len(json_data)
            packet = header_size_bytes + header + json_data
            
            logger.debug(f"Packet created: {len(header_size_bytes)} bytes header size + {header_size} bytes header + {len(json_data)} bytes data")
        except Exception as e:
            logger.error(f"Failed to create packet: {e}")
            return False
            
        # Send the packet
        try:
            if self.is_server:
                # Server mode - send to specific client
                if client_addr not in self.tcp_clients:
                    logger.error(f"Cannot send to {client_addr} - not connected")
                    return False
                    
                client_socket = self.tcp_clients[client_addr]
                client_socket.sendall(packet)
                
                # Update metrics
                if self.metrics_enabled and self.metrics_manager:
                    self.metrics_manager.record_send(
                        len(packet), 
                        transport_type, 
                        compressed, 
                        encrypted,
                        time.time() - start_time if start_time else 0
                    )
                    
                logger.debug(f"Sent {len(packet)} bytes to {client_addr} via TCP")
                return True
                
            else:
                # Client mode - send to server
                if not self.tcp_client_socket:
                    logger.error("Cannot send - not connected to server")
                    return False
                    
                self.tcp_client_socket.sendall(packet)
                
                # Update metrics
                if self.metrics_enabled and self.metrics_manager:
                    self.metrics_manager.record_send(
                        len(packet), 
                        transport_type, 
                        compressed, 
                        encrypted,
                        time.time() - start_time if start_time else 0
                    )
                    
                logger.debug(f"Sent {len(packet)} bytes to server via TCP")
                return True
                
        except Exception as e:
            logger.error(f"TCP send error: {e}")
            
            # If connection lost, clean up
            if self.is_server and client_addr in self.tcp_clients:
                logger.warning(f"Removing disconnected client {client_addr}")
                del self.tcp_clients[client_addr]
                
            return False

    def _tcp_server_listener(self):
        """
        Thread function to listen for incoming TCP connections.
        
        This method runs in a separate thread and accepts incoming TCP connections,
        creating a new thread to handle communication with each client.
        """
        logger.info(f"TCP server listener started on {self.host}:{self.tcp_port}")
        
        while self.server_running:
            try:
                # Accept new connections
                client_socket, client_addr = self.tcp_server_socket.accept()
                logger.info(f"New TCP connection from {client_addr}")
                
                # Wrap with SSL if enabled
                if self.ssl_enabled and self.ssl_context:
                    try:
                        client_socket = SSLWrapper.wrap_socket(
                            client_socket, 
                            self.ssl_context,
                            server_side=True,
                            verify_peer=True,
                            ca_file=self.ssl_ca_certificate
                        )
                        logger.debug(f"SSL handshake completed with {client_addr}")
                    except Exception as e:
                        logger.error(f"SSL handshake failed with {client_addr}: {e}")
                        client_socket.close()
                        continue
                
                # Store the connection
                self.tcp_clients[client_addr] = client_socket
                
                # Register with the heartbeat manager if enabled
                if self.heartbeat_enabled and self.heartbeat_manager:
                    self.heartbeat_manager.register_connection(client_addr)
                
                # Start a thread to handle this client
                client_handler = threading.Thread(
                    target=self._handle_tcp_client,
                    args=(client_socket, client_addr)
                )
                client_handler.daemon = True
                client_handler.start()
                
            except Exception as e:
                if self.server_running:  # Only log if still supposed to be running
                    logger.error(f"TCP server listener error: {e}")
                break
        
        logger.info("TCP server listener stopped")

    def _handle_tcp_client(self, client_socket, client_addr):
        """
        Handle communication with a TCP client.
        
        Args:
            client_socket (socket): The client socket
            client_addr (tuple): Client address (host, port)
        """
        logger.debug(f"Started TCP client handler for {client_addr}")
        
        while self.server_running and client_addr in self.tcp_clients:
            try:
                # Receive data with timeout
                client_socket.settimeout(1.0)  # 1 second timeout to allow checking server_running
                
                # Read message size (4 bytes)
                header = client_socket.recv(4)
                if not header:  # Connection closed
                    break
                    
                # Get the message size
                message_size = int.from_bytes(header, byteorder='big')
                
                # Read the message
                data = b''
                remaining = message_size
                while remaining > 0:
                    chunk = client_socket.recv(min(4096, remaining))
                    if not chunk:  # Connection closed
                        break
                    data += chunk
                    remaining -= len(chunk)
                    
                if len(data) < message_size:  # Incomplete message
                    logger.warning(f"Incomplete message from {client_addr}: got {len(data)}/{message_size} bytes")
                    continue
                    
                # Process the message
                self._process_tcp_message(data, client_addr)
                
            except socket.timeout:
                # This is just to allow checking server_running periodically
                continue
            except ConnectionError as e:
                logger.warning(f"TCP connection lost to {client_addr}: {e}")
                break
            except Exception as e:
                logger.error(f"Error handling TCP client {client_addr}: {e}")
                break
        
        # Clean up
        try:
            client_socket.close()
        except:
            pass
        
        # Remove from clients dictionary
        if client_addr in self.tcp_clients:
            del self.tcp_clients[client_addr]
        
        logger.debug(f"TCP client handler for {client_addr} stopped")

    def _process_tcp_message(self, data, client_addr):
        """
        Process a received TCP message.
        
        Args:
            data (bytes): The received data
            client_addr (tuple): Client address (host, port)
        """
        try:
            # Make sure we have at least 4 bytes for the header size
            if len(data) < 4:
                logger.error(f"Received TCP message too small (only {len(data)} bytes)")
                return
            
            # Extract header size
            header_size = int.from_bytes(data[:4], byteorder='big')
            
            # Check if we have enough data for the complete header
            if len(data) < 4 + header_size:
                logger.error(f"TCP message header incomplete: expected {header_size} bytes, got {len(data) - 4}")
                return
            
            # Extract and parse header
            header_end = 4 + header_size
            header_data = data[4:header_end]
            
            # Debug the header data if it's causing issues
            try:
                header_text = header_data.decode('utf-8')
                logger.debug(f"Header data: {header_text}")
                metadata = json.loads(header_text)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse header JSON: {e}")
                logger.debug(f"Raw header: {header_data}")
                return
            except UnicodeDecodeError as e:
                logger.error(f"Failed to decode header as UTF-8: {e}")
                return
            
            # Make sure there is message data after the header
            if len(data) <= header_end:
                logger.error("No message data after header")
                return
            
            message_data = data[header_end:]
            
            # Apply decompression if needed
            if metadata.get('compressed', False):
                try:
                    message_data = self.compression.decompress_data(
                        message_data,
                        metadata.get('compression_type', 'zlib')
                    )
                except Exception as e:
                    logger.error(f"Decompression error: {e}")
                    return
                
            # Apply decryption if needed
            if metadata.get('encrypted', False) and self.encryption_enabled and self.encryption_manager:
                try:
                    message_data = self.encryption_manager.decrypt(message_data)
                except Exception as e:
                    logger.error(f"Decryption error: {e}")
                    return
                
            # Parse the message data if it's JSON
            if metadata.get('format', 'json') == 'json':
                try:
                    # Debug the message data if it's causing issues
                    message_text = message_data.decode('utf-8')
                    logger.debug(f"Message data: {message_text}")
                    message = json.loads(message_text)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message JSON: {e}")
                    logger.debug(f"Raw message: {message_data}")
                    return
                except UnicodeDecodeError as e:
                    logger.error(f"Failed to decode message as UTF-8: {e}")
                    return
            else:
                message = message_data
                
            # Get the message type
            message_type = metadata.get('type')
            if isinstance(message, dict) and 'type' in message:
                message_type = message['type']
                
            # Call the appropriate handler if registered
            if message_type and message_type in self.message_handlers:
                try:
                    self.message_handlers[message_type](message, client_addr)
                except Exception as e:
                    logger.error(f"Error in message handler for {message_type}: {e}")
            else:
                logger.warning(f"Received message with unknown type: {message_type}")
                
        except Exception as e:
            logger.error(f"Error processing TCP message: {e}")
            logger.debug(f"Raw message data: {data[:100]}{'...' if len(data) > 100 else ''}")

    def _tcp_client_listener(self):
        """
        Thread function to listen for incoming data from the server (client mode).
        
        This method runs in a separate thread and handles incoming data from the server
        when operating in client mode.
        """
        logger.debug("TCP client listener started")
        
        while self.client_connected:
            try:
                # Receive data with timeout
                self.tcp_client_socket.settimeout(1.0)  # 1 second timeout to allow checking client_connected
                
                # Read message size (4 bytes)
                header = self.tcp_client_socket.recv(4)
                if not header:  # Connection closed
                    logger.warning("Server closed the connection")
                    self.client_connected = False
                    break
                    
                # Get the message size
                message_size = int.from_bytes(header, byteorder='big')
                
                # Read the message
                data = b''
                remaining = message_size
                while remaining > 0:
                    chunk = self.tcp_client_socket.recv(min(4096, remaining))
                    if not chunk:  # Connection closed
                        logger.warning("Server closed the connection during message transfer")
                        self.client_connected = False
                        break
                    data += chunk
                    remaining -= len(chunk)
                    
                if not self.client_connected:  # Connection was closed
                    break
                    
                if len(data) < message_size:  # Incomplete message
                    logger.warning(f"Incomplete message from server: got {len(data)}/{message_size} bytes")
                    continue
                    
                # Process the message
                self._process_tcp_message(data, self.server_addr)
                
            except socket.timeout:
                # This is just to allow checking client_connected periodically
                continue
            except ConnectionError as e:
                logger.warning(f"TCP connection lost to server: {e}")
                self.client_connected = False
                break
            except Exception as e:
                logger.error(f"Error in TCP client listener: {e}")
                if self.client_connected:  # Only try to recover if still supposed to be connected
                    time.sleep(1)  # Avoid tight loop on persistent error
                
        logger.debug("TCP client listener stopped")

    def _udp_listener(self):
        """
        Thread function to listen for incoming UDP messages.
        
        This method runs in a separate thread and handles incoming UDP datagrams
        for both server and client modes.
        """
        logger.debug(f"UDP listener started on port {self.udp_port if self.is_server else 'dynamic'}")
        
        while (self.server_running and self.is_server) or (self.client_connected and not self.is_server):
            try:
                # Receive data with timeout
                self.udp_socket.settimeout(1.0)  # 1 second timeout to allow checking server_running/client_connected
                data, addr = self.udp_socket.recvfrom(65535)  # Max UDP packet size
                
                if not data:
                    continue
                    
                # Process the UDP message
                self._process_udp_message(data, addr)
                
            except socket.timeout:
                # This is just to allow checking server_running/client_connected periodically
                continue
            except Exception as e:
                logger.error(f"Error in UDP listener: {e}")
                time.sleep(0.1)  # Avoid tight loop on persistent error
                
        logger.debug("UDP listener stopped")

    def _process_udp_message(self, data, addr):
        """
        Process a received UDP message.
        
        Args:
            data (bytes): The received data
            addr (tuple): Sender address (host, port)
        """
        try:
            # Extract header size
            header_size = int.from_bytes(data[:4], byteorder='big')
            
            # Extract and parse header
            header_end = 4 + header_size
            header_data = data[4:header_end]
            message_data = data[header_end:]
            metadata = json.loads(header_data.decode('utf-8'))
            
            # If this is a reliable UDP message, send an acknowledgment
            if metadata.get('reliable', False):
                message_id = metadata.get('message_id')
                if message_id:
                    # Send ACK for reliable messages
                    ack_data = json.dumps({
                        'type': 'ack',
                        'message_id': message_id,
                        'timestamp': time.time()
                    }).encode('utf-8')
                    self.udp_socket.sendto(ack_data, addr)
                    
            # If this is an ACK, process it for reliable UDP
            if metadata.get('type') == 'ack':
                message_id = metadata.get('message_id')
                if message_id in self.reliable_udp_messages:
                    del self.reliable_udp_messages[message_id]
                    return  # No further processing for ACKs
                    
            # Apply decompression if needed
            if metadata.get('compressed', False):
                message_data = self.compression.decompress_data(
                    message_data,
                    metadata.get('compression_type', 'zlib')
                )
                
            # Apply decryption if needed
            if metadata.get('encrypted', False) and self.encryption_enabled and self.encryption_manager:
                message_data = self.encryption_manager.decrypt(message_data)
                
            # Parse the message data if it's JSON
            if metadata.get('format', 'json') == 'json':
                message = json.loads(message_data.decode('utf-8'))
            else:
                message = message_data
                
            # Get the message type
            message_type = metadata.get('type')
            if isinstance(message, dict) and 'type' in message:
                message_type = message['type']
                
            # Store client address if in server mode
            if self.is_server and addr not in self.udp_clients:
                self.udp_clients.add(addr)
                
            # Call the appropriate handler if registered
            if message_type and message_type in self.message_handlers:
                self.message_handlers[message_type](message, addr)
            else:
                logger.warning(f"Received UDP message with unknown type: {message_type}")
                
        except Exception as e:
            logger.error(f"Error processing UDP message: {e}")

    def configure_transport_selection(self, enable=True, strategy=TransportSelectionStrategy.ADAPTIVE, size_threshold=4096):
        """
        Configure transport selection settings.
        
        Args:
            enable (bool): Whether to enable transport selection
            strategy (TransportSelectionStrategy): Strategy to use for transport selection
            size_threshold (int): Size threshold for selecting TCP vs UDP (in bytes)
            
        Returns:
            bool: True if configuration was successful
        """
        self.transport_selection_enabled = enable
        
        if enable:
            # Create or reconfigure transport selector
            if self.transport_selector is None:
                self.transport_selector = TransportSelector(strategy=strategy)
            else:
                self.transport_selector.configure(strategy=strategy, size_threshold=size_threshold)
            
            # Set callbacks for transport availability
            self.transport_selector.set_callbacks(
                tcp_available=self._is_tcp_available,
                udp_available=self._is_udp_available
            )
            
            logger.info(f"Transport selection enabled with strategy {strategy.name}")
            return True
        else:
            self.transport_selector = None
            logger.info("Transport selection disabled")
            return True

    def _is_tcp_available(self) -> bool:
        """
        Check if TCP transport is available.
        
        Returns:
            bool: True if TCP is available, False otherwise
        """
        # Check if the TCP socket is open and ready
        if self.is_server:
            return self.tcp_server_socket is not None
        else:
            return self.tcp_client_socket is not None and self.client_connected


    def _is_udp_available(self) -> bool:
        """
        Check if UDP transport is available.
        
        Returns:
            bool: True if UDP is available, False otherwise
        """
        # Check if the UDP socket is open
        return self.udp_socket is not None

    def send_udp(self, data, addr, compress=None, encrypt=None):
        """
        Send data via UDP.

        Args:
            data: Data to send
            addr: Address to send to
            compress: Whether to compress the data (overrides global setting)
            encrypt: Whether to encrypt the data (overrides global setting)

        Returns:
            bool: True if the data was sent
        """
        # Verify we're connected or running a server
        if not self.server_running and not self.client_connected:
            logger.error("Cannot send UDP data - not connected to a server and not running as a server")
            return False

        # Initialize transport_type for metrics
        transport_type = TransportType.UDP

        # Make sure the data is a dictionary
        if not isinstance(data, dict):
            data = {'type': 'raw', 'data': data}

        # Record the start time for metrics
        start_time = time.time() if self.metrics_enabled else None

        # Convert to JSON and encode
        try:
            json_data = json.dumps(data).encode('utf-8')
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to encode message to JSON: {e}")
            return False

        # Apply compression if enabled and size is above threshold
        if (compress is True) or (compress is None and self.compression_enabled and 
                 len(json_data) > self.compression_threshold):
            try:
                json_data, comp_type = self.compression.compress_data(
                    json_data, 
                    self.default_compression_type, 
                    self.compression_level
                )
                compressed = True
            except Exception as e:
                logger.error(f"Compression error: {e}")
                compressed = False
        else:
            compressed = False

        # Apply encryption if enabled
        if (encrypt is True) or (encrypt is None and self.encryption_enabled and 
                 self.encryption_manager):
            try:
                json_data = self.encryption_manager.encrypt(json_data)
                encrypted = True
            except Exception as e:
                logger.error(f"Encryption error: {e}")
                return False
        else:
            encrypted = False

        # Add metadata for the receiver
        metadata = {
            'compressed': compressed,
            'encrypted': encrypted,
            'timestamp': time.time(),
            'format': 'json',
            'type': data.get('type', 'generic')
        }

        # Combine metadata and data
        try:
            # Convert header to bytes
            header = json.dumps(metadata).encode('utf-8')

            # Calculate sizes
            header_size = len(header)

            # Limit header size to a reasonable value
            if header_size > 8192:  # 8KB max header size
                logger.warning(f"Header too large ({header_size} bytes), truncating metadata")
                metadata = {
                    'compressed': compressed,
                    'encrypted': encrypted,
                    'type': data.get('type', 'generic')
                }
                header = json.dumps(metadata).encode('utf-8')
                header_size = len(header)

            # Header size as 32-bit integer (4 bytes)
            header_size_bytes = header_size.to_bytes(4, byteorder='big')

            # Assemble the packet
            packet = header_size_bytes + header + json_data

            logger.debug(f"Packet created: {len(header_size_bytes)} bytes header size + {header_size} bytes header + {len(json_data)} bytes data")
        except Exception as e:
            logger.error(f"Failed to create packet: {e}")
            return False

        # Send the packet
        try:
            self.udp_socket.sendto(packet, addr)

            # Update metrics
            if self.metrics_enabled and self.metrics_manager:
                self.metrics_manager.record_send(
                    len(packet), 
                    transport_type, 
                    compressed, 
                    encrypted,
                    time.time() - start_time if start_time else 0
                )

            logger.debug(f"Sent {len(packet)} bytes to {addr} via UDP")
            return True

        except Exception as e:
            logger.error(f"UDP send error: {e}")
            return False