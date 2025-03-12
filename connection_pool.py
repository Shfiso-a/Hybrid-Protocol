#!/usr/bin/env python3
"""
Connection pool module for the hybrid protocol library.

This module provides a reusable connection pooling system for TCP socket connections,
enabling efficient connection reuse, automatic connection lifecycle management,
and support for both plain TCP and SSL/TLS connections.

Typical usage:
    from connection_pool import ConnectionPool, Connection
    
    # Create a connection pool
    pool = ConnectionPool(max_connections=20)
    pool.start()
    
    # Get a connection
    conn = pool.get_connection(('example.com', 443), 
                              ssl_params={'ca_file': 'ca.crt'})
    
    # Use the connection
    conn.socket.sendall(b'Hello')
    data = conn.socket.recv(1024)
    
    # Release the connection back to the pool
    pool.release_connection(conn)
    
    # Stop the pool when done
    pool.stop()
"""

import socket
import time
import threading
import logging
import queue
import ssl
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any, Union, DefaultDict

__version__ = '1.0.0'
__all__ = ['ConnectionPool', 'Connection', 'ConnectionPoolError']

logger = logging.getLogger("HybridProtocol.ConnectionPool")

class ConnectionPoolError(Exception):
    """Exceptions related to connection pool operations."""
    pass

class Connection:
    """Represents a reusable connection in the pool."""
    
    def __init__(self, sock: socket.socket, addr: Tuple[str, int], 
                is_ssl: bool = False, created_time: Optional[float] = None):
        """
        Initialize a connection object.
        
        Args:
            sock (socket.socket): Socket object
            addr (tuple): (host, port) tuple
            is_ssl (bool): Whether this is an SSL connection
            created_time (float, optional): When the connection was created
        """
        self.socket = sock
        self.addr = addr
        self.is_ssl = is_ssl
        self.created_time = created_time or time.time()
        self.last_used = time.time()
        self.in_use = False
        self.error_count = 0
        
    def mark_used(self) -> None:
        """Mark the connection as in use."""
        self.in_use = True
        self.last_used = time.time()
        
    def mark_unused(self) -> None:
        """Mark the connection as available."""
        self.in_use = False
        self.last_used = time.time()
        
    def mark_error(self) -> None:
        """Mark that an error occurred on this connection."""
        self.error_count += 1
        
    def is_healthy(self, max_errors: int = 3, 
                 max_idle_time: float = 300, 
                 max_lifetime: float = 3600) -> bool:
        """
        Check if the connection is healthy.
        
        Args:
            max_errors (int): Maximum number of errors before connection is unhealthy
            max_idle_time (float): Maximum time in seconds a connection can be idle
            max_lifetime (float): Maximum lifetime of a connection in seconds
            
        Returns:
            bool: True if the connection is healthy
        """
        now = time.time()
        
        # Check error count
        if self.error_count >= max_errors:
            return False
            
        # Check idle time
        if now - self.last_used > max_idle_time:
            return False
            
        # Check lifetime
        if now - self.created_time > max_lifetime:
            return False
            
        return True
        
    def close(self) -> None:
        """Close the connection."""
        try:
            self.socket.close()
        except Exception as e:
            logger.debug(f"Error closing connection to {self.addr}: {e}")

class ConnectionPool:
    """
    Manages a pool of reusable TCP connections.
    Reduces connection establishment overhead for frequently
    accessed endpoints.
    """
    
    def __init__(self, max_connections: int = 10, connection_timeout: float = 5.0,
                 max_idle_time: float = 300, max_lifetime: float = 3600, 
                 max_errors: int = 3, maintenance_interval: float = 60):
        """
        Initialize the connection pool.
        
        Args:
            max_connections (int): Maximum number of connections to keep per remote address
            connection_timeout (float): Timeout in seconds for establishing new connections
            max_idle_time (float): Maximum time in seconds a connection can be idle
            max_lifetime (float): Maximum lifetime of a connection in seconds
            max_errors (int): Maximum number of errors before connection is removed
            maintenance_interval (float): Interval in seconds for running maintenance tasks
        """
        # Configuration
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.max_idle_time = max_idle_time
        self.max_lifetime = max_lifetime
        self.max_errors = max_errors
        self.maintenance_interval = maintenance_interval
        
        # Connection storage - by address
        self.pool: DefaultDict[Tuple[str, int], List[Connection]] = defaultdict(list)
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Maintenance thread
        self.running = False
        self.maintenance_thread: Optional[threading.Thread] = None
        
        # SSL context cache
        self.ssl_contexts: Dict[Tuple[Optional[str], Optional[str], Optional[str]], ssl.SSLContext] = {}
        
    def start(self) -> None:
        """
        Start the connection pool maintenance thread.
        
        This thread periodically checks connections for issues and removes
        unhealthy connections from the pool.
        """
        if self.running:
            return
            
        self.running = True
        self.maintenance_thread = threading.Thread(target=self._maintenance_thread)
        self.maintenance_thread.daemon = True
        self.maintenance_thread.start()
        logger.info("Connection pool maintenance started")
        
    def stop(self) -> None:
        """
        Stop the connection pool and close all connections.
        
        Should be called when the pool is no longer needed to 
        ensure all sockets are properly closed.
        """
        self.running = False
        
        with self.lock:
            # Close all connections
            for addr in list(self.pool.keys()):
                for conn in self.pool[addr]:
                    conn.close()
                self.pool[addr] = []
                
        logger.info("Connection pool stopped")
        
    def get_connection(self, addr: Tuple[str, int], 
                      ssl_params: Optional[Dict[str, Any]] = None, 
                      create_if_needed: bool = True) -> Connection:
        """
        Get a connection from the pool or create a new one.
        
        Args:
            addr (tuple): (host, port) tuple
            ssl_params (dict, optional): SSL parameters including:
                - key_file (str): Path to client key file
                - cert_file (str): Path to client certificate file
                - ca_file (str): Path to CA certificate file
            create_if_needed (bool): Whether to create a new connection if none available
            
        Returns:
            Connection: A connection object
            
        Raises:
            ConnectionPoolError: If no connection could be established
        """
        is_ssl = ssl_params is not None
        
        with self.lock:
            # Try to find an available connection in the pool
            connections = self.pool[addr]
            
            for i, conn in enumerate(connections):
                if not conn.in_use and conn.is_ssl == is_ssl and conn.is_healthy(
                        self.max_errors, self.max_idle_time, self.max_lifetime):
                    # Found an available connection
                    conn.mark_used()
                    
                    # Move to the end of the list (most recently used)
                    connections.pop(i)
                    connections.append(conn)
                    
                    return conn
                    
        # No available connection found, create a new one if allowed
        if create_if_needed:
            return self._create_connection(addr, ssl_params)
        else:
            raise ConnectionPoolError(f"No available connection for {addr}")
            
    def _create_connection(self, addr: Tuple[str, int], 
                          ssl_params: Optional[Dict[str, Any]] = None) -> Connection:
        """
        Create a new connection.
        
        Args:
            addr (tuple): (host, port) tuple
            ssl_params (dict, optional): SSL parameters
            
        Returns:
            Connection: A new connection object
            
        Raises:
            ConnectionPoolError: If the connection could not be established
        """
        try:
            # Create socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.connection_timeout)
            
            # Connect
            sock.connect(addr)
            
            # Apply SSL if needed
            is_ssl = False
            if ssl_params:
                context = self._get_ssl_context(ssl_params)
                sock = context.wrap_socket(sock, server_hostname=addr[0])
                is_ssl = True
                
            # Create connection object
            conn = Connection(sock, addr, is_ssl=is_ssl)
            conn.mark_used()
            
            # Add to pool
            with self.lock:
                pool_connections = self.pool[addr]
                
                # Check if we need to remove an old connection
                if len(pool_connections) >= self.max_connections:
                    # Find the oldest unused connection
                    oldest = None
                    oldest_idx = -1
                    
                    for i, c in enumerate(pool_connections):
                        if not c.in_use and (oldest is None or c.last_used < oldest.last_used):
                            oldest = c
                            oldest_idx = i
                            
                    if oldest is not None:
                        # Remove and close the oldest
                        oldest.close()
                        pool_connections.pop(oldest_idx)
                    else:
                        # All connections are in use, can't add more
                        conn.close()
                        raise ConnectionPoolError(f"All connections to {addr} are in use")
                        
                # Add the new connection
                pool_connections.append(conn)
                
            return conn
            
        except socket.error as e:
            logger.error(f"Error creating connection to {addr}: {e}")
            raise ConnectionPoolError(f"Failed to connect to {addr}: {e}")
            
    def release_connection(self, conn: Optional[Connection]) -> None:
        """
        Release a connection back to the pool.
        
        Args:
            conn (Connection): Connection to release
        """
        if conn is None:
            return
            
        with self.lock:
            if conn.addr in self.pool and conn in self.pool[conn.addr]:
                conn.mark_unused()
            else:
                # Connection not in pool, close it
                conn.close()
                
    def remove_connection(self, conn: Optional[Connection]) -> None:
        """
        Remove a connection from the pool and close it.
        
        Args:
            conn (Connection): Connection to remove
        """
        if conn is None:
            return
            
        with self.lock:
            if conn.addr in self.pool:
                connections = self.pool[conn.addr]
                if conn in connections:
                    connections.remove(conn)
                    
        conn.close()
        
    def _get_ssl_context(self, ssl_params: Dict[str, Any]) -> ssl.SSLContext:
        """
        Get or create an SSL context.
        
        Args:
            ssl_params (dict): SSL parameters
            
        Returns:
            ssl.SSLContext: SSL context
        """
        key_file = ssl_params.get('key_file')
        cert_file = ssl_params.get('cert_file')
        ca_file = ssl_params.get('ca_file')
        
        # Use cached context if available
        context_key = (key_file, cert_file, ca_file)
        if context_key in self.ssl_contexts:
            return self.ssl_contexts[context_key]
            
        # Create a new context
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        
        if ca_file:
            context.load_verify_locations(cafile=ca_file)
            
        if key_file and cert_file:
            context.load_cert_chain(certfile=cert_file, keyfile=key_file)
            
        # Cache the context
        self.ssl_contexts[context_key] = context
        
        return context
        
    def _maintenance_thread(self) -> None:
        """Thread that periodically checks and cleans up connections."""
        while self.running:
            try:
                self._clean_connections()
            except Exception as e:
                logger.error(f"Error in connection pool maintenance: {e}")
                
            time.sleep(self.maintenance_interval)
            
    def _clean_connections(self) -> None:
        """Clean up idle and unhealthy connections."""
        with self.lock:
            for addr in list(self.pool.keys()):
                connections = self.pool[addr]
                
                # Find connections to remove
                to_remove = []
                for i, conn in enumerate(connections):
                    if not conn.in_use and not conn.is_healthy(
                            self.max_errors, self.max_idle_time, self.max_lifetime):
                        to_remove.append(i)
                        
                # Remove connections in reverse order
                for i in reversed(to_remove):
                    conn = connections.pop(i)
                    conn.close()
                    
                # Remove empty entries
                if not connections:
                    del self.pool[addr]
                    
    def get_pool_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the connection pool.
        
        Returns:
            dict: Pool statistics including:
                - addresses: Number of unique remote addresses
                - total_connections: Total number of connections
                - in_use_connections: Number of connections currently in use
                - available_connections: Number of available connections
                - ssl_connections: Number of SSL connections
                - per_address: Stats broken down by remote address
        """
        with self.lock:
            stats = {
                'addresses': len(self.pool),
                'total_connections': 0,
                'in_use_connections': 0,
                'available_connections': 0,
                'ssl_connections': 0,
                'per_address': {}
            }
            
            for addr, connections in self.pool.items():
                addr_str = f"{addr[0]}:{addr[1]}"
                addr_stats = {
                    'total': len(connections),
                    'in_use': 0,
                    'available': 0,
                    'ssl': 0
                }
                
                for conn in connections:
                    stats['total_connections'] += 1
                    
                    if conn.in_use:
                        stats['in_use_connections'] += 1
                        addr_stats['in_use'] += 1
                    else:
                        stats['available_connections'] += 1
                        addr_stats['available'] += 1
                        
                    if conn.is_ssl:
                        stats['ssl_connections'] += 1
                        addr_stats['ssl'] += 1
                        
                stats['per_address'][addr_str] = addr_stats
                
            return stats

# Example usage
if __name__ == "__main__":
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create a connection pool
    pool = ConnectionPool(
        max_connections=5,
        connection_timeout=3.0
    )
    pool.start()
    
    try:
        # Create a plain connection
        print("\nCreating plain connection to httpbin.org:80...")
        conn = pool.get_connection(('httpbin.org', 80))
        
        # Send an HTTP request
        request = b"GET /json HTTP/1.1\r\nHost: httpbin.org\r\nConnection: keep-alive\r\n\r\n"
        conn.socket.sendall(request)
        
        # Receive the response
        response = b""
        while True:
            chunk = conn.socket.recv(4096)
            if not chunk:
                break
            response += chunk
            if b"\r\n\r\n" in response and b"}\n" in response:
                break
                
        print(f"Received response: {len(response)} bytes")
        print(response.decode('utf-8').split('\r\n\r\n')[1][:150] + "...")
        
        # Release the connection
        pool.release_connection(conn)
        
        # Create an SSL connection
        print("\nCreating SSL connection to httpbin.org:443...")
        ssl_conn = pool.get_connection(('httpbin.org', 443), ssl_params={})
        
        # Send an HTTPS request
        request = b"GET /json HTTP/1.1\r\nHost: httpbin.org\r\nConnection: keep-alive\r\n\r\n"
        ssl_conn.socket.sendall(request)
        
        # Receive the response
        response = b""
        while True:
            chunk = ssl_conn.socket.recv(4096)
            if not chunk:
                break
            response += chunk
            if b"\r\n\r\n" in response and b"}\n" in response:
                break
                
        print(f"Received SSL response: {len(response)} bytes")
        print(response.decode('utf-8').split('\r\n\r\n')[1][:150] + "...")
        
        # Release the connection
        pool.release_connection(ssl_conn)
        
        # Check pool stats
        stats = pool.get_pool_stats()
        print("\nConnection pool stats:")
        print(f"- Total addresses: {stats['addresses']}")
        print(f"- Total connections: {stats['total_connections']}")
        print(f"- In use: {stats['in_use_connections']}")
        print(f"- Available: {stats['available_connections']}")
        print(f"- SSL connections: {stats['ssl_connections']}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Stop the pool
        pool.stop() 