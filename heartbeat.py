#!/usr/bin/env python3
"""
Heartbeat module for the hybrid protocol library.

This module provides connection monitoring and health tracking through 
periodic heartbeat messages. It detects connection failures, measures 
round-trip time (RTT), and calculates connection health metrics.

Typical usage:
    from heartbeat import HeartbeatManager
    
    # Create a heartbeat manager
    heartbeat_mgr = HeartbeatManager(heartbeat_interval=5.0)
    
    # Set up callbacks
    heartbeat_mgr.set_callbacks(
        send_heartbeat=lambda msg, addr: send_function(msg, addr),
        connection_lost=lambda addr: handle_lost_connection(addr),
        connection_restored=lambda addr: handle_restored_connection(addr)
    )
    
    # Start the heartbeat manager
    heartbeat_mgr.start()
    
    # Process received heartbeats
    heartbeat_mgr.process_heartbeat(received_data, sender_address)
    
    # When done
    heartbeat_mgr.stop()
"""

import time
import threading
import logging
import uuid
from typing import Dict, Tuple, List, Optional, Callable, Any, Union

__version__ = '1.0.0'
__all__ = ['HeartbeatManager', 'ConnectionState']

logger = logging.getLogger("HybridProtocol.Heartbeat")

class ConnectionState:
    """Track the state of a connection with a peer."""
    
    def __init__(self, peer_addr: Tuple[str, int], 
                heartbeat_interval: float = 5.0, 
                timeout_threshold: float = 3.0):
        """
        Initialize the connection state.
        
        Args:
            peer_addr (tuple): (host, port) of the peer
            heartbeat_interval (float): Interval between heartbeats in seconds
            timeout_threshold (float): Number of missed heartbeats before connection is considered lost
        """
        self.peer_addr = peer_addr
        self.last_seen = time.time()
        self.heartbeat_interval = heartbeat_interval
        self.timeout_threshold = timeout_threshold
        self.rtt_samples: List[float] = []  # Round-trip time samples
        self.avg_rtt: Optional[float] = None
        self.is_alive = True
        self.heartbeat_replies: Dict[str, float] = {}  # Track sent heartbeats waiting for replies
        
    def update_last_seen(self) -> None:
        """Update the last_seen timestamp."""
        self.last_seen = time.time()
        
    def record_heartbeat_reply(self, heartbeat_id: str, send_time: float) -> float:
        """
        Record a heartbeat reply and calculate RTT.
        
        Args:
            heartbeat_id (str): ID of the heartbeat
            send_time (float): Time the heartbeat was sent
            
        Returns:
            float: Round-trip time in seconds
        """
        now = time.time()
        rtt = now - send_time
        
        # Add RTT sample and calculate average
        self.rtt_samples.append(rtt)
        # Keep last 10 samples
        if len(self.rtt_samples) > 10:
            self.rtt_samples.pop(0)
        self.avg_rtt = sum(self.rtt_samples) / len(self.rtt_samples)
        
        # Remove from waiting list
        if heartbeat_id in self.heartbeat_replies:
            del self.heartbeat_replies[heartbeat_id]
            
        return rtt
        
    def add_heartbeat_sent(self, heartbeat_id: str) -> None:
        """
        Record a sent heartbeat.
        
        Args:
            heartbeat_id (str): Unique ID for the heartbeat
        """
        self.heartbeat_replies[heartbeat_id] = time.time()
        
    def is_connection_timed_out(self) -> bool:
        """
        Check if the connection has timed out.
        
        Returns:
            bool: True if the connection has timed out, False otherwise
        """
        elapsed = time.time() - self.last_seen
        return elapsed > self.heartbeat_interval * self.timeout_threshold
        
    def get_connection_health(self) -> float:
        """
        Get a health score for the connection.
        
        The health score is a value between 0.0 (dead) and 1.0 (perfect),
        calculated using time since last seen and average RTT.
        
        Returns:
            float: Health score from 0.0 (dead) to 1.0 (perfect)
        """
        if not self.is_alive:
            return 0.0
            
        elapsed = time.time() - self.last_seen
        timeout = self.heartbeat_interval * self.timeout_threshold
        
        # Calculate health based on time since last message
        time_health = max(0.0, 1.0 - (elapsed / timeout))
        
        # Calculate health based on RTT if available
        rtt_health = 1.0
        if self.avg_rtt is not None:
            # Higher RTT = lower health, capped at reasonable values
            # 1000ms RTT is considered very poor (0.2 health)
            rtt_health = max(0.2, min(1.0, 1.0 - (self.avg_rtt / 1.0)))
            
        # Combined health score (weighted average)
        return 0.7 * time_health + 0.3 * rtt_health

class HeartbeatManager:
    """
    Manages heartbeats and connection monitoring for the hybrid protocol.
    
    The HeartbeatManager sends periodic heartbeats to all connected peers
    and monitors responses to detect connection failures. It also calculates
    connection health metrics based on response times.
    """
    
    def __init__(self, heartbeat_interval: float = 5.0, connection_timeout: float = 15.0):
        """
        Initialize the heartbeat manager.
        
        Args:
            heartbeat_interval (float): Interval between heartbeats in seconds
            connection_timeout (float): Time after which a connection is considered dead
        """
        self.heartbeat_interval = heartbeat_interval
        self.timeout_threshold = connection_timeout / heartbeat_interval
        self.connections: Dict[Tuple[str, int], ConnectionState] = {}  # addr -> ConnectionState
        
        # Callbacks
        self.send_heartbeat_callback: Optional[Callable[[Dict[str, Any], Tuple[str, int]], None]] = None
        self.connection_lost_callback: Optional[Callable[[Tuple[str, int]], None]] = None
        self.connection_restored_callback: Optional[Callable[[Tuple[str, int]], None]] = None
        
        # State
        self.running = False
        self.heartbeat_thread: Optional[threading.Thread] = None
        
    def start(self) -> None:
        """
        Start the heartbeat manager.
        
        This begins the heartbeat thread that periodically sends heartbeats
        and checks for connection timeouts.
        """
        if self.running:
            return
            
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_thread)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        logger.info("Heartbeat manager started")
        
    def stop(self) -> None:
        """
        Stop the heartbeat manager.
        
        This stops the heartbeat thread and releases resources.
        """
        self.running = False
        logger.info("Heartbeat manager stopped")
        
    def set_callbacks(self, 
                     send_heartbeat: Optional[Callable[[Dict[str, Any], Tuple[str, int]], None]] = None, 
                     connection_lost: Optional[Callable[[Tuple[str, int]], None]] = None, 
                     connection_restored: Optional[Callable[[Tuple[str, int]], None]] = None) -> None:
        """
        Set the callbacks for heartbeat events.
        
        Args:
            send_heartbeat (callable): Function to send a heartbeat message
                                      Takes (message, address) as parameters
            connection_lost (callable): Function called when a connection is lost
                                       Takes (address) as a parameter
            connection_restored (callable): Function called when a connection is restored
                                          Takes (address) as a parameter
        """
        self.send_heartbeat_callback = send_heartbeat
        self.connection_lost_callback = connection_lost
        self.connection_restored_callback = connection_restored
        
    def add_connection(self, peer_addr: Tuple[str, int]) -> None:
        """
        Add a connection to monitor.
        
        Args:
            peer_addr (tuple): (host, port) of the peer
        """
        if peer_addr not in self.connections:
            self.connections[peer_addr] = ConnectionState(
                peer_addr, 
                heartbeat_interval=self.heartbeat_interval,
                timeout_threshold=self.timeout_threshold
            )
            logger.debug(f"Added connection to monitor: {peer_addr}")
            
    def remove_connection(self, peer_addr: Tuple[str, int]) -> None:
        """
        Remove a connection from monitoring.
        
        Args:
            peer_addr (tuple): (host, port) of the peer
        """
        if peer_addr in self.connections:
            del self.connections[peer_addr]
            logger.debug(f"Removed connection from monitoring: {peer_addr}")
            
    def update_connection(self, peer_addr: Tuple[str, int]) -> None:
        """
        Update the last_seen timestamp for a connection.
        
        This should be called whenever any message is received from the peer,
        not just heartbeat messages.
        
        Args:
            peer_addr (tuple): (host, port) of the peer
        """
        if peer_addr in self.connections:
            self.connections[peer_addr].update_last_seen()
        else:
            self.add_connection(peer_addr)
            
    def process_heartbeat(self, heartbeat_data: Dict[str, Any], sender_addr: Tuple[str, int]) -> None:
        """
        Process a received heartbeat or heartbeat reply.
        
        This method handles both heartbeat requests (sending replies)
        and heartbeat replies (updating RTT).
        
        Args:
            heartbeat_data (dict): Heartbeat message data
            sender_addr (tuple): (host, port) of the sender
        """
        # Update connection state
        self.update_connection(sender_addr)
        
        # Check if this is a heartbeat reply
        if '_heartbeat_reply' in heartbeat_data and '_heartbeat_id' in heartbeat_data:
            heartbeat_id = heartbeat_data['_heartbeat_id']
            
            if sender_addr in self.connections and heartbeat_id in self.connections[sender_addr].heartbeat_replies:
                send_time = self.connections[sender_addr].heartbeat_replies[heartbeat_id]
                rtt = self.connections[sender_addr].record_heartbeat_reply(heartbeat_id, send_time)
                logger.debug(f"Heartbeat reply from {sender_addr}, RTT: {rtt:.6f}s")
                
                # Check if connection was previously dead
                if not self.connections[sender_addr].is_alive:
                    self.connections[sender_addr].is_alive = True
                    logger.info(f"Connection restored: {sender_addr}")
                    if self.connection_restored_callback:
                        self.connection_restored_callback(sender_addr)
                        
        # Check if this is a heartbeat request and send reply
        elif '_heartbeat' in heartbeat_data and '_heartbeat_id' in heartbeat_data:
            if self.send_heartbeat_callback:
                heartbeat_id = heartbeat_data['_heartbeat_id']
                reply = {
                    '_heartbeat_reply': True,
                    '_heartbeat_id': heartbeat_id
                }
                self.send_heartbeat_callback(reply, sender_addr)
                logger.debug(f"Sent heartbeat reply to {sender_addr}")
                
    def _heartbeat_thread(self) -> None:
        """Thread that sends heartbeats and checks for timeouts."""
        last_heartbeat_time = time.time()
        
        while self.running:
            now = time.time()
            
            # Send heartbeats if interval has elapsed
            if now - last_heartbeat_time >= self.heartbeat_interval:
                last_heartbeat_time = now
                self._send_heartbeats()
                
            # Check for timed out connections
            self._check_timeouts()
            
            # Sleep to avoid CPU spinning
            time.sleep(min(1.0, self.heartbeat_interval / 4))
            
    def _send_heartbeats(self) -> None:
        """Send heartbeats to all connections."""
        if not self.send_heartbeat_callback:
            return
            
        for addr, conn in list(self.connections.items()):
            heartbeat_id = str(uuid.uuid4())
            heartbeat = {
                '_heartbeat': True,
                '_heartbeat_id': heartbeat_id
            }
            
            # Record sent heartbeat
            conn.add_heartbeat_sent(heartbeat_id)
            
            # Send the heartbeat
            try:
                self.send_heartbeat_callback(heartbeat, addr)
                logger.debug(f"Sent heartbeat to {addr}")
            except Exception as e:
                logger.error(f"Error sending heartbeat to {addr}: {e}")
                
    def _check_timeouts(self) -> None:
        """Check for timed out connections."""
        for addr, conn in list(self.connections.items()):
            if conn.is_alive and conn.is_connection_timed_out():
                conn.is_alive = False
                logger.warning(f"Connection lost: {addr}")
                if self.connection_lost_callback:
                    self.connection_lost_callback(addr)
                    
    def get_connection_status(self) -> Dict[Tuple[str, int], Tuple[bool, float, Optional[float]]]:
        """
        Get the status of all connections.
        
        Returns:
            dict: Mapping of address to tuple containing:
                 (is_alive, health_score, avg_rtt)
        """
        status = {}
        for addr, conn in self.connections.items():
            status[addr] = (conn.is_alive, conn.get_connection_health(), conn.avg_rtt)
        return status

# Example usage
if __name__ == "__main__":
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Demonstrate heartbeat manager with mock connections
    def mock_send_heartbeat(message, addr):
        print(f"MOCK: Sending heartbeat to {addr}: {message}")
        
    def mock_connection_lost(addr):
        print(f"MOCK: Connection lost to {addr}")
        
    def mock_connection_restored(addr):
        print(f"MOCK: Connection restored to {addr}")
    
    # Create heartbeat manager
    manager = HeartbeatManager(heartbeat_interval=2.0, connection_timeout=6.0)
    manager.set_callbacks(
        send_heartbeat=mock_send_heartbeat,
        connection_lost=mock_connection_lost,
        connection_restored=mock_connection_restored
    )
    
    # Add some example connections
    peer1 = ('example.com', 8080)
    peer2 = ('192.168.1.100', 9000)
    
    manager.add_connection(peer1)
    manager.add_connection(peer2)
    
    # Start the manager
    manager.start()
    
    try:
        print("Heartbeat manager running... (Ctrl+C to stop)")
        
        # Simulate receiving a heartbeat from peer1
        heartbeat = {'_heartbeat': True, '_heartbeat_id': 'abc123'}
        manager.process_heartbeat(heartbeat, peer1)
        
        # Simulate receiving a reply from peer1
        time.sleep(1)
        reply = {'_heartbeat_reply': True, '_heartbeat_id': 'test-id-1'}
        manager.process_heartbeat(reply, peer1)
        
        # Wait for timeouts to occur
        time.sleep(10)
        
        # Check status
        status = manager.get_connection_status()
        print("\nConnection Status:")
        for addr, (is_alive, health, rtt) in status.items():
            print(f"  {addr}: {'ALIVE' if is_alive else 'DEAD'}, Health: {health:.2f}, RTT: {rtt or 'N/A'}")
            
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        manager.stop() 