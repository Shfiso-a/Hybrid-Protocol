#!/usr/bin/env python3
"""
Transport Selection Module for Hybrid Protocol

This module provides intelligent transport selection functionality for the Hybrid Protocol,
enabling automatic or policy-based decisions about whether to use TCP or UDP for each message.
It offers various strategies for transport selection:

- Manual: Explicit selection by the user
- Size-based: Chooses transport based on message size (large messages use TCP)
- Priority-based: Selects transport based on message priority levels
- Reliability-based: Uses TCP for messages requiring guaranteed delivery
- Adaptive: Makes dynamic decisions based on network conditions and historical performance

The module tracks transport performance metrics (latency, success rate) and can adapt to 
changing network conditions to optimize message delivery.

Version: 1.0.0
"""

import logging
import time
import json
import threading
from enum import Enum, auto
from typing import Dict, List, Optional, Callable, Any, Union, Tuple

__version__ = "1.0.0"
__all__ = ["TransportSelectionStrategy", "TransportSelector"]

logger = logging.getLogger("HybridProtocol.TransportSelection")

class TransportSelectionStrategy(Enum):
    """
    Available strategies for transport selection.
    
    - MANUAL: Explicit selection by the user
    - SIZE_BASED: Select based on message size (large messages use TCP)
    - PRIORITY: Select based on message priority levels  
    - RELIABILITY: Select based on required delivery guarantees
    - ADAPTIVE: Learn and adapt based on historical performance metrics
    """
    MANUAL = auto()        # Manual selection by the user
    SIZE_BASED = auto()    # Select based on message size
    PRIORITY = auto()      # Select based on message priority
    ADAPTIVE = auto()      # Learn and adapt based on historical performance
    RELIABILITY = auto()   # Select based on required reliability

class TransportSelector:
    """
    Intelligent transport selection for the hybrid protocol.
    
    Automatically chooses the most appropriate transport (TCP or UDP)
    based on message characteristics and network conditions. This class
    monitors transport performance and can adapt to changing conditions.
    """
    
    def __init__(self, strategy: TransportSelectionStrategy = TransportSelectionStrategy.ADAPTIVE):
        """
        Initialize the transport selector.
        
        Args:
            strategy: Strategy to use for transport selection
        """
        self.strategy = strategy
        self.transport_stats: Dict[str, Dict[str, Any]] = {
            'tcp': {
                'latency_samples': [],
                'avg_latency': None,
                'success_rate': 1.0,
                'last_failure': 0
            },
            'udp': {
                'latency_samples': [],
                'avg_latency': None,
                'success_rate': 1.0,
                'last_failure': 0
            }
        }
        
        # Configuration
        self.size_threshold: int = 4096  # Messages larger than this use TCP by default
        self.priority_thresholds: Dict[Any, Any] = {}  # Set by the user
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Callbacks
        self.tcp_available_callback: Optional[Callable[[], bool]] = None
        self.udp_available_callback: Optional[Callable[[], bool]] = None
        
    def set_callbacks(self, tcp_available: Optional[Callable[[], bool]] = None, 
                      udp_available: Optional[Callable[[], bool]] = None) -> None:
        """
        Set callbacks for checking transport availability.
        
        Args:
            tcp_available: Function returning True if TCP is available
            udp_available: Function returning True if UDP is available
        """
        self.tcp_available_callback = tcp_available
        self.udp_available_callback = udp_available
        
    def configure(self, **kwargs) -> None:
        """
        Configure the transport selector.
        
        Args:
            size_threshold: Size threshold for selecting TCP vs UDP (in bytes)
            priority_thresholds: Dictionary mapping priority levels to transport types
            strategy: Strategy to use for transport selection
        """
        with self.lock:
            if 'size_threshold' in kwargs:
                self.size_threshold = kwargs['size_threshold']
                
            if 'priority_thresholds' in kwargs:
                self.priority_thresholds = kwargs['priority_thresholds']
                
            if 'strategy' in kwargs:
                self.strategy = kwargs['strategy']
        
    def record_transport_result(self, transport_type: Any, 
                               latency: Optional[float] = None, 
                               success: bool = True) -> None:
        """
        Record the result of using a transport to improve future decisions.
        
        Args:
            transport_type: Transport type used (TCP or UDP)
            latency: Measured latency in seconds
            success: Whether the transmission was successful
        """
        transport = transport_type.name.lower()
        
        with self.lock:
            stats = self.transport_stats[transport]
            
            # Update latency if provided
            if latency is not None:
                stats['latency_samples'].append(latency)
                if len(stats['latency_samples']) > 20:
                    stats['latency_samples'].pop(0)
                stats['avg_latency'] = sum(stats['latency_samples']) / len(stats['latency_samples'])
            
            # Update success rate
            if not success:
                stats['last_failure'] = time.time()
                # Decrease success rate (weighted average)
                stats['success_rate'] = 0.8 * stats['success_rate']
            else:
                # Increase success rate (weighted average)
                stats['success_rate'] = 0.8 * stats['success_rate'] + 0.2
                
    def select_transport(self, message: Dict[str, Any], 
                        message_size: Optional[int] = None, 
                        priority: Optional[Any] = None, 
                        reliability_required: bool = False) -> Any:
        """
        Select the most appropriate transport for a message.
        
        Args:
            message: The message to send
            message_size: Size of the message in bytes (calculated if not provided)
            priority: Priority level of the message
            reliability_required: Whether reliability is required
            
        Returns:
            TransportType: The selected transport type (TCP or UDP)
        """
        # Import here to avoid circular imports
        from hybrid_protocol import TransportType
        
        # Check if the specified transports are available
        tcp_available = True
        udp_available = True
        
        if self.tcp_available_callback:
            tcp_available = self.tcp_available_callback()
            
        if self.udp_available_callback:
            udp_available = self.udp_available_callback()
            
        # Default to TCP if no other selection is made
        default_transport = TransportType.TCP
        
        # If only one transport is available, use that
        if not tcp_available and udp_available:
            return TransportType.UDP
        elif not udp_available and tcp_available:
            return TransportType.TCP
        elif not tcp_available and not udp_available:
            logger.error("No transports available")
            return default_transport
            
        # Apply selection strategy
        with self.lock:
            if self.strategy == TransportSelectionStrategy.MANUAL:
                # Manual selection is handled by the caller
                return default_transport
                
            elif self.strategy == TransportSelectionStrategy.SIZE_BASED:
                # Select based on message size
                if message_size is None:
                    message_size = len(json.dumps(message).encode('utf-8'))
                    
                if message_size > self.size_threshold:
                    return TransportType.TCP
                else:
                    return TransportType.UDP
                    
            elif self.strategy == TransportSelectionStrategy.PRIORITY:
                # Select based on message priority
                if priority is not None and priority in self.priority_thresholds:
                    return self.priority_thresholds[priority]
                else:
                    return default_transport
                    
            elif self.strategy == TransportSelectionStrategy.RELIABILITY:
                # Select based on required reliability
                if reliability_required:
                    return TransportType.TCP
                else:
                    return TransportType.UDP
                    
            elif self.strategy == TransportSelectionStrategy.ADAPTIVE:
                # Learn and adapt based on historical performance
                tcp_stats = self.transport_stats['tcp']
                udp_stats = self.transport_stats['udp']
                
                # Check if we have reliability requirements
                if reliability_required:
                    return TransportType.TCP
                
                # Check if we have size requirements
                if message_size is None:
                    message_size = len(json.dumps(message).encode('utf-8'))
                    
                if message_size > self.size_threshold:
                    return TransportType.TCP
                
                # Check for recent failures
                current_time = time.time()
                tcp_failure_recency = current_time - tcp_stats['last_failure']
                udp_failure_recency = current_time - udp_stats['last_failure']
                
                # If one transport has failed recently, prefer the other
                if tcp_failure_recency < 60 and udp_failure_recency > 60:
                    return TransportType.UDP
                elif udp_failure_recency < 60 and tcp_failure_recency > 60:
                    return TransportType.TCP
                
                # If we have latency data, prefer the faster transport
                if tcp_stats['avg_latency'] is not None and udp_stats['avg_latency'] is not None:
                    # Prefer UDP for low latency unless its success rate is bad
                    if udp_stats['avg_latency'] < tcp_stats['avg_latency'] and udp_stats['success_rate'] > 0.7:
                        return TransportType.UDP
                    else:
                        return TransportType.TCP
                
                # Default to TCP for better reliability
                return TransportType.TCP
            
        # Fallback
        return default_transport
        
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current transport statistics.
        
        Returns:
            Dict containing transport statistics and performance metrics
        """
        with self.lock:
            return {
                'strategy': self.strategy.name,
                'size_threshold': self.size_threshold,
                'tcp': {
                    'avg_latency': self.transport_stats['tcp']['avg_latency'],
                    'success_rate': self.transport_stats['tcp']['success_rate'],
                    'samples': len(self.transport_stats['tcp']['latency_samples'])
                },
                'udp': {
                    'avg_latency': self.transport_stats['udp']['avg_latency'],
                    'success_rate': self.transport_stats['udp']['success_rate'],
                    'samples': len(self.transport_stats['udp']['latency_samples'])
                }
            }
            
    def reset_stats(self) -> None:
        """Reset all transport statistics to their initial values."""
        with self.lock:
            for transport in self.transport_stats:
                self.transport_stats[transport] = {
                    'latency_samples': [],
                    'avg_latency': None,
                    'success_rate': 1.0,
                    'last_failure': 0
                }


# Example usage
def example_usage():
    """Demonstrate the usage of the TransportSelector class."""
    # For this example, we'll create a mock TransportType enum
    class MockTransportType(Enum):
        TCP = auto()
        UDP = auto()
    
    # Set up logging for the example
    logging.basicConfig(level=logging.INFO)
    
    # Create a transport selector with adaptive strategy
    selector = TransportSelector(strategy=TransportSelectionStrategy.ADAPTIVE)
    
    # Configure it with custom settings
    selector.configure(
        size_threshold=8192,  # 8KB threshold for TCP vs UDP
        priority_thresholds={
            'HIGH': MockTransportType.TCP,
            'MEDIUM': MockTransportType.TCP,
            'LOW': MockTransportType.UDP
        }
    )
    
    # Define callback functions to check transport availability
    def is_tcp_available():
        return True  # TCP is available
        
    def is_udp_available():
        return True  # UDP is available
    
    # Set the callbacks
    selector.set_callbacks(
        tcp_available=is_tcp_available,
        udp_available=is_udp_available
    )
    
    # Example message
    message = {
        "type": "sensor_data",
        "temperature": 22.5,
        "humidity": 65,
        "timestamp": time.time()
    }
    
    # Select transport for a small message
    transport = selector.select_transport(
        message=message,
        priority='LOW',
        reliability_required=False
    )
    print(f"Selected transport for small message: {transport.name}")
    
    # Record a successful transmission with low latency
    selector.record_transport_result(
        transport_type=transport,
        latency=0.01,  # 10ms
        success=True
    )
    
    # Select transport for a large message
    large_message = {"data": "x" * 10000}  # 10KB of data
    transport = selector.select_transport(
        message=large_message,
        reliability_required=True
    )
    print(f"Selected transport for large message: {transport.name}")
    
    # Get current statistics
    stats = selector.get_stats()
    print(f"Transport statistics: {stats}")

if __name__ == "__main__":
    example_usage() 