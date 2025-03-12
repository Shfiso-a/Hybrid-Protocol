#!/usr/bin/env python3
"""
Quality of Service (QoS) module for the hybrid protocol library.

This module provides advanced QoS capabilities including:
- Priority-based message scheduling: Messages are processed based on priority levels
- Flow control: Multiple strategies to prevent overwhelming receivers
- Rate limiting: Control message transmission rates
- Congestion control: Adaptive window sizing based on network conditions

These QoS mechanisms help ensure critical messages are delivered promptly
while preventing network congestion and managing resource utilization efficiently.

Typical usage:
    from qos import QoSManager, PriorityLevel, FlowControlStrategy
    
    # Create QoS manager with rate limiting
    qos = QoSManager(
        flow_control_strategy=FlowControlStrategy.RATE_LIMITING,
        flow_control_params={'rate': 100, 'capacity': 200}
    )
    
    # Set callback function for message sending
    qos.set_send_callback(lambda data, addr, transport: send_func(data, addr, transport))
    
    # Queue messages with different priorities
    qos.queue_message((critical_data, addr, transport), PriorityLevel.CRITICAL)
    qos.queue_message((normal_data, addr, transport), PriorityLevel.NORMAL)
    qos.queue_message((bulk_data, addr, transport), PriorityLevel.BULK)
    
    # The QoS manager will send these messages in priority order
    # while respecting the flow control constraints
"""

import time
import threading
import queue
import logging
from enum import Enum, auto
import heapq
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, TypeVar, Generic

__version__ = '1.0.0'
__all__ = ['PriorityLevel', 'FlowControlStrategy', 'PriorityQueue', 
           'RateLimiter', 'WindowBasedFlowControl', 'QoSManager']

logger = logging.getLogger("HybridProtocol.QoS")

T = TypeVar('T')  # Type variable for queue items

class PriorityLevel(Enum):
    """
    Priority levels for message handling.
    
    These levels determine the order in which messages are processed:
    - CRITICAL: Highest priority, for emergency or system-critical messages
    - HIGH: Important messages that require prompt handling
    - NORMAL: Default priority for typical messages
    - LOW: Non-essential messages that can be delayed if necessary
    - BULK: Lowest priority, for large data transfers or background operations
    """
    CRITICAL = 0   # Highest priority - emergency, critical
    HIGH = 1       # Important messages that should be processed quickly
    NORMAL = 2     # Default priority level
    LOW = 3        # Non-essential, background messages
    BULK = 4       # Lowest priority - large data transfers, non-time-sensitive

class FlowControlStrategy(Enum):
    """
    Available flow control strategies.
    
    - NONE: No flow control, send messages as fast as possible
    - RATE_LIMITING: Limit messages per second using token bucket algorithm
    - WINDOW_BASED: TCP-like sliding window congestion control
    - TOKEN_BUCKET: Token bucket algorithm (more sophisticated version)
    - LEAKY_BUCKET: Leaky bucket algorithm for consistent output rates
    """
    NONE = auto()           # No flow control
    RATE_LIMITING = auto()  # Limit messages per second
    WINDOW_BASED = auto()   # TCP-like sliding window
    TOKEN_BUCKET = auto()   # Token bucket algorithm
    LEAKY_BUCKET = auto()   # Leaky bucket algorithm

class PriorityQueue(Generic[T]):
    """
    A queue that processes items based on priority.
    
    This queue uses a heap to efficiently manage items with different priorities,
    ensuring that higher-priority items are always processed before lower-priority ones.
    """
    
    def __init__(self) -> None:
        """Initialize the priority queue with an empty heap queue."""
        self.queue: List[Tuple[int, int, T]] = []  # Heap queue [(priority, timestamp, item)]
        self.lock = threading.Lock()
        self.counter = 0  # Used for tie-breaking
        
    def put(self, item: T, priority: Union[PriorityLevel, int] = PriorityLevel.NORMAL) -> None:
        """
        Add an item to the queue with the specified priority.
        
        Args:
            item: Item to add to the queue
            priority: Priority level (lower number = higher priority)
        """
        with self.lock:
            # Convert enum to int if needed
            priority_value = priority.value if isinstance(priority, PriorityLevel) else priority
            # Push to heap queue - lower numbers = higher priority
            self.counter += 1
            heapq.heappush(self.queue, (priority_value, self.counter, item))
            
    def get(self) -> Optional[T]:
        """
        Get the highest priority item from the queue.
        
        Returns:
            The item with the highest priority, or None if the queue is empty
        """
        with self.lock:
            if not self.queue:
                return None
            # Return just the item, not the priority
            return heapq.heappop(self.queue)[2]
            
    def empty(self) -> bool:
        """
        Check if the queue is empty.
        
        Returns:
            True if the queue is empty, False otherwise
        """
        with self.lock:
            return len(self.queue) == 0
            
    def size(self) -> int:
        """
        Get the number of items in the queue.
        
        Returns:
            The number of items currently in the queue
        """
        with self.lock:
            return len(self.queue)

class RateLimiter:
    """
    Rate limiter using token bucket algorithm.
    
    This class implements a token bucket algorithm for rate limiting.
    Tokens are added to a bucket at a constant rate, and messages consume
    tokens when sent. If there are no tokens available, messages must wait.
    """
    
    def __init__(self, rate: float, capacity: Optional[float] = None) -> None:
        """
        Initialize the rate limiter.
        
        Args:
            rate: Maximum rate in tokens per second
            capacity: Maximum bucket capacity. Defaults to the same value as rate.
        """
        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        self.tokens = self.capacity  # Start with a full bucket
        self.last_update = time.time()
        self.lock = threading.Lock()
        
    def consume(self, tokens: float = 1) -> bool:
        """
        Try to consume tokens from the bucket.
        
        Args:
            tokens: Number of tokens to consume
            
        Returns:
            True if tokens were consumed, False if not enough tokens
        """
        with self.lock:
            # Refill tokens based on time elapsed
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            # Check if we have enough tokens
            if tokens <= self.tokens:
                self.tokens -= tokens
                return True
            else:
                return False
                
    def wait_for_tokens(self, tokens: float = 1, timeout: Optional[float] = None) -> bool:
        """
        Wait until tokens are available, then consume them.
        
        Args:
            tokens: Number of tokens to consume
            timeout: Maximum time to wait in seconds, or None to wait indefinitely
            
        Returns:
            True if tokens were consumed, False if timeout occurred
        """
        start_time = time.time()
        while True:
            if self.consume(tokens):
                return True
                
            # Check timeout
            if timeout is not None and time.time() - start_time > timeout:
                return False
                
            # Sleep a bit before retrying
            time.sleep(0.01)

class WindowBasedFlowControl:
    """
    TCP-like sliding window flow control.
    
    This class implements a TCP-like sliding window algorithm for flow control.
    It maintains a congestion window that grows additively on success and
    shrinks multiplicatively on failures, adapting to network conditions.
    """
    
    def __init__(self, initial_window_size: int = 10, max_window_size: int = 100) -> None:
        """
        Initialize the flow control.
        
        Args:
            initial_window_size: Initial window size (in messages)
            max_window_size: Maximum window size (in messages)
        """
        self.window_size = initial_window_size
        self.max_window_size = max_window_size
        self.in_flight = 0
        self.lock = threading.Lock()
        
    def can_send(self) -> bool:
        """
        Check if a new message can be sent based on current window.
        
        Returns:
            True if a new message can be sent, False otherwise
        """
        with self.lock:
            return self.in_flight < self.window_size
            
    def message_sent(self) -> None:
        """
        Call when a message is sent to increment the in-flight counter.
        """
        with self.lock:
            self.in_flight += 1
            
    def message_acknowledged(self, success: bool = True) -> None:
        """
        Call when a message is acknowledged to update the window size.
        
        Args:
            success: Whether the message was delivered successfully
        """
        with self.lock:
            self.in_flight -= 1
            
            # Adjust window size based on success/failure (simplified TCP congestion control)
            if success:
                # Additive increase
                self.window_size = min(self.window_size + 1, self.max_window_size)
            else:
                # Multiplicative decrease
                self.window_size = max(1, self.window_size // 2)
                
    def get_window_stats(self) -> Dict[str, int]:
        """
        Get current window statistics.
        
        Returns:
            Dictionary with window statistics
        """
        with self.lock:
            return {
                "window_size": self.window_size,
                "max_window_size": self.max_window_size,
                "in_flight": self.in_flight,
                "available": max(0, self.window_size - self.in_flight)
            }

class QoSManager:
    """
    Manages Quality of Service for the hybrid protocol.
    
    This class provides comprehensive QoS functionality including:
    - Priority-based message scheduling using a priority queue
    - Flow control using multiple selectable strategies
    - Automatic message sending based on priority and flow control constraints
    """
    
    def __init__(self, flow_control_strategy: FlowControlStrategy = FlowControlStrategy.RATE_LIMITING,
                 flow_control_params: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the QoS manager.
        
        Args:
            flow_control_strategy: Flow control strategy to use
            flow_control_params: Parameters for the flow control strategy
                - For RATE_LIMITING: 'rate' (msgs/sec) and 'capacity'
                - For WINDOW_BASED: 'window_size' and 'max_window_size'
        """
        # Default flow control parameters
        if flow_control_params is None:
            flow_control_params = {
                'rate': 100,  # messages per second
                'capacity': 200,
                'window_size': 50,
                'max_window_size': 200
            }
            
        self.flow_control_strategy = flow_control_strategy
        self.flow_control_params = flow_control_params
        
        # Create appropriate flow control mechanism
        self.rate_limiter: Optional[RateLimiter] = None
        self.window_control: Optional[WindowBasedFlowControl] = None
        
        if flow_control_strategy == FlowControlStrategy.RATE_LIMITING:
            self.rate_limiter = RateLimiter(
                flow_control_params.get('rate', 100),
                flow_control_params.get('capacity', 200)
            )
        elif flow_control_strategy == FlowControlStrategy.WINDOW_BASED:
            self.window_control = WindowBasedFlowControl(
                flow_control_params.get('window_size', 50),
                flow_control_params.get('max_window_size', 200)
            )
        
        # Priority queue for outgoing messages
        self.send_queue: PriorityQueue = PriorityQueue()
        
        # Start the sender thread if using priority queueing
        self.running = True
        self.sender_thread: Optional[threading.Thread] = None
        self.sender_thread = threading.Thread(target=self._sender_thread)
        self.sender_thread.daemon = True
        self.sender_thread.start()
        
        # Callback to be set by the protocol
        self.send_callback: Optional[Callable] = None
        
        logger.info(f"QoS manager initialized with {flow_control_strategy.name} flow control")
    
    def set_send_callback(self, callback: Callable[[Any, Any, Any], None]) -> None:
        """
        Set the callback function to be called when sending a message.
        
        Args:
            callback: Function that takes (message_data, addr, transport_type) as parameters
        """
        self.send_callback = callback
        
    def queue_message(self, message_data: Tuple[Any, Any, Any], 
                     priority: PriorityLevel = PriorityLevel.NORMAL) -> None:
        """
        Queue a message to be sent with the given priority.
        
        Args:
            message_data: Tuple of (data, addr, transport_type) to send
            priority: Priority level for this message
        """
        self.send_queue.put(message_data, priority)
        logger.debug(f"Queued message with priority {priority.name}")
        
    def can_send_now(self) -> bool:
        """
        Check if a new message can be sent based on flow control.
        
        Returns:
            True if a new message can be sent, False otherwise
        """
        if self.flow_control_strategy == FlowControlStrategy.NONE:
            return True
        elif self.flow_control_strategy == FlowControlStrategy.RATE_LIMITING:
            return self.rate_limiter.consume() if self.rate_limiter else True
        elif self.flow_control_strategy == FlowControlStrategy.WINDOW_BASED:
            return self.window_control.can_send() if self.window_control else True
        return True
        
    def message_sent(self) -> None:
        """
        Call when a message is sent for flow control tracking.
        
        This updates the flow control state based on the current strategy.
        """
        if self.flow_control_strategy == FlowControlStrategy.WINDOW_BASED and self.window_control:
            self.window_control.message_sent()
            
    def message_acknowledged(self, success: bool = True) -> None:
        """
        Call when a message is acknowledged for flow control tracking.
        
        Args:
            success: Whether the message was successfully delivered
        """
        if self.flow_control_strategy == FlowControlStrategy.WINDOW_BASED and self.window_control:
            self.window_control.message_acknowledged(success)
            
    def _sender_thread(self) -> None:
        """
        Thread that processes the send queue based on priority.
        
        This internal method runs as a background thread to continuously
        process messages from the queue in priority order while respecting
        flow control constraints.
        """
        while self.running:
            if not self.send_queue.empty() and self.can_send_now() and self.send_callback:
                # Get the highest priority message and send it
                message_data = self.send_queue.get()
                if message_data:
                    try:
                        self.send_callback(*message_data)
                        self.message_sent()
                    except Exception as e:
                        logger.error(f"Error in QoS sender: {e}")
            time.sleep(0.001)  # Small sleep to prevent CPU spinning
            
    def stop(self) -> None:
        """
        Stop the QoS manager.
        
        This stops the background sender thread and releases resources.
        """
        self.running = False
        logger.info("QoS manager stopped")
        
    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the priority queue.
        
        Returns:
            Dictionary with queue statistics
        """
        return {
            "queued_messages": self.send_queue.size(),
            "queue_empty": self.send_queue.empty()
        }
        
    def get_flow_control_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the flow control.
        
        Returns:
            Dictionary with flow control statistics based on the active strategy
        """
        stats = {
            "strategy": self.flow_control_strategy.name,
        }
        
        if self.flow_control_strategy == FlowControlStrategy.RATE_LIMITING and self.rate_limiter:
            stats.update({
                "rate": self.rate_limiter.rate,
                "capacity": self.rate_limiter.capacity,
                "current_tokens": self.rate_limiter.tokens
            })
        elif self.flow_control_strategy == FlowControlStrategy.WINDOW_BASED and self.window_control:
            stats.update(self.window_control.get_window_stats())
            
        return stats
        
    def update_flow_control_params(self, params: Dict[str, Any]) -> None:
        """
        Update flow control parameters dynamically.
        
        Args:
            params: Dictionary with updated parameters
                - For RATE_LIMITING: 'rate' (msgs/sec) and/or 'capacity'
                - For WINDOW_BASED: 'window_size' and/or 'max_window_size'
        """
        if self.flow_control_strategy == FlowControlStrategy.RATE_LIMITING and self.rate_limiter:
            if 'rate' in params:
                self.rate_limiter.rate = params['rate']
            if 'capacity' in params:
                self.rate_limiter.capacity = params['capacity']
                
        elif self.flow_control_strategy == FlowControlStrategy.WINDOW_BASED and self.window_control:
            if 'max_window_size' in params:
                self.window_control.max_window_size = params['max_window_size']
            if 'window_size' in params and params['window_size'] <= self.window_control.max_window_size:
                self.window_control.window_size = params['window_size']
                
        # Update stored parameters
        self.flow_control_params.update(params)
        logger.info(f"Updated flow control parameters: {params}")


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example message sending function
    def example_send(data, addr, transport):
        print(f"Sending message to {addr} via {transport}: {data}")
        return True
    
    print("\nQoS Module Example\n" + "="*20)
    
    # Create QoS manager with rate limiting
    qos = QoSManager(
        flow_control_strategy=FlowControlStrategy.RATE_LIMITING,
        flow_control_params={'rate': 10, 'capacity': 20}  # 10 messages per second
    )
    
    # Set callback function
    qos.set_send_callback(example_send)
    
    # Queue messages with different priorities
    print("\nQueuing messages with different priorities...")
    for i in range(10):
        # Alternate between different priority levels
        if i % 5 == 0:
            priority = PriorityLevel.CRITICAL
        elif i % 5 == 1:
            priority = PriorityLevel.HIGH
        elif i % 5 == 2:
            priority = PriorityLevel.NORMAL
        elif i % 5 == 3:
            priority = PriorityLevel.LOW
        else:
            priority = PriorityLevel.BULK
            
        message = f"Message {i}"
        addr = ("example.com", 8080)
        transport = "TCP"
        
        qos.queue_message((message, addr, transport), priority)
        print(f"  Queued: {message} with priority {priority.name}")
    
    # Let the QoS manager process the queue
    print("\nProcessing queue (messages will be sent in priority order)...")
    time.sleep(1.5)  # Give some time for processing
    
    # Update flow control parameters
    print("\nUpdating flow control parameters...")
    qos.update_flow_control_params({'rate': 20})  # Increase rate to 20 messages per second
    
    # Get statistics
    queue_stats = qos.get_queue_stats()
    flow_stats = qos.get_flow_control_stats()
    
    print("\nQoS Statistics:")
    print(f"  Queue: {queue_stats['queued_messages']} messages remaining")
    print(f"  Flow Control Strategy: {flow_stats['strategy']}")
    if 'rate' in flow_stats:
        print(f"  Rate: {flow_stats['rate']} messages/second")
        
    # Stop the QoS manager
    print("\nStopping QoS manager...")
    qos.stop()
    print("Example complete!") 