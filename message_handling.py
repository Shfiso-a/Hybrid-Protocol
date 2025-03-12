#!/usr/bin/env python3
"""
Message handling module for the hybrid protocol library.

This module provides advanced message handling capabilities including:
- Message batching: Combine multiple small messages into a single transmission
- Message fragmentation: Split large messages into smaller chunks for transmission
- Automatic reassembly: Reconstruct fragmented messages on the receiving end

These features improve network efficiency and enable handling messages of any size
regardless of transport layer limitations.

Typical usage:
    from message_handling import MessageBatcher, MessageFragmenter
    
    # Create and configure message batcher
    batcher = MessageBatcher(max_batch_size=1024*64)
    batcher.set_send_callback(lambda batch, addr, transport: send_func(batch, addr, transport))
    batcher.start()
    
    # Create and configure message fragmenter
    fragmenter = MessageFragmenter(mtu=1400)
    fragmenter.set_send_callback(lambda fragment, addr, transport: send_func(fragment, addr, transport))
    fragmenter.start()
    
    # When sending messages
    if not fragmenter.fragment_message(large_message, destination, transport_type):
        if not batcher.queue_message(large_message, destination, transport_type):
            # Send directly if not fragmented or batched
            send_func(large_message, destination, transport_type)
    
    # When receiving messages
    if not fragmenter.process_fragment(message, sender, transport, message_handler):
        if not batcher.process_batch(message, sender, transport, message_handler):
            # Process directly if not a fragment or batch
            message_handler(message, sender, transport)
"""

import time
import json
import uuid
import logging
import threading
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Callable, Optional, Union, DefaultDict

__version__ = '1.0.0'
__all__ = ['MessageBatcher', 'MessageFragmenter']

logger = logging.getLogger("HybridProtocol.MessageHandling")

class MessageBatcher:
    """
    Provides message batching functionality to reduce protocol overhead
    by combining multiple small messages into a single transmission.
    
    Message batching improves network efficiency by reducing header overhead
    when sending many small messages to the same destination.
    """
    
    def __init__(self, max_batch_size: int = 1024*1024, max_delay: float = 0.1, min_messages: int = 2):
        """
        Initialize the message batcher.
        
        Args:
            max_batch_size (int): Maximum combined size of messages in a batch in bytes
            max_delay (float): Maximum time to wait before sending a batch in seconds
            min_messages (int): Minimum number of messages to form a batch
        """
        self.max_batch_size = max_batch_size
        self.max_delay = max_delay
        self.min_messages = min_messages
        
        # Message queues by destination and transport
        self.message_queues: DefaultDict[Tuple[Any, Any], List[Tuple[Dict[str, Any], float]]] = defaultdict(list)
        
        # Locks for thread safety
        self.queue_lock = threading.Lock()
        
        # Callbacks
        self.send_callback: Optional[Callable[[Dict[str, Any], Any, Any], None]] = None
        
        # State
        self.running = False
        self.batcher_thread: Optional[threading.Thread] = None
        
    def set_send_callback(self, callback: Callable[[Dict[str, Any], Any, Any], None]) -> None:
        """
        Set the callback function to send batched messages.
        
        Args:
            callback: Function that takes (message_data, address, transport_type) as parameters
        """
        self.send_callback = callback
        
    def start(self) -> None:
        """
        Start the batcher thread.
        
        This begins the background thread that monitors message queues and
        sends batched messages when appropriate.
        """
        if self.running:
            return
            
        self.running = True
        self.batcher_thread = threading.Thread(target=self._batcher_thread)
        self.batcher_thread.daemon = True
        self.batcher_thread.start()
        logger.info("Message batcher started")
        
    def stop(self) -> None:
        """
        Stop the batcher thread.
        
        This stops the background thread and releases resources.
        Any unsent messages in the queues will not be sent.
        """
        self.running = False
        logger.info("Message batcher stopped")
        
    def queue_message(self, data: Dict[str, Any], addr: Any, transport_type: Any) -> bool:
        """
        Queue a message for batching.
        
        Args:
            data (dict): Message data
            addr (tuple): Destination address (e.g., IP and port)
            transport_type: Transport type (e.g., TCP or UDP)
            
        Returns:
            bool: True if the message was queued, False if it was too large and should be sent directly
        """
        # Skip batching for large messages
        message_size = len(json.dumps(data).encode('utf-8'))
        if message_size > self.max_batch_size // 2:
            logger.debug(f"Message too large for batching ({message_size} bytes), sending directly")
            return False
            
        queue_key = (addr, transport_type)
        
        with self.queue_lock:
            self.message_queues[queue_key].append((data, time.time()))
            
            # Send immediately if we've reached the minimum batch size and total size threshold
            queue = self.message_queues[queue_key]
            if len(queue) >= self.min_messages:
                current_size = sum(len(json.dumps(d).encode('utf-8')) for d, _ in queue)
                if current_size >= self.max_batch_size:
                    self._send_batch(queue_key)
                    
        return True
        
    def _batcher_thread(self) -> None:
        """Thread that checks for batches ready to send based on age."""
        while self.running:
            now = time.time()
            to_send = []
            
            with self.queue_lock:
                # Check each queue
                for queue_key, queue in list(self.message_queues.items()):
                    if not queue:
                        continue
                        
                    # Check if we have enough messages and they've waited long enough
                    if len(queue) >= self.min_messages:
                        oldest_timestamp = queue[0][1]
                        if now - oldest_timestamp >= self.max_delay:
                            to_send.append(queue_key)
                            
            # Send ready batches
            for queue_key in to_send:
                self._send_batch(queue_key)
                
            time.sleep(min(0.01, self.max_delay / 10))  # Sleep to avoid CPU spinning
            
    def _send_batch(self, queue_key: Tuple[Any, Any]) -> None:
        """
        Send a batch of messages.
        
        Args:
            queue_key: (addr, transport_type) tuple
        """
        if not self.send_callback:
            return
            
        addr, transport_type = queue_key
        
        with self.queue_lock:
            if queue_key not in self.message_queues or not self.message_queues[queue_key]:
                return
                
            # Get messages to batch
            messages = [data for data, _ in self.message_queues[queue_key]]
            self.message_queues[queue_key] = []
            
        if not messages:
            return
            
        # Batch the messages
        batch = {
            '_batch': True,
            'messages': messages
        }
        
        try:
            # Send the batch
            self.send_callback(batch, addr, transport_type)
            logger.debug(f"Sent batch of {len(messages)} messages ({len(json.dumps(batch).encode('utf-8'))} bytes) to {addr}")
        except Exception as e:
            logger.error(f"Error sending message batch: {e}")
            
    def process_batch(self, batch_data: Dict[str, Any], sender_addr: Any, 
                     transport_type: Any, message_handler: Callable[[Dict[str, Any], Any, Any], None]) -> bool:
        """
        Process a received message batch.
        
        Args:
            batch_data (dict): Batch message data
            sender_addr (tuple): Sender address
            transport_type: Transport type
            message_handler (callable): Function to handle individual messages
            
        Returns:
            bool: True if this was a batch message and was processed, False otherwise
        """
        if not isinstance(batch_data, dict) or not batch_data.get('_batch') or 'messages' not in batch_data:
            return False
            
        messages = batch_data['messages']
        logger.debug(f"Received batch of {len(messages)} messages from {sender_addr}")
        
        # Process each message in the batch
        for message in messages:
            try:
                message_handler(message, sender_addr, transport_type)
            except Exception as e:
                logger.error(f"Error processing message from batch: {e}")
                
        return True

class MessageFragmenter:
    """
    Provides message fragmentation functionality to handle messages
    larger than the maximum transmission unit (MTU).
    
    Message fragmentation enables sending messages of any size by breaking
    them into smaller chunks that fit within network MTU constraints, and
    automatically reassembling them on the receiving end.
    """
    
    def __init__(self, mtu: int = 1400, fragment_timeout: float = 30.0):
        """
        Initialize the message fragmenter.
        
        Args:
            mtu (int): Maximum transmission unit (payload size) in bytes
            fragment_timeout (float): Timeout for reassembling fragments in seconds
        """
        self.mtu = mtu
        self.fragment_timeout = fragment_timeout
        
        # Fragment buffers for reassembly
        # message_id -> {'fragments': {fragment_index: data}, 'timestamp': time, 'total_fragments': count}
        self.fragment_buffers: Dict[str, Dict[str, Any]] = {}
        
        # Lock for thread safety
        self.buffer_lock = threading.Lock()
        
        # Callback
        self.send_callback: Optional[Callable[[Dict[str, Any], Any, Any], None]] = None
        
        # Cleanup thread
        self.running = False
        self.cleanup_thread: Optional[threading.Thread] = None
        
    def set_send_callback(self, callback: Callable[[Dict[str, Any], Any, Any], None]) -> None:
        """
        Set the callback function to send message fragments.
        
        Args:
            callback: Function that takes (fragment_data, address, transport_type) as parameters
        """
        self.send_callback = callback
        
    def start(self) -> None:
        """
        Start the cleanup thread.
        
        This begins the background thread that removes stale fragment buffers
        to prevent memory leaks when fragments are lost.
        """
        if self.running:
            return
            
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_thread)
        self.cleanup_thread.daemon = True
        self.cleanup_thread.start()
        logger.info("Message fragmenter started")
        
    def stop(self) -> None:
        """
        Stop the cleanup thread.
        
        This stops the background thread and releases resources.
        Any incomplete message fragments will be lost.
        """
        self.running = False
        logger.info("Message fragmenter stopped")
        
    def fragment_message(self, data: Dict[str, Any], addr: Any, transport_type: Any) -> bool:
        """
        Fragment a large message into smaller chunks.
        
        Args:
            data (dict): Message data
            addr (tuple): Destination address
            transport_type: Transport type
            
        Returns:
            bool: True if the message was fragmented, False if it was small enough to send directly
        """
        if not self.send_callback:
            return False
            
        # Convert to JSON to check size
        message_json = json.dumps(data).encode('utf-8')
        message_size = len(message_json)
        
        # Check if fragmentation is needed
        if message_size <= self.mtu:
            return False
            
        # Generate a message ID for this fragmented message
        message_id = str(uuid.uuid4())
        
        # Calculate number of fragments needed
        payload_size = self.mtu - 200  # Leave room for fragment metadata
        total_fragments = (message_size + payload_size - 1) // payload_size
        
        logger.debug(f"Fragmenting message of size {message_size} bytes into {total_fragments} parts")
        
        # Split the message into fragments
        for i in range(total_fragments):
            start = i * payload_size
            end = min(start + payload_size, message_size)
            
            fragment_data = {
                '_fragment': True,
                '_message_id': message_id,
                '_fragment_index': i,
                '_total_fragments': total_fragments,
                '_payload': message_json[start:end].decode('utf-8')
            }
            
            try:
                # Send the fragment
                self.send_callback(fragment_data, addr, transport_type)
            except Exception as e:
                logger.error(f"Error sending message fragment: {e}")
                return False
                
        return True
        
    def process_fragment(self, fragment_data: Dict[str, Any], sender_addr: Any, 
                        transport_type: Any, message_handler: Callable[[Dict[str, Any], Any, Any], None]) -> bool:
        """
        Process a received message fragment.
        
        Args:
            fragment_data (dict): Fragment message data
            sender_addr (tuple): Sender address
            transport_type: Transport type
            message_handler (callable): Function to handle reassembled messages
            
        Returns:
            bool: True if this was a fragment and was processed, False otherwise
        """
        if not isinstance(fragment_data, dict) or not fragment_data.get('_fragment'):
            return False
            
        # Extract fragment information
        message_id = fragment_data.get('_message_id')
        fragment_index = fragment_data.get('_fragment_index')
        total_fragments = fragment_data.get('_total_fragments')
        payload = fragment_data.get('_payload')
        
        if not all([message_id, fragment_index is not None, total_fragments, payload]):
            logger.warning(f"Received invalid fragment from {sender_addr}")
            return True
            
        with self.buffer_lock:
            # Create buffer entry if it doesn't exist
            if message_id not in self.fragment_buffers:
                self.fragment_buffers[message_id] = {
                    'fragments': {},
                    'timestamp': time.time(),
                    'total_fragments': total_fragments
                }
                
            # Add this fragment to the buffer
            buffer = self.fragment_buffers[message_id]
            buffer['fragments'][fragment_index] = payload
            
            # Check if we have all fragments
            if len(buffer['fragments']) == buffer['total_fragments']:
                # Reassemble the message
                reassembled = self._reassemble_message(message_id)
                if reassembled:
                    try:
                        # Process the reassembled message
                        message_handler(reassembled, sender_addr, transport_type)
                    except Exception as e:
                        logger.error(f"Error processing reassembled message: {e}")
                
                # Remove the buffer entry
                del self.fragment_buffers[message_id]
                
        return True
        
    def _reassemble_message(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Reassemble a fragmented message from its parts.
        
        Args:
            message_id (str): ID of the fragmented message
            
        Returns:
            dict: Reassembled message data or None if reassembly failed
        """
        buffer = self.fragment_buffers.get(message_id)
        if not buffer:
            return None
            
        # Check if we have all fragments
        if len(buffer['fragments']) != buffer['total_fragments']:
            return None
            
        # Reassemble the fragments in order
        payload_parts = []
        for i in range(buffer['total_fragments']):
            if i not in buffer['fragments']:
                logger.warning(f"Missing fragment {i} for message {message_id}")
                return None
            payload_parts.append(buffer['fragments'][i])
            
        # Join and parse the JSON
        try:
            json_data = ''.join(payload_parts)
            return json.loads(json_data)
        except Exception as e:
            logger.error(f"Error reassembling message {message_id}: {e}")
            return None
            
    def _cleanup_thread(self) -> None:
        """Thread that cleans up stale fragment buffers."""
        while self.running:
            time.sleep(5)  # Check every 5 seconds
            
            now = time.time()
            with self.buffer_lock:
                # Find expired buffers
                expired = [
                    message_id for message_id, buffer in self.fragment_buffers.items()
                    if now - buffer['timestamp'] > self.fragment_timeout
                ]
                
                # Remove expired buffers
                for message_id in expired:
                    logger.warning(f"Fragment buffer for message {message_id} expired after {self.fragment_timeout}s")
                    del self.fragment_buffers[message_id]

    def get_fragmentation_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the current fragmentation state.
        
        Returns:
            dict: Statistics about fragment buffers
        """
        with self.buffer_lock:
            stats = {
                'active_buffers': len(self.fragment_buffers),
                'mtu': self.mtu,
                'fragment_timeout': self.fragment_timeout,
                'buffers': {}
            }
            
            for message_id, buffer in self.fragment_buffers.items():
                stats['buffers'][message_id] = {
                    'received_fragments': len(buffer['fragments']),
                    'total_fragments': buffer['total_fragments'],
                    'age': time.time() - buffer['timestamp']
                }
                
            return stats

# Example usage
if __name__ == "__main__":
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example message handling system
    class MessageSystem:
        def __init__(self):
            # Create batcher and fragmenter
            self.batcher = MessageBatcher(max_batch_size=64*1024, max_delay=0.1)
            self.fragmenter = MessageFragmenter(mtu=1400)
            
            # Set callbacks
            self.batcher.set_send_callback(self.send_message)
            self.fragmenter.set_send_callback(self.send_message)
            
            # Start components
            self.batcher.start()
            self.fragmenter.start()
            
        def send_message(self, data, addr, transport_type):
            print(f"MOCK: Sending message to {addr} via {transport_type}: {len(json.dumps(data).encode('utf-8'))} bytes")
            # In a real system, this would send the message using a transport protocol
            
            # Simulate message receipt for demonstration
            self.receive_message(data, addr, transport_type)
            
        def process_message(self, data, addr, transport_type):
            print(f"PROCESSED: Message from {addr}: {data.get('content', '(no content)')}")
            
        def receive_message(self, data, addr, transport_type):
            # Process received message
            if not self.fragmenter.process_fragment(data, addr, transport_type, self.process_message):
                if not self.batcher.process_batch(data, addr, transport_type, self.process_message):
                    self.process_message(data, addr, transport_type)
                    
        def send(self, data, addr, transport_type):
            # First try fragmentation for large messages
            if not self.fragmenter.fragment_message(data, addr, transport_type):
                # Then try batching for small messages
                if not self.batcher.queue_message(data, addr, transport_type):
                    # Send directly if neither applies
                    self.send_message(data, addr, transport_type)
                    
        def stop(self):
            self.batcher.stop()
            self.fragmenter.stop()
    
    # Demonstrate usage
    system = MessageSystem()
    
    try:
        print("\nSending a small message (should be batched):")
        small_message = {"type": "text", "content": "This is a small message"}
        system.send(small_message, ("example.com", 8080), "UDP")
        
        # Send more small messages to trigger batching
        for i in range(5):
            system.send({"type": "text", "content": f"Small message {i}"}, ("example.com", 8080), "UDP")
            
        print("\nSending a large message (should be fragmented):")
        # Create a large message that exceeds the MTU
        large_content = "X" * 5000  # 5KB of data
        large_message = {"type": "data", "content": large_content}
        system.send(large_message, ("example.com", 8080), "TCP")
        
        # Sleep to let batches process
        time.sleep(0.2)
        
        # Print fragmentation stats
        print("\nFragmentation stats:")
        stats = system.fragmenter.get_fragmentation_stats()
        print(f"  Active buffers: {stats['active_buffers']}")
        print(f"  MTU: {stats['mtu']} bytes")
        
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        system.stop() 