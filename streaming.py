#!/usr/bin/env python3
"""
Streaming Module for Hybrid Protocol

This module provides a streaming data transfer mechanism that allows continuous 
data flow between peers using the Hybrid Protocol. It offers reliable, 
bidirectional streaming capabilities over both TCP and UDP transports with 
features such as:

- Bidirectional streaming for both sending and receiving data
- Automatic sequence tracking and acknowledgment for reliability
- Configurable buffer sizes and backpressure handling strategies
- Pause, resume, and graceful termination of streams
- Stream health monitoring and statistics collection
- Support for multiple concurrent streams

The module consists of two main classes:
1. Stream: Represents an individual data stream
2. StreamManager: Manages multiple streams and their lifecycle

Version: 1.0.0
"""

import time
import threading
import logging
import queue
import json
import uuid
from enum import Enum, auto
from typing import Dict, Any, Optional, Callable, Tuple, List, Union, Set

__version__ = "1.0.0"
__all__ = [
    "StreamState", 
    "StreamDirection", 
    "BackpressureStrategy",
    "Stream", 
    "StreamManager"
]

logger = logging.getLogger("HybridProtocol.Streaming")

class StreamState(Enum):
    """States of a data stream."""
    CREATED = auto()    # Stream created but not started
    ACTIVE = auto()     # Stream is active and transferring data
    PAUSED = auto()     # Stream is temporarily paused
    CLOSING = auto()    # Stream is in the process of closing
    CLOSED = auto()     # Stream is closed
    ERROR = auto()      # Stream encountered an error

class StreamDirection(Enum):
    """Direction of a data stream."""
    SEND = auto()       # We are sending data
    RECEIVE = auto()    # We are receiving data
    BIDIRECTIONAL = auto()  # Both sending and receiving

class BackpressureStrategy(Enum):
    """
    Strategy for handling backpressure when buffers fill up.
    
    - BLOCK: Block the sender until buffer space is available (reliable, may cause blocking)
    - DROP: Drop new data if buffer is full (unreliable, never blocks)
    - SAMPLE: Keep only latest data by discarding older items (good for telemetry/sensors)
    """
    BLOCK = auto()      # Block the sender until buffer space is available
    DROP = auto()       # Drop new data if buffer is full
    SAMPLE = auto()     # Keep only latest data (for telemetry, sensors)

class Stream:
    """
    Represents a data stream for continuous data transfer.
    
    A Stream provides a reliable channel for continuous data transfer between two peers.
    It handles sequencing, acknowledgment, and flow control automatically.
    """
    
    def __init__(self, 
                 stream_id: str, 
                 direction: StreamDirection, 
                 peer_addr: Tuple[str, int], 
                 transport_type: Any,
                 buffer_size: int = 1000, 
                 backpressure: BackpressureStrategy = BackpressureStrategy.BLOCK):
        """
        Initialize a stream.
        
        Args:
            stream_id: Unique identifier for the stream
            direction: Direction of data flow
            peer_addr: (host, port) of the peer
            transport_type: Transport type to use
            buffer_size: Size of the data buffer
            backpressure: How to handle backpressure
        """
        self.stream_id = stream_id
        self.direction = direction
        self.peer_addr = peer_addr
        self.transport_type = transport_type
        self.buffer_size = buffer_size
        self.backpressure = backpressure
        self.state = StreamState.CREATED
        
        # Sender data
        self.send_queue: queue.Queue = queue.Queue(maxsize=buffer_size if backpressure == BackpressureStrategy.BLOCK else 0)
        self.send_sequence: int = 0
        
        # Receiver data
        self.receive_buffer: Dict[int, Any] = {}  # sequence -> data
        self.receive_callback: Optional[Callable[[str, Any], None]] = None
        self.receive_sequence: int = 0
        self.next_ack: int = 0
        self.last_ack_time: float = 0
        
        # Callbacks
        self.send_callback: Optional[Callable[[Dict[str, Any], Tuple[str, int], Any], None]] = None
        self.error_callback: Optional[Callable[[str, str], None]] = None
        self.close_callback: Optional[Callable[[str], None]] = None
        
        # Stats
        self.bytes_sent: int = 0
        self.bytes_received: int = 0
        self.packets_sent: int = 0
        self.packets_received: int = 0
        self.created_time: float = time.time()
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        
        # Thread
        self.thread: Optional[threading.Thread] = None
        self.running: bool = False
        
    def start(self) -> bool:
        """
        Start the stream.
        
        Returns:
            bool: True if the stream was started
        """
        if self.state != StreamState.CREATED:
            return False
            
        self.state = StreamState.ACTIVE
        self.start_time = time.time()
        self.running = True
        
        # Start the stream thread
        self.thread = threading.Thread(target=self._stream_thread)
        self.thread.daemon = True
        self.thread.start()
        
        logger.info(f"Stream {self.stream_id} started")
        return True
        
    def pause(self) -> bool:
        """
        Pause the stream.
        
        Returns:
            bool: True if the stream was paused
        """
        if self.state == StreamState.ACTIVE:
            self.state = StreamState.PAUSED
            logger.info(f"Stream {self.stream_id} paused")
            return True
        return False
        
    def resume(self) -> bool:
        """
        Resume a paused stream.
        
        Returns:
            bool: True if the stream was resumed
        """
        if self.state == StreamState.PAUSED:
            self.state = StreamState.ACTIVE
            logger.info(f"Stream {self.stream_id} resumed")
            return True
        return False
        
    def close(self) -> bool:
        """
        Close the stream.
        
        Returns:
            bool: True if the stream was closed
        """
        if self.state in [StreamState.ACTIVE, StreamState.PAUSED]:
            self.state = StreamState.CLOSING
            logger.info(f"Stream {self.stream_id} closing")
            return True
        return False
        
    def set_callbacks(self, 
                      send_callback: Optional[Callable[[Dict[str, Any], Tuple[str, int], Any], None]] = None, 
                      receive_callback: Optional[Callable[[str, Any], None]] = None, 
                      error_callback: Optional[Callable[[str, str], None]] = None, 
                      close_callback: Optional[Callable[[str], None]] = None) -> None:
        """
        Set callbacks for stream events.
        
        Args:
            send_callback: Called to send data
            receive_callback: Called when data is received
            error_callback: Called when an error occurs
            close_callback: Called when the stream is closed
        """
        self.send_callback = send_callback
        self.receive_callback = receive_callback
        self.error_callback = error_callback
        self.close_callback = close_callback
        
    def write(self, data: Any) -> bool:
        """
        Write data to the stream.
        
        Args:
            data: Data to write
            
        Returns:
            bool: True if the data was queued
        """
        if self.direction not in [StreamDirection.SEND, StreamDirection.BIDIRECTIONAL]:
            logger.error(f"Cannot write to stream {self.stream_id} with direction {self.direction}")
            return False
            
        if self.state not in [StreamState.ACTIVE, StreamState.PAUSED]:
            logger.error(f"Cannot write to stream {self.stream_id} in state {self.state}")
            return False
            
        try:
            if self.backpressure == BackpressureStrategy.BLOCK:
                # Block until space is available
                self.send_queue.put(data)
            elif self.backpressure == BackpressureStrategy.DROP:
                # Drop if queue is full
                if self.send_queue.full():
                    logger.warning(f"Stream {self.stream_id} send queue full, dropping data")
                    return False
                self.send_queue.put(data)
            elif self.backpressure == BackpressureStrategy.SAMPLE:
                # Replace old data with new data
                if self.send_queue.full():
                    try:
                        self.send_queue.get_nowait()
                    except queue.Empty:
                        pass
                self.send_queue.put(data)
                
            return True
        except Exception as e:
            logger.error(f"Error writing to stream {self.stream_id}: {e}")
            self._handle_error(e)
            return False
            
    def process_stream_data(self, message: Dict[str, Any]) -> bool:
        """
        Process incoming stream data.
        
        Args:
            message: Stream message
            
        Returns:
            bool: True if the message was processed
        """
        if self.direction not in [StreamDirection.RECEIVE, StreamDirection.BIDIRECTIONAL]:
            logger.error(f"Cannot receive on stream {self.stream_id} with direction {self.direction}")
            return False
            
        if self.state not in [StreamState.ACTIVE, StreamState.PAUSED]:
            logger.debug(f"Ignoring data for stream {self.stream_id} in state {self.state}")
            return False
            
        try:
            # Extract message data
            sequence = message.get('sequence')
            data = message.get('data')
            is_ack = message.get('ack', False)
            
            if is_ack:
                # This is an acknowledgment
                self._process_ack(message)
                return True
                
            if sequence is None or data is None:
                logger.warning(f"Invalid stream data message for {self.stream_id}")
                return False
                
            # Store the data in the receive buffer
            self.receive_buffer[sequence] = data
            self.packets_received += 1
            
            # Try to process sequential data
            self._process_received_data()
            
            # Send acknowledgment
            self._send_ack(sequence)
            
            return True
        except Exception as e:
            logger.error(f"Error processing stream data for {self.stream_id}: {e}")
            self._handle_error(e)
            return False
            
    def process_stream_control(self, message: Dict[str, Any]) -> bool:
        """
        Process stream control message.
        
        Args:
            message: Control message
            
        Returns:
            bool: True if the message was processed
        """
        try:
            command = message.get('command')
            
            if command == 'start':
                # Start the stream if in CREATED state
                if self.state == StreamState.CREATED:
                    self.start()
                    
            elif command == 'pause':
                # Pause the stream
                self.pause()
                
            elif command == 'resume':
                # Resume the stream
                self.resume()
                
            elif command == 'close':
                # Close the stream
                self.close()
                
            elif command == 'error':
                # Handle error
                error_msg = message.get('error', 'Unknown error')
                self._handle_error(Exception(error_msg))
                
            else:
                logger.warning(f"Unknown stream control command for {self.stream_id}: {command}")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error processing stream control for {self.stream_id}: {e}")
            self._handle_error(e)
            return False
            
    def _stream_thread(self) -> None:
        """Main stream processing thread."""
        last_activity_time = time.time()
        
        while self.running:
            try:
                if self.state == StreamState.ACTIVE:
                    # Only process if the stream is active
                    
                    # Send queued data if available
                    if self.direction in [StreamDirection.SEND, StreamDirection.BIDIRECTIONAL]:
                        self._send_queued_data()
                    
                    # Process received data
                    if self.direction in [StreamDirection.RECEIVE, StreamDirection.BIDIRECTIONAL]:
                        self._process_received_data()
                    
                    last_activity_time = time.time()
                    
                elif self.state == StreamState.CLOSING:
                    # Send any remaining data
                    self._flush_send_queue()
                    
                    # Send close notification
                    self._send_close()
                    
                    self.state = StreamState.CLOSED
                    self.end_time = time.time()
                    self.running = False
                    
                    # Call close callback
                    if self.close_callback:
                        try:
                            self.close_callback(self.stream_id)
                        except Exception as e:
                            logger.error(f"Error in stream close callback: {e}")
                    
                    logger.info(f"Stream {self.stream_id} closed")
                    break
                    
                elif self.state == StreamState.CLOSED or self.state == StreamState.ERROR:
                    # Stream is closed or in error state
                    self.running = False
                    break
                    
                # Check for inactivity
                if time.time() - last_activity_time > 60:  # 1 minute inactivity timeout
                    logger.warning(f"Stream {self.stream_id} inactive for too long, closing")
                    self._handle_error(Exception("Stream inactive"))
                    break
                    
            except Exception as e:
                logger.error(f"Error in stream thread for {self.stream_id}: {e}")
                self._handle_error(e)
                
            time.sleep(0.01)  # Small sleep to prevent CPU spinning
            
    def _send_queued_data(self) -> None:
        """Send data from the send queue."""
        if not self.send_callback:
            return
            
        try:
            while not self.send_queue.empty():
                # Get data from the queue
                data = self.send_queue.get_nowait()
                
                # Create stream data message
                message = {
                    '_stream': True,
                    'stream_id': self.stream_id,
                    'sequence': self.send_sequence,
                    'data': data
                }
                
                # Send the message
                self.send_callback(message, self.peer_addr, self.transport_type)
                
                # Update stats
                self.send_sequence += 1
                self.packets_sent += 1
                
                # Estimate bytes sent (approximate)
                try:
                    if isinstance(data, str):
                        self.bytes_sent += len(data.encode('utf-8'))
                    elif isinstance(data, bytes):
                        self.bytes_sent += len(data)
                    else:
                        # JSON payload
                        self.bytes_sent += len(json.dumps(data).encode('utf-8'))
                except:
                    pass
                
                # Avoid sending too much at once
                if self.send_sequence % 10 == 0:
                    break
                    
        except queue.Empty:
            pass  # Queue is empty
        except Exception as e:
            logger.error(f"Error sending queued data for stream {self.stream_id}: {e}")
            self._handle_error(e)
            
    def _flush_send_queue(self) -> None:
        """Flush any remaining data in the send queue."""
        if not self.send_callback:
            return
            
        try:
            while not self.send_queue.empty():
                # Get data from the queue
                data = self.send_queue.get_nowait()
                
                # Create stream data message
                message = {
                    '_stream': True,
                    'stream_id': self.stream_id,
                    'sequence': self.send_sequence,
                    'data': data
                }
                
                # Send the message
                self.send_callback(message, self.peer_addr, self.transport_type)
                
                # Update stats
                self.send_sequence += 1
                self.packets_sent += 1
                
        except queue.Empty:
            pass  # Queue is empty
        except Exception as e:
            logger.error(f"Error flushing send queue for stream {self.stream_id}: {e}")
            
    def _process_received_data(self) -> None:
        """Process sequentially received data."""
        if not self.receive_callback:
            return
            
        while self.receive_sequence in self.receive_buffer:
            # Get the next sequential data
            data = self.receive_buffer.pop(self.receive_sequence)
            
            # Estimate bytes received (approximate)
            try:
                if isinstance(data, str):
                    self.bytes_received += len(data.encode('utf-8'))
                elif isinstance(data, bytes):
                    self.bytes_received += len(data)
                else:
                    # JSON payload
                    self.bytes_received += len(json.dumps(data).encode('utf-8'))
            except:
                pass
                
            # Call the receive callback
            try:
                self.receive_callback(self.stream_id, data)
            except Exception as e:
                logger.error(f"Error in stream receive callback: {e}")
                
            # Update sequence
            self.receive_sequence += 1
            
    def _send_ack(self, sequence: int) -> None:
        """
        Send acknowledgment for received data.
        
        Args:
            sequence: Sequence number to acknowledge
        """
        if not self.send_callback:
            return
            
        now = time.time()
        
        # Don't send ACKs too frequently
        if sequence < self.next_ack and now - self.last_ack_time < 0.1:
            return
            
        try:
            # Create ACK message
            message = {
                '_stream': True,
                'stream_id': self.stream_id,
                'ack': True,
                'sequence': sequence
            }
            
            # Send the ACK
            self.send_callback(message, self.peer_addr, self.transport_type)
            
            # Update ACK tracking
            self.next_ack = sequence + 10  # Send every 10 messages
            self.last_ack_time = now
            
        except Exception as e:
            logger.error(f"Error sending ACK for stream {self.stream_id}: {e}")
            
    def _process_ack(self, message: Dict[str, Any]) -> None:
        """
        Process an acknowledgment message.
        
        Args:
            message: ACK message
        """
        # For now, we just log it. In a more complex implementation,
        # we might use ACKs for flow control or reliability.
        sequence = message.get('sequence')
        logger.debug(f"Received ACK for stream {self.stream_id}, sequence {sequence}")
        
    def _send_close(self) -> None:
        """Send a close message to the peer."""
        if not self.send_callback:
            return
            
        try:
            # Create close message
            message = {
                '_stream_control': True,
                'stream_id': self.stream_id,
                'command': 'close'
            }
            
            # Send the message
            self.send_callback(message, self.peer_addr, self.transport_type)
            
        except Exception as e:
            logger.error(f"Error sending close for stream {self.stream_id}: {e}")
            
    def _handle_error(self, error: Exception) -> None:
        """
        Handle a stream error.
        
        Args:
            error: The error that occurred
        """
        logger.error(f"Stream {self.stream_id} error: {error}")
        
        self.state = StreamState.ERROR
        
        # Call error callback
        if self.error_callback:
            try:
                self.error_callback(self.stream_id, str(error))
            except Exception as e:
                logger.error(f"Error in stream error callback: {e}")
                
        # Send error notification to peer
        if self.send_callback:
            try:
                message = {
                    '_stream_control': True,
                    'stream_id': self.stream_id,
                    'command': 'error',
                    'error': str(error)
                }
                self.send_callback(message, self.peer_addr, self.transport_type)
            except Exception as e:
                logger.error(f"Error sending error notification: {e}")
                
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics for the stream.
        
        Returns:
            Stream statistics
        """
        stats = {
            'stream_id': self.stream_id,
            'state': self.state.name,
            'direction': self.direction.name,
            'peer_addr': self.peer_addr,
            'transport_type': getattr(self.transport_type, 'name', str(self.transport_type)),
            'bytes_sent': self.bytes_sent,
            'bytes_received': self.bytes_received,
            'packets_sent': self.packets_sent,
            'packets_received': self.packets_received,
            'send_queue_size': self.send_queue.qsize(),
            'receive_buffer_size': len(self.receive_buffer),
            'created_time': self.created_time,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': (self.end_time or time.time()) - (self.start_time or self.created_time)
        }
        
        if self.bytes_sent > 0 and self.start_time:
            duration = (self.end_time or time.time()) - self.start_time
            if duration > 0:
                stats['send_rate_bytes_per_sec'] = self.bytes_sent / duration
                
        if self.bytes_received > 0 and self.start_time:
            duration = (self.end_time or time.time()) - self.start_time
            if duration > 0:
                stats['receive_rate_bytes_per_sec'] = self.bytes_received / duration
                
        return stats

class StreamManager:
    """
    Manages data streams for the hybrid protocol.
    
    The StreamManager provides functionality for:
    - Creating and tracking multiple streams
    - Handling stream message routing
    - Managing stream lifecycle (create, data transfer, close)
    - Collecting stream statistics
    """
    
    def __init__(self) -> None:
        """Initialize the stream manager."""
        # Stream storage
        self.streams: Dict[str, Stream] = {}  # stream_id -> Stream
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Callbacks
        self.send_callback: Optional[Callable[[Dict[str, Any], Tuple[str, int], Any], None]] = None
        self.data_callback: Optional[Callable[[str, Any], None]] = None
        
    def set_send_callback(self, callback: Callable[[Dict[str, Any], Tuple[str, int], Any], None]) -> None:
        """
        Set the callback for sending messages.
        
        Args:
            callback: Function for sending messages (message, addr, transport_type)
        """
        self.send_callback = callback
        
    def set_data_callback(self, callback: Callable[[str, Any], None]) -> None:
        """
        Set the callback for receiving stream data.
        
        Args:
            callback: Function to call with received data (stream_id, data)
        """
        self.data_callback = callback
        
    def create_stream(self, 
                     peer_addr: Tuple[str, int], 
                     transport_type: Any, 
                     direction: StreamDirection = StreamDirection.BIDIRECTIONAL,
                     buffer_size: int = 1000, 
                     backpressure: BackpressureStrategy = BackpressureStrategy.BLOCK,
                     stream_id: Optional[str] = None, 
                     auto_start: bool = True) -> Optional[str]:
        """
        Create a new data stream.
        
        Args:
            peer_addr: (host, port) of the peer
            transport_type: Transport type to use
            direction: Direction of data flow
            buffer_size: Size of the data buffer
            backpressure: How to handle backpressure
            stream_id: Specific stream ID to use
            auto_start: Whether to start the stream immediately
            
        Returns:
            Stream ID of the created stream, or None if creation failed
        """
        # Generate a stream ID if not provided
        if stream_id is None:
            stream_id = str(uuid.uuid4())
            
        with self.lock:
            # Check if a stream with this ID already exists
            if stream_id in self.streams:
                logger.warning(f"Stream {stream_id} already exists")
                return None
                
            # Create the stream
            stream = Stream(
                stream_id=stream_id,
                direction=direction,
                peer_addr=peer_addr,
                transport_type=transport_type,
                buffer_size=buffer_size,
                backpressure=backpressure
            )
            
            # Set callbacks
            stream.set_callbacks(
                send_callback=self.send_callback,
                receive_callback=self._handle_stream_data,
                error_callback=self._handle_stream_error,
                close_callback=self._handle_stream_close
            )
            
            # Store the stream
            self.streams[stream_id] = stream
            
            # Start the stream if requested
            if auto_start:
                stream.start()
                
            # Send stream creation notification
            if self.send_callback:
                message = {
                    '_stream_control': True,
                    'stream_id': stream_id,
                    'command': 'create',
                    'direction': direction.name
                }
                self.send_callback(message, peer_addr, transport_type)
                
            logger.info(f"Created stream {stream_id} to {peer_addr}")
            return stream_id
            
    def write_to_stream(self, stream_id: str, data: Any) -> bool:
        """
        Write data to a stream.
        
        Args:
            stream_id: Stream ID
            data: Data to write
            
        Returns:
            True if the data was written
        """
        with self.lock:
            if stream_id not in self.streams:
                logger.warning(f"Stream {stream_id} not found")
                return False
                
            stream = self.streams[stream_id]
            return stream.write(data)
            
    def close_stream(self, stream_id: str) -> bool:
        """
        Close a stream.
        
        Args:
            stream_id: Stream ID
            
        Returns:
            True if the stream was closed
        """
        with self.lock:
            if stream_id not in self.streams:
                logger.warning(f"Stream {stream_id} not found")
                return False
                
            stream = self.streams[stream_id]
            return stream.close()
            
    def process_stream_message(self, message: Dict[str, Any], sender_addr: Tuple[str, int]) -> bool:
        """
        Process a stream message.
        
        Args:
            message: Stream message
            sender_addr: Sender address
            
        Returns:
            True if the message was processed
        """
        if not message.get('_stream') and not message.get('_stream_control'):
            return False
            
        stream_id = message.get('stream_id')
        if stream_id is None:
            logger.warning(f"Stream message without stream_id from {sender_addr}")
            return False
            
        with self.lock:
            # Check if this is a control message
            if message.get('_stream_control'):
                return self._process_stream_control(message, sender_addr)
                
            # Process data message
            if stream_id in self.streams:
                # Existing stream - process data
                stream = self.streams[stream_id]
                return stream.process_stream_data(message)
            else:
                # Unknown stream - create it if it's an initial message
                logger.warning(f"Data for unknown stream {stream_id} from {sender_addr}")
                return False
                
    def _process_stream_control(self, message: Dict[str, Any], sender_addr: Tuple[str, int]) -> bool:
        """
        Process a stream control message.
        
        Args:
            message: Control message
            sender_addr: Sender address
            
        Returns:
            True if the message was processed
        """
        stream_id = message.get('stream_id')
        command = message.get('command')
        
        if command == 'create':
            # Request to create a new stream
            direction_name = message.get('direction', 'BIDIRECTIONAL')
            try:
                direction = StreamDirection[direction_name]
            except (KeyError, ValueError):
                direction = StreamDirection.BIDIRECTIONAL
                
            # Create the stream with the opposite direction
            if direction == StreamDirection.SEND:
                our_direction = StreamDirection.RECEIVE
            elif direction == StreamDirection.RECEIVE:
                our_direction = StreamDirection.SEND
            else:
                our_direction = StreamDirection.BIDIRECTIONAL
                
            # Get transport type from the message
            transport_type = message.get('transport_type', 'TCP')
            # This would need to be converted to your actual transport type enum
            
            # Create the stream
            self.create_stream(
                peer_addr=sender_addr,
                transport_type=transport_type,
                direction=our_direction,
                stream_id=stream_id
            )
            
            return True
            
        elif stream_id in self.streams:
            # Process control message for existing stream
            stream = self.streams[stream_id]
            return stream.process_stream_control(message)
            
        else:
            logger.warning(f"Control message for unknown stream {stream_id} from {sender_addr}")
            return False
            
    def _handle_stream_data(self, stream_id: str, data: Any) -> None:
        """
        Handle data received from a stream.
        
        Args:
            stream_id: Stream ID
            data: Received data
        """
        if self.data_callback:
            try:
                self.data_callback(stream_id, data)
            except Exception as e:
                logger.error(f"Error in data callback for stream {stream_id}: {e}")
        
    def _handle_stream_error(self, stream_id: str, error: str) -> None:
        """
        Handle a stream error.
        
        Args:
            stream_id: Stream ID
            error: Error message
        """
        logger.error(f"Stream {stream_id} error: {error}")
        
        with self.lock:
            if stream_id in self.streams:
                # Remove the stream after error
                stream = self.streams.pop(stream_id)
                # We don't close it here as it's already in error state
                
    def _handle_stream_close(self, stream_id: str) -> None:
        """
        Handle a stream closing.
        
        Args:
            stream_id: Stream ID
        """
        logger.info(f"Stream {stream_id} closed")
        
        with self.lock:
            if stream_id in self.streams:
                # Remove the stream from our mapping
                self.streams.pop(stream_id)
                
    def get_stream_stats(self, stream_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics for streams.
        
        Args:
            stream_id: Specific stream ID or None for all
            
        Returns:
            Stream statistics
        """
        with self.lock:
            if stream_id is not None:
                # Stats for a specific stream
                if stream_id in self.streams:
                    return self.streams[stream_id].get_stats()
                else:
                    return {"error": f"Stream {stream_id} not found"}
            else:
                # Stats for all streams
                stats = {
                    'total_streams': len(self.streams),
                    'active_streams': sum(1 for s in self.streams.values() if s.state == StreamState.ACTIVE),
                    'streams': {}
                }
                
                for sid, stream in self.streams.items():
                    stats['streams'][sid] = stream.get_stats()
                    
                return stats
                
    def pause_stream(self, stream_id: str) -> bool:
        """
        Pause a stream.
        
        Args:
            stream_id: Stream ID
            
        Returns:
            True if the stream was paused
        """
        with self.lock:
            if stream_id not in self.streams:
                logger.warning(f"Stream {stream_id} not found")
                return False
                
            stream = self.streams[stream_id]
            return stream.pause()
            
    def resume_stream(self, stream_id: str) -> bool:
        """
        Resume a paused stream.
        
        Args:
            stream_id: Stream ID
            
        Returns:
            True if the stream was resumed
        """
        with self.lock:
            if stream_id not in self.streams:
                logger.warning(f"Stream {stream_id} not found")
                return False
                
            stream = self.streams[stream_id]
            return stream.resume()
            
    def get_streams(self) -> List[str]:
        """
        Get list of active stream IDs.
        
        Returns:
            List of stream IDs
        """
        with self.lock:
            return list(self.streams.keys())


# Example usage
def example_usage():
    """Demonstrate the usage of the StreamManager and Stream classes."""
    # Setup logging for the example
    logging.basicConfig(level=logging.INFO)
    
    # Create a stream manager
    manager = StreamManager()
    
    # Set up a send callback (this would typically connect to your transport layer)
    def example_send(message, addr, transport_type):
        print(f"Sending message: {message} to {addr} via {transport_type}")
        # In a real implementation, this would actually send the message
    
    # Set up a data callback (this processes received stream data)
    def example_data_callback(stream_id, data):
        print(f"Received data on stream {stream_id}: {data}")
    
    # Set the callbacks
    manager.set_send_callback(example_send)
    manager.set_data_callback(example_data_callback)
    
    # Create a stream
    peer_addr = ('127.0.0.1', 12345)
    transport_type = "TCP"  # This would typically be a TransportType enum
    
    stream_id = manager.create_stream(
        peer_addr=peer_addr,
        transport_type=transport_type,
        direction=StreamDirection.BIDIRECTIONAL,
        buffer_size=100,
        backpressure=BackpressureStrategy.BLOCK
    )
    
    if stream_id:
        # Write some data to the stream
        manager.write_to_stream(stream_id, "Hello, world!")
        manager.write_to_stream(stream_id, {"type": "json_data", "value": 42})
        
        # Simulate receiving a stream message
        message = {
            '_stream': True,
            'stream_id': stream_id,
            'sequence': 0,
            'data': "Response from peer"
        }
        manager.process_stream_message(message, peer_addr)
        
        # Get stream statistics
        stats = manager.get_stream_stats(stream_id)
        print(f"Stream stats: {stats}")
        
        # Pause the stream
        manager.pause_stream(stream_id)
        
        # Resume the stream
        manager.resume_stream(stream_id)
        
        # Close the stream when done
        manager.close_stream(stream_id)
    
    # Get overall stream statistics
    stats = manager.get_stream_stats()
    print(f"Overall stats: {stats}")

if __name__ == "__main__":
    example_usage() 