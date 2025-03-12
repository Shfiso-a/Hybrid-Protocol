# Hybrid Network Protocol

A Python implementation of a custom network protocol that combines TCP and UDP for optimized communication with advanced features.

## Overview

This project implements a custom protocol that leverages both TCP and UDP transport protocols:

- **TCP** (Transmission Control Protocol) for reliable, connection-oriented, ordered communication
- **UDP** (User Datagram Protocol) for fast, connectionless communication with lower overhead

By combining these protocols in a single interface, applications can choose the most appropriate transport method depending on the specific needs of each message.

## Features

### Core Features
- **Dual Protocol Support**: Send messages over either TCP or UDP from the same interface
- **Reliable UDP**: Optional acknowledgment-based reliability for UDP messages
- **Message Typing**: Built-in message type system with handler registration
- **Client/Server Architecture**: Supports both server and client modes
- **Thread Safety**: Concurrent handling of connections and messages
- **JSON Serialization**: All messages are JSON-serialized for compatibility and readability

### Advanced Features
- **Compression**: Optional message compression to reduce bandwidth usage
- **Encryption**: Secure communication with AES-GCM encryption
- **SSL/TLS Support**: Secure connections with certificate verification
- **Connection Pooling**: Efficient connection management for high-traffic scenarios
- **Heartbeat Monitoring**: Automatic detection of connection health and failures
- **Message Batching**: Reduce overhead by combining small messages
- **Message Fragmentation**: Handle large messages by breaking them into smaller chunks
- **Metrics Collection**: Comprehensive performance monitoring and statistics
- **Quality of Service (QoS)**: Message prioritization and flow control
- **Publish-Subscribe Messaging**: Topic-based message distribution
- **Streaming**: Continuous data transfer with automatic sequencing
- **Intelligent Transport Selection**: Automatic selection of TCP vs UDP based on message characteristics
- **Protocol Versioning**: Backward compatibility and feature negotiation

## Installation

This project requires Python 3.6 or higher.

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/hybrid-protocol.git
   cd hybrid-protocol
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Dependencies

The project has the following core dependencies:
- cryptography >= 3.4.0
- semver >= 2.13.0
- pycryptodome >= 3.15.0

Optional dependencies for metrics visualization:
- matplotlib >= 3.4.0
- pandas >= 1.3.0

## Basic Usage

### Starting a Server

```python
from hybrid_protocol import HybridProtocol

# Create a server
server = HybridProtocol("0.0.0.0", 5000, 5001)  # TCP port 5000, UDP port 5001

# Register message handlers
def handle_chat_message(message, client_addr):
    print(f"Chat from {client_addr}: {message['content']}")
    # Echo back to the sender
    server.send_tcp({"type": "chat_response", "content": f"Echo: {message['content']}"}, client_addr)

# Register handler for message type "chat"
server.register_handler("chat", handle_chat_message)

# Start the server
server.start_server()

try:
    # Keep the main thread running
    while True:
        import time
        time.sleep(1)
except KeyboardInterrupt:
    # Gracefully stop the server when Ctrl+C is pressed
    server.stop()
```

### Connecting as a Client

```python
from hybrid_protocol import HybridProtocol

# Connect to server
client = HybridProtocol("localhost", 5000, 5001)  # Same TCP and UDP ports as server

# Register message handlers
def handle_chat_response(message, server_addr):
    print(f"Response from server: {message['content']}")

# Register handler for "chat_response" message type
client.register_handler("chat_response", handle_chat_response)

# Connect to the server
client.connect()

try:
    # Send a chat message over TCP (reliable)
    client.send_tcp({"type": "chat", "content": "Hello, server!"})
    
    # Send a quick status update over UDP (faster but unreliable)
    client.send_udp({"type": "status", "content": "Typing..."}, addr=("localhost", 5001))
    
    # Send a critical UDP message with reliability
    client.send_udp({"type": "important", "content": "Critical information"}, addr=("localhost", 5001))
    
    # Keep the main thread running
    while True:
        import time
        time.sleep(1)
except KeyboardInterrupt:
    # Gracefully disconnect when Ctrl+C is pressed
    client.stop()
```

## Advanced Usage

### Enabling Compression

```python
# Enable compression with default settings
protocol.configure_compression(enable=True)

# Enable compression with custom settings
protocol.configure_compression(
    enable=True,
    compression_type="zlib",
    compression_level=6,  # 0-9, higher = better compression but slower
    threshold=1024  # Only compress messages larger than 1KB
)

# Get compression statistics
stats = protocol.get_compression_stats()
print(f"Compression stats: {stats}")
```

### Enabling Encryption

```python
# Enable encryption with default settings (auto-generated key)
protocol.configure_encryption(enable=True)

# Enable encryption with custom settings
from hybrid_protocol import EncryptionType
protocol.configure_encryption(
    enable=True,
    encryption_key=b'your-32-byte-key-here--------------',  # 32 bytes for AES-256
    encryption_type=EncryptionType.AES_GCM  # Advanced encryption
)

# Get encryption statistics
stats = protocol.get_encryption_stats()
print(f"Encryption stats: {stats}")
```

### Enabling SSL/TLS

```python
# Enable SSL with default settings
protocol.configure_ssl(enable=True)

# Enable SSL with custom certificate files
protocol.configure_ssl(
    enable=True,
    cert_file="server.crt",
    key_file="server.key",
    ca_file="ca.crt",
    verify_peer=True
)
```

### Connection Pooling

```python
# Enable connection pooling with default settings
protocol.configure_connection_pooling(enable=True)

# Enable connection pooling with custom settings
protocol.configure_connection_pooling(
    enable=True,
    max_connections=20,
    connection_timeout=10.0,
    max_idle_time=600,
    max_lifetime=3600
)

# Get connection pool statistics
stats = protocol.get_connection_pool_stats()
print(f"Connection pool stats: {stats}")
```

### Heartbeat Monitoring

```python
# Enable heartbeat monitoring with default settings
protocol.configure_heartbeat(enable=True)

# Enable heartbeat monitoring with custom settings
protocol.configure_heartbeat(
    enable=True,
    heartbeat_interval=3.0,  # Send heartbeats every 3 seconds
    connection_timeout=10.0  # Consider connection lost after 10 seconds with no response
)

# Get heartbeat status
status = protocol.get_heartbeat_status()
print(f"Heartbeat status: {status}")
```

### Message Handling (Batching and Fragmentation)

```python
# Enable message handling with default settings
protocol.configure_message_handling(enable=True)

# Enable message handling with custom settings
protocol.configure_message_handling(
    enable=True,
    max_batch_size=16384,  # Maximum batch size in bytes
    max_batch_delay=0.1,   # Maximum delay before sending a batch (seconds)
    min_batch_messages=5,  # Minimum messages to trigger a batch
    mtu=1400,              # Maximum transmission unit for fragmentation
    fragment_timeout=30.0  # Time to keep fragments in buffer (seconds)
)

# Get message handling statistics
stats = protocol.get_message_handling_stats()
print(f"Message handling stats: {stats}")
```

### Metrics Collection

```python
# Enable metrics collection with default settings
protocol.configure_metrics(enable=True)

# Enable metrics collection with custom settings
protocol.configure_metrics(
    enable=True,
    metrics_dir="./metrics",      # Directory to store metrics
    csv_export=True,              # Export metrics as CSV files
    collection_interval=30.0,     # Collect metrics every 30 seconds
    retention_period=86400        # Keep metrics for 24 hours
)

# Get current metrics
metrics = protocol.get_metrics()
print(f"Current metrics: {metrics}")

# Log a summary of metrics
protocol.log_metrics_summary()
```

### Quality of Service (QoS)

```python
from hybrid_protocol import PriorityLevel, FlowControlStrategy

# Enable QoS with default settings
protocol.configure_qos(enable=True)

# Enable QoS with custom settings
protocol.configure_qos(
    enable=True,
    flow_control_strategy=FlowControlStrategy.RATE_LIMITING,
    flow_control_params={
        "rate_limit": 1000,  # Messages per second
        "burst_size": 100    # Burst allowance
    }
)

# Send a message with priority
protocol.send_tcp(
    {"type": "critical_command", "command": "shutdown"},
    priority=PriorityLevel.HIGH
)

# Get QoS statistics
stats = protocol.get_qos_stats()
print(f"QoS stats: {stats}")
```

### Publish-Subscribe Messaging

```python
from hybrid_protocol import TransportType, SubscriptionType

# Enable PubSub
protocol.configure_pubsub(enable=True)

# Add a subscriber
subscriber_id = protocol.add_subscriber(("192.168.1.100", 5000))

# Subscribe to topics
protocol.subscribe("sensors/temperature", subscriber_id, 
                  transport_type=TransportType.UDP,
                  subscription_type=SubscriptionType.EXACT)

# Subscribe with pattern matching
protocol.subscribe("sensors/*", subscriber_id,
                  subscription_type=SubscriptionType.PATTERN)

# Publish a message to a topic
protocol.publish("sensors/temperature", {"value": 23.5, "unit": "C"})

# Unsubscribe from a topic
protocol.unsubscribe("sensors/temperature", subscriber_id)

# Get subscribers for a topic
subscribers = protocol.get_subscribers_for_topic("sensors/temperature")
print(f"Subscribers: {subscribers}")

# Get PubSub statistics
stats = protocol.get_pubsub_stats()
print(f"PubSub stats: {stats}")
```

### Streaming Data Transfer

```python
from hybrid_protocol import StreamDirection, BackpressureStrategy, TransportType

# Enable streaming
protocol.configure_streaming(enable=True)

# Create a stream
stream_id = protocol.create_stream(
    peer_addr=("192.168.1.100", 5000),
    transport_type=TransportType.TCP,
    direction=StreamDirection.BIDIRECTIONAL,
    buffer_size=1000,
    backpressure=BackpressureStrategy.BLOCK
)

# Register a handler for stream data
def handle_stream_data(stream_id, data):
    print(f"Received on stream {stream_id}: {data}")

protocol.register_stream_handler(stream_id, handle_stream_data)

# Write data to the stream
protocol.write_to_stream(stream_id, "Hello, world!")
protocol.write_to_stream(stream_id, {"type": "sensor_data", "value": 23.5})

# Pause, resume, and close the stream
protocol.pause_stream(stream_id)
protocol.resume_stream(stream_id)
protocol.close_stream(stream_id)

# Get stream statistics
stats = protocol.get_stream_stats(stream_id)
print(f"Stream stats: {stats}")
```

### Intelligent Transport Selection

```python
from hybrid_protocol import TransportSelectionStrategy

# Enable transport selection
protocol.configure_transport_selection(
    enable=True,
    strategy=TransportSelectionStrategy.ADAPTIVE,
    size_threshold=4096  # Use TCP for messages larger than 4KB
)

# Send a message with automatic transport selection
protocol.send(
    {"type": "sensor_data", "readings": [1, 2, 3, 4, 5]},
    reliability_required=False
)

# Get transport statistics
stats = protocol.get_transport_stats()
print(f"Transport stats: {stats}")
```

### Protocol Versioning

```python
# Enable protocol versioning
protocol.configure_protocol_version(enable=True, version="1.0.0")

# Get protocol version information
info = protocol.get_protocol_version_info()
print(f"Protocol version: {info}")

# Check if a feature is supported
if protocol.supports_feature("encryption", peer_addr):
    # Use encryption for this peer
    protocol.send_tcp({"type": "secure_data", "content": "confidential"}, peer_addr)
```

## Protocol Design

### Message Format

All messages are JSON objects with at least a `type` field. Additional fields depend on the specific message type.

Example:
```json
{
  "type": "chat",
  "content": "Hello, everyone!",
  "sender": "Alice",
  "timestamp": 1623456789
}
```

### Transport Selection Guidelines

- **Use TCP for**:
  - Important messages that must be delivered
  - Messages requiring strict ordering
  - Larger data transfers
  - Authentication/login data
  
- **Use UDP for**:
  - Real-time updates where occasional loss is acceptable
  - High-frequency status updates
  - Time-sensitive data where latency matters more than reliability
  - Broadcast/multicast scenarios

- **Use Reliable UDP for**:
  - Messages that need delivery confirmation but can tolerate some delay
  - When you want delivery confirmation but don't need strict ordering

## Project Structure

The project is organized in a modular fashion:

- `hybrid_protocol.py` - Main protocol implementation and interface
- `compression.py` - Message compression functionality
- `encryption.py` - Message encryption and SSL support
- `connection_pool.py` - TCP connection pooling
- `heartbeat.py` - Connection health monitoring
- `message_handling.py` - Message batching and fragmentation
- `metrics.py` - Performance monitoring and statistics
- `qos.py` - Quality of Service features
- `pubsub.py` - Publish-Subscribe messaging
- `streaming.py` - Continuous data streaming
- `transport_selection.py` - Intelligent protocol selection
- `protocol_version.py` - Version negotiation and compatibility

## Example Use Cases

1. **Online Gaming**:
   - Use TCP for login, game state initialization, and critical game events
   - Use UDP for player position updates, non-critical animations
   - Use Reliable UDP for important game events where ordering isn't critical
   - Use Heartbeat for player connection monitoring
   - Use Streaming for voice chat

2. **IoT Platform**:
   - Use TCP for device registration and configuration
   - Use UDP for telemetry and sensor readings
   - Use Publish-Subscribe for distributing sensor data to multiple clients
   - Use QoS for prioritizing critical device commands
   - Use Protocol Versioning for handling different generations of devices

3. **Real-time Collaboration Application**:
   - Use TCP for document synchronization
   - Use PubSub for broadcasting user presence and activities
   - Use Compression for efficient document transfer
   - Use Encryption for securing sensitive content
   - Use Metrics for monitoring system performance

## Requirements

- Python 3.6 or higher
- Dependencies listed in requirements.txt

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Roadmap

Future planned features:
- WebSocket transport support
- QUIC protocol support
- Distributed mode with server clustering
- Interactive CLI for monitoring and administration
- Web-based dashboard for protocol metrics

## Running Tests

To run the comprehensive test suite:

```bash
python protocol_test.py
```

For specific test classes or individual tests:

```bash
# Run a specific test class
python -m unittest protocol_test.TestHybridProtocol

# Run a specific test method
python -m unittest protocol_test.TestHybridProtocol.test_basic_connection

# Run with verbose output
python -m unittest -v protocol_test
```

Tests are configured to use different ports to avoid conflicts:
- Basic tests use ports 5500 (TCP) and 5501 (UDP)
- Advanced tests use ports 5600 (TCP) and 5601 (UDP)

## Testing

The project includes a comprehensive test suite to ensure the reliability and correctness of the hybrid protocol implementation. The tests cover a wide range of features and scenarios, including:

- **Server and Client Operations**: Tests for starting the server and connecting clients.
- **TCP and UDP Communication**: Verification of message sending and receiving over both TCP and UDP.
- **Compression and Encryption**: Tests for optional message compression and secure communication.
- **Connection Pooling and Heartbeat Monitoring**: Ensures efficient connection management and connection health tracking.
- **Quality of Service (QoS)**: Tests for message prioritization and flow control.
- **Protocol Versioning and Transport Selection**: Verification of backward compatibility and intelligent transport decisions.
- **Advanced Messaging Features**: Tests for message batching, fragmentation, and publish-subscribe messaging.
- **Streaming and Metrics Collection**: Ensures continuous data transfer and performance monitoring.

To run the tests, use the following command:

```bash
python -m unittest test_hybrid_protocol.py
```

This will execute all the tests and report any issues encountered. The test suite is designed to be extensible, allowing for easy addition of new tests as the protocol evolves.

## Known Issues and Potential Bugs

This project is made newly so there is a lot of bugs
While the hybrid protocol implementation is designed to be robust and reliable, there are some known issues and potential bugs that users should be aware of:

1. **UDP Packet Loss**: Due to the nature of UDP, packet loss can occur, especially in unreliable network conditions. While the protocol supports reliable UDP, users should ensure that critical messages are sent with reliability enabled.

2. **High Latency in Large Message Fragmentation**: Fragmenting and reassembling large messages can introduce latency. Users should consider optimizing message sizes and using batching where possible.

3. **SSL/TLS Configuration Errors**: Incorrect SSL/TLS configuration can lead to connection failures. Ensure that certificate paths and verification settings are correctly configured.

4. **Concurrency Issues**: While the protocol is designed to be thread-safe, improper use of shared resources in custom handlers can lead to race conditions. Users should ensure thread safety in their implementations.

5. **Version Compatibility**: Protocol versioning is supported, but users should verify compatibility when integrating with older versions of the protocol.

6. **Resource Management**: In high-load scenarios, resource management (e.g., connection pooling, thread usage) should be monitored to prevent exhaustion.

Users are encouraged to report any additional bugs or issues they encounter to help improve the protocol's reliability and performance. 
