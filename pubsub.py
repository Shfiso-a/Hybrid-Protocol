#!/usr/bin/env python3
"""
Publish-Subscribe (PubSub) module for the hybrid protocol library.

This module provides a publish-subscribe messaging pattern implementation including:
- Topic-based message distribution: Publishers send messages to topics, not specific recipients
- Dynamic subscriptions: Clients can subscribe and unsubscribe from topics at runtime
- Flexible messaging: Support for both exact topic matching and pattern/wildcard matching
- Transport independence: Works with both TCP and UDP transports
- Publisher and subscriber isolation: Publishers don't need to know about subscribers

The publish-subscribe pattern allows for loosely coupled communication between
components, where senders (publishers) and receivers (subscribers) don't need to
be aware of each other, improving scalability and flexibility.

Typical usage:
    from pubsub import PubSubManager, SubscriptionType
    
    # Create PubSub manager
    pubsub = PubSubManager()
    pubsub.set_send_callback(lambda message, addr, transport: send_func(message, addr, transport))
    
    # Register subscribers
    subscriber_id = pubsub.add_subscriber(client_addr)
    
    # Subscribe to topics
    pubsub.subscribe("sensor/temperature", subscriber_id, "TCP")
    pubsub.subscribe("sensor/*", subscriber_id, "UDP", SubscriptionType.PATTERN)
    
    # Publish messages
    pubsub.publish("sensor/temperature", {"value": 22.5})
    
    # Process incoming subscribe/unsubscribe requests
    if pubsub.process_pubsub_message(message, sender_addr):
        # Message was a pubsub control message
        pass
"""

import time
import threading
import logging
import fnmatch
import uuid
from enum import Enum, auto
from typing import Dict, List, Tuple, Any, Callable, Optional, Union, Set

__version__ = '1.0.0'
__all__ = ['SubscriptionType', 'PubSubManager']

logger = logging.getLogger("HybridProtocol.PubSub")

class SubscriptionType(Enum):
    """
    Types of subscriptions supported by the PubSub system.
    
    - EXACT: Message is delivered only when the topic matches exactly
    - PATTERN: Message is delivered when the topic matches a wildcard pattern
      (e.g., "sensor/*" would match "sensor/temperature" and "sensor/humidity")
    """
    EXACT = auto()    # Exact topic match
    PATTERN = auto()  # Pattern/wildcard match

class PubSubManager:
    """
    Provides publish/subscribe functionality for the hybrid protocol.
    
    This class manages subscriptions, handles message routing, and provides
    methods for publishing messages and subscribing to topics. It supports
    both exact topic matching and pattern-based wildcards.
    """
    
    def __init__(self) -> None:
        """
        Initialize the publish/subscribe manager with empty subscription tables.
        """
        # Subscriptions: {topic: {subscriber_id: transport_type}}
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        
        # Pattern subscriptions: {subscriber_id: (pattern, transport_type)}
        self.pattern_subscriptions: Dict[str, Tuple[str, Any]] = {}
        
        # Subscriber info: {subscriber_id: addr}
        self.subscribers: Dict[str, Tuple[str, int]] = {}
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Callback for sending messages
        self.send_callback: Optional[Callable[[Dict[str, Any], Tuple[str, int], Any], None]] = None
        
    def set_send_callback(self, callback: Callable[[Dict[str, Any], Tuple[str, int], Any], None]) -> None:
        """
        Set the callback function for sending messages.
        
        Args:
            callback: Function that takes (message_data, addr, transport_type) as parameters
        """
        self.send_callback = callback
        
    def add_subscriber(self, addr: Tuple[str, int]) -> str:
        """
        Add a new subscriber.
        
        Args:
            addr: (host, port) tuple of the subscriber
            
        Returns:
            Unique subscriber ID (UUID)
        """
        subscriber_id = str(uuid.uuid4())
        
        with self.lock:
            self.subscribers[subscriber_id] = addr
            
        logger.debug(f"Added subscriber {subscriber_id} at {addr}")
        return subscriber_id
        
    def remove_subscriber(self, subscriber_id: str) -> bool:
        """
        Remove a subscriber and all their subscriptions.
        
        Args:
            subscriber_id: ID of the subscriber to remove
            
        Returns:
            True if the subscriber existed and was removed, False otherwise
        """
        with self.lock:
            if subscriber_id in self.subscribers:
                del self.subscribers[subscriber_id]
                
                # Remove from topic subscriptions
                for topic in list(self.subscriptions.keys()):
                    if subscriber_id in self.subscriptions[topic]:
                        del self.subscriptions[topic][subscriber_id]
                        if not self.subscriptions[topic]:
                            del self.subscriptions[topic]
                
                # Remove from pattern subscriptions
                if subscriber_id in self.pattern_subscriptions:
                    del self.pattern_subscriptions[subscriber_id]
                    
                logger.debug(f"Removed subscriber {subscriber_id}")
                return True
                
        return False
        
    def subscribe(self, topic: str, subscriber_id: str, transport_type: Any, 
                 subscription_type: SubscriptionType = SubscriptionType.EXACT) -> bool:
        """
        Subscribe to a topic or pattern.
        
        Args:
            topic: Topic or pattern to subscribe to
            subscriber_id: ID of the subscriber
            transport_type: Transport type (TCP/UDP) for receiving messages
            subscription_type: Type of subscription (exact match or pattern)
            
        Returns:
            True if successful, False if subscriber does not exist
        """
        if subscriber_id not in self.subscribers:
            logger.warning(f"Unknown subscriber {subscriber_id}")
            return False
            
        with self.lock:
            if subscription_type == SubscriptionType.EXACT:
                # Subscribe to exact topic
                if topic not in self.subscriptions:
                    self.subscriptions[topic] = {}
                    
                self.subscriptions[topic][subscriber_id] = transport_type
                logger.debug(f"Subscriber {subscriber_id} subscribed to topic {topic}")
                
            elif subscription_type == SubscriptionType.PATTERN:
                # Subscribe to pattern
                self.pattern_subscriptions[subscriber_id] = (topic, transport_type)
                logger.debug(f"Subscriber {subscriber_id} subscribed to pattern {topic}")
                
        return True
        
    def unsubscribe(self, topic: str, subscriber_id: str, 
                   subscription_type: SubscriptionType = SubscriptionType.EXACT) -> bool:
        """
        Unsubscribe from a topic or pattern.
        
        Args:
            topic: Topic or pattern to unsubscribe from
            subscriber_id: ID of the subscriber
            subscription_type: Type of subscription (exact match or pattern)
            
        Returns:
            True if successfully unsubscribed, False if not subscribed
        """
        with self.lock:
            if subscription_type == SubscriptionType.EXACT:
                # Unsubscribe from exact topic
                if topic in self.subscriptions and subscriber_id in self.subscriptions[topic]:
                    del self.subscriptions[topic][subscriber_id]
                    if not self.subscriptions[topic]:
                        del self.subscriptions[topic]
                    logger.debug(f"Subscriber {subscriber_id} unsubscribed from topic {topic}")
                    return True
                    
            elif subscription_type == SubscriptionType.PATTERN:
                # Unsubscribe from pattern
                if subscriber_id in self.pattern_subscriptions and self.pattern_subscriptions[subscriber_id][0] == topic:
                    del self.pattern_subscriptions[subscriber_id]
                    logger.debug(f"Subscriber {subscriber_id} unsubscribed from pattern {topic}")
                    return True
                    
        return False
        
    def publish(self, topic: str, message_data: Dict[str, Any], publisher_id: Optional[str] = None) -> int:
        """
        Publish a message to a topic.
        
        This sends the message to all subscribers of the exact topic
        and to all subscribers whose pattern matches the topic.
        
        Args:
            topic: Topic to publish to
            message_data: Message data (must be serializable)
            publisher_id: Optional ID of the publisher
            
        Returns:
            Number of subscribers the message was sent to
        """
        if not self.send_callback:
            return 0
            
        # Add pubsub metadata to message
        message = message_data.copy()
        message['_pubsub'] = True
        message['_topic'] = topic
        if publisher_id:
            message['_publisher'] = publisher_id
            
        sent_count = 0
        
        with self.lock:
            # Send to exact topic subscribers
            if topic in self.subscriptions:
                for subscriber_id, transport_type in self.subscriptions[topic].items():
                    if subscriber_id in self.subscribers:
                        try:
                            self.send_callback(message, self.subscribers[subscriber_id], transport_type)
                            sent_count += 1
                        except Exception as e:
                            logger.error(f"Error publishing to subscriber {subscriber_id}: {e}")
            
            # Send to pattern subscribers
            for subscriber_id, (pattern, transport_type) in self.pattern_subscriptions.items():
                if fnmatch.fnmatch(topic, pattern) and subscriber_id in self.subscribers:
                    try:
                        self.send_callback(message, self.subscribers[subscriber_id], transport_type)
                        sent_count += 1
                    except Exception as e:
                        logger.error(f"Error publishing to pattern subscriber {subscriber_id}: {e}")
                        
        logger.debug(f"Published message to topic {topic}, reached {sent_count} subscribers")
        return sent_count
        
    def process_pubsub_message(self, message: Dict[str, Any], sender_addr: Tuple[str, int]) -> bool:
        """
        Process a pub/sub control message (subscribe/unsubscribe/publish).
        
        This method handles incoming control messages from clients who want to
        perform pub/sub operations without calling the API methods directly.
        
        Args:
            message: Message data containing pub/sub commands
            sender_addr: Sender's address (host, port)
            
        Returns:
            True if this was a pub/sub control message and was processed, False otherwise
        """
        if not message.get('_pubsub_control'):
            return False
            
        # Handle different control messages
        command = message.get('command')
        
        if command == 'subscribe':
            # Subscribe request
            subscriber_id = message.get('subscriber_id')
            topic = message.get('topic')
            transport_type = message.get('transport_type')
            subscription_type = SubscriptionType.PATTERN if message.get('pattern', False) else SubscriptionType.EXACT
            
            if not all([subscriber_id, topic, transport_type]):
                logger.warning("Invalid subscribe request")
                return True
                
            # Make sure subscriber exists
            if subscriber_id not in self.subscribers:
                self.subscribers[subscriber_id] = sender_addr
                
            # Subscribe
            self.subscribe(topic, subscriber_id, transport_type, subscription_type)
            
        elif command == 'unsubscribe':
            # Unsubscribe request
            subscriber_id = message.get('subscriber_id')
            topic = message.get('topic')
            subscription_type = SubscriptionType.PATTERN if message.get('pattern', False) else SubscriptionType.EXACT
            
            if not all([subscriber_id, topic]):
                logger.warning("Invalid unsubscribe request")
                return True
                
            # Unsubscribe
            self.unsubscribe(topic, subscriber_id, subscription_type)
            
        elif command == 'publish':
            # Publish request
            topic = message.get('topic')
            payload = message.get('payload')
            publisher_id = message.get('publisher_id')
            
            if not all([topic, payload]):
                logger.warning("Invalid publish request")
                return True
                
            # Publish
            self.publish(topic, payload, publisher_id)
            
        return True
        
    def generate_subscribe_message(self, topic: str, subscriber_id: str, 
                                  transport_type: Any, is_pattern: bool = False) -> Dict[str, Any]:
        """
        Generate a subscribe control message.
        
        This creates a message that can be sent to the server to subscribe
        to a topic or pattern.
        
        Args:
            topic: Topic or pattern to subscribe to
            subscriber_id: ID of the subscriber
            transport_type: Transport type for receiving messages
            is_pattern: Whether this is a pattern subscription
            
        Returns:
            Subscribe control message in the correct format
        """
        return {
            '_pubsub_control': True,
            'command': 'subscribe',
            'subscriber_id': subscriber_id,
            'topic': topic,
            'transport_type': transport_type,
            'pattern': is_pattern
        }
        
    def generate_unsubscribe_message(self, topic: str, subscriber_id: str, 
                                    is_pattern: bool = False) -> Dict[str, Any]:
        """
        Generate an unsubscribe control message.
        
        This creates a message that can be sent to the server to unsubscribe
        from a topic or pattern.
        
        Args:
            topic: Topic or pattern to unsubscribe from
            subscriber_id: ID of the subscriber
            is_pattern: Whether this is a pattern subscription
            
        Returns:
            Unsubscribe control message in the correct format
        """
        return {
            '_pubsub_control': True,
            'command': 'unsubscribe',
            'subscriber_id': subscriber_id,
            'topic': topic,
            'pattern': is_pattern
        }
        
    def generate_publish_message(self, topic: str, payload: Dict[str, Any], 
                               publisher_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate a publish control message.
        
        This creates a message that can be sent to the server to publish
        a message to a topic.
        
        Args:
            topic: Topic to publish to
            payload: Message payload (must be serializable)
            publisher_id: Optional ID of the publisher
            
        Returns:
            Publish control message in the correct format
        """
        message = {
            '_pubsub_control': True,
            'command': 'publish',
            'topic': topic,
            'payload': payload
        }
        
        if publisher_id:
            message['publisher_id'] = publisher_id
            
        return message
    
    def get_subscription_stats(self) -> Dict[str, Any]:
        """
        Get statistics about subscriptions.
        
        Returns:
            Dictionary with subscription statistics
        """
        with self.lock:
            stats = {
                'subscriber_count': len(self.subscribers),
                'topic_count': len(self.subscriptions),
                'pattern_subscription_count': len(self.pattern_subscriptions),
                'subscribers': list(self.subscribers.keys()),
                'topics': list(self.subscriptions.keys()),
                'patterns': {
                    subscriber_id: pattern 
                    for subscriber_id, (pattern, _) in self.pattern_subscriptions.items()
                }
            }
            
            # Count total subscriptions
            total_subscriptions = 0
            for topic, subscribers in self.subscriptions.items():
                total_subscriptions += len(subscribers)
                
            stats['total_subscriptions'] = total_subscriptions + len(self.pattern_subscriptions)
            
            return stats
            
    def get_topics(self) -> List[str]:
        """
        Get a list of all active topics.
        
        Returns:
            List of topics with active subscriptions
        """
        with self.lock:
            return list(self.subscriptions.keys())
            
    def get_subscribers_for_topic(self, topic: str) -> Set[str]:
        """
        Get all subscribers for a specific topic.
        
        This returns only exact topic subscribers, not pattern subscribers.
        
        Args:
            topic: Topic to get subscribers for
            
        Returns:
            Set of subscriber IDs subscribed to the topic
        """
        with self.lock:
            if topic in self.subscriptions:
                return set(self.subscriptions[topic].keys())
            return set()


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example message sending function
    def example_send(message, addr, transport):
        print(f"MOCK: Sending message via {transport} to {addr}: {message}")
        
    print("\nPubSub Module Example\n" + "="*25)
    
    # Create PubSub manager
    pubsub = PubSubManager()
    pubsub.set_send_callback(example_send)
    
    # Register subscribers
    client1_addr = ("client1.example.com", 8080)
    client2_addr = ("client2.example.com", 8080)
    
    client1_id = pubsub.add_subscriber(client1_addr)
    client2_id = pubsub.add_subscriber(client2_addr)
    
    print(f"\nRegistered clients:\n- Client 1: {client1_id}\n- Client 2: {client2_id}")
    
    # Subscribe to topics
    pubsub.subscribe("sensors/temperature", client1_id, "TCP")
    pubsub.subscribe("sensors/humidity", client1_id, "TCP")
    pubsub.subscribe("sensors/*", client2_id, "UDP", SubscriptionType.PATTERN)
    
    print("\nSubscriptions:")
    stats = pubsub.get_subscription_stats()
    print(f"- Total subscribers: {stats['subscriber_count']}")
    print(f"- Total topics: {stats['topic_count']}")
    print(f"- Topics: {', '.join(stats['topics'])}")
    print(f"- Pattern subscriptions: {stats['pattern_subscription_count']}")
    
    # Publish messages
    print("\nPublishing messages:")
    print("- Temperature message")
    count = pubsub.publish("sensors/temperature", {"value": 22.5, "unit": "C"})
    print(f"  Reached {count} subscribers")
    
    print("- Humidity message")
    count = pubsub.publish("sensors/humidity", {"value": 45, "unit": "%"})
    print(f"  Reached {count} subscribers")
    
    print("- Pressure message (matches pattern)")
    count = pubsub.publish("sensors/pressure", {"value": 1013, "unit": "hPa"})
    print(f"  Reached {count} subscribers")
    
    # Unsubscribe
    print("\nUnsubscribing client1 from temperature topic")
    pubsub.unsubscribe("sensors/temperature", client1_id)
    
    # Publish again
    print("\nPublishing temperature message again:")
    count = pubsub.publish("sensors/temperature", {"value": 23.0, "unit": "C"})
    print(f"  Reached {count} subscribers (should be fewer)")
    
    # Process a control message
    print("\nProcessing a control message (publish request):")
    control_msg = pubsub.generate_publish_message(
        "sensors/light", {"value": 1200, "unit": "lux"}, "external_publisher"
    )
    pubsub.process_pubsub_message(control_msg, ("external.example.com", 9090))
    
    # Get final stats
    print("\nFinal subscription stats:")
    stats = pubsub.get_subscription_stats()
    print(f"- Total subscriptions: {stats['total_subscriptions']}")
    print(f"- Topic subscriptions: {len(stats['topics'])}")
    print(f"- Pattern subscriptions: {stats['pattern_subscription_count']}")
    
    print("\nExample complete!") 