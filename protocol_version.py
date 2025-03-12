#!/usr/bin/env python3
"""
Protocol Version Module for Hybrid Protocol

This module provides version negotiation and compatibility management for the Hybrid Protocol.
It enables different versions of the protocol to communicate by:

- Tracking protocol versions of connected peers
- Negotiating common supported features
- Providing compatibility checks for specific features
- Handling version-dependent behavior through feature detection

The protocol follows semantic versioning (MAJOR.MINOR.PATCH) where:
- MAJOR version changes indicate incompatible API changes
- MINOR version changes add functionality in a backward-compatible manner
- PATCH version changes make backward-compatible bug fixes

Version: 1.0.0
"""

import logging
import semver
from typing import Dict, Set, List, Tuple, Optional, Any, Union

__version__ = "1.0.0"
__all__ = ["ProtocolVersion"]

logger = logging.getLogger("HybridProtocol.Versioning")

class ProtocolVersion:
    """
    Manages protocol versioning and compatibility.
    
    This class ensures that different versions of the protocol can communicate
    by negotiating capabilities and handling backward compatibility. It tracks
    peer versions, determines common feature sets, and provides feature detection
    for version-specific behavior.
    """
    
    # Current protocol version (semantic versioning)
    CURRENT_VERSION = "1.0.0"
    
    # Features by version
    FEATURES: Dict[str, List[str]] = {
        "1.0.0": [
            "basic_messaging",
            "reliable_udp"
        ],
        "1.1.0": [
            "compression",
            "encryption"
        ],
        "1.2.0": [
            "heartbeat",
            "qos"
        ],
        "1.3.0": [
            "batching",
            "fragmentation"
        ],
        "1.4.0": [
            "pubsub"
        ],
        "1.5.0": [
            "transport_selection",
            "metrics"
        ],
        "2.0.0": [
            "connection_pooling",
            "streaming"
        ]
    }
    
    def __init__(self, version: Optional[str] = None):
        """
        Initialize with a specific protocol version.
        
        Args:
            version: Protocol version to use. Defaults to current version.
        """
        self.version: str = version or self.CURRENT_VERSION
        self.peer_versions: Dict[Tuple[str, int], str] = {}  # addr -> version
        self.feature_cache: Dict[Tuple[str, int], Set[str]] = {}  # addr -> set of features
        
    def get_version_info(self) -> Dict[str, Any]:
        """
        Get information about the current protocol version.
        
        Returns:
            Dictionary containing version information and supported features
        """
        return {
            "version": self.version,
            "features": list(self.get_supported_features(self.version))
        }
        
    def get_supported_features(self, version: Optional[str] = None) -> Set[str]:
        """
        Get the features supported by a specific version.
        
        Args:
            version: Version to check. Defaults to the current version.
            
        Returns:
            Set of supported feature names
        """
        version = version or self.version
        features: Set[str] = set()
        
        # Add features from all compatible versions
        for ver, ver_features in self.FEATURES.items():
            if semver.compare(ver, version) <= 0:  # version >= ver
                features.update(ver_features)
                
        return features
        
    def set_peer_version(self, addr: Tuple[str, int], version: str) -> Set[str]:
        """
        Set the protocol version for a peer.
        
        Args:
            addr: Peer address as (host, port) tuple
            version: Peer's protocol version
            
        Returns:
            Common supported features between local and peer versions
        """
        self.peer_versions[addr] = version
        features = self.get_common_features(addr)
        self.feature_cache[addr] = features
        
        return features
        
    def get_peer_version(self, addr: Tuple[str, int]) -> Optional[str]:
        """
        Get the protocol version for a peer.
        
        Args:
            addr: Peer address as (host, port) tuple
            
        Returns:
            Peer's protocol version, or None if unknown
        """
        return self.peer_versions.get(addr)
        
    def get_common_features(self, addr: Tuple[str, int]) -> Set[str]:
        """
        Get the common supported features between this instance and a peer.
        
        Args:
            addr: Peer address as (host, port) tuple
            
        Returns:
            Common supported features as a set of feature names
        """
        # Return cached result if available
        if addr in self.feature_cache:
            return self.feature_cache[addr]
            
        # Get peer version
        peer_version = self.get_peer_version(addr)
        if not peer_version:
            # If peer version is unknown, assume minimum feature set
            return set(self.FEATURES["1.0.0"])
            
        # Get features supported by both sides
        our_features = self.get_supported_features()
        peer_features = self.get_supported_features(peer_version)
        
        common_features = our_features.intersection(peer_features)
        self.feature_cache[addr] = common_features
        
        return common_features
        
    def supports_feature(self, feature: str, addr: Optional[Tuple[str, int]] = None) -> bool:
        """
        Check if a specific feature is supported.
        
        Args:
            feature: Feature name to check
            addr: Peer address to check compatibility with.
                 If None, only checks local support.
                                   
        Returns:
            True if the feature is supported
        """
        if addr is None:
            # Check only local support
            return feature in self.get_supported_features()
        else:
            # Check if both sides support the feature
            return feature in self.get_common_features(addr)
            
    def generate_version_message(self) -> Dict[str, Any]:
        """
        Generate a version negotiation message.
        
        This message is sent to peers to establish version compatibility.
        
        Returns:
            Version negotiation message as a dictionary
        """
        return {
            "_version": True,
            "version": self.version,
            "features": list(self.get_supported_features())
        }
        
    def process_version_message(self, message: Dict[str, Any], 
                               sender_addr: Tuple[str, int]) -> bool:
        """
        Process a version negotiation message from a peer.
        
        Args:
            message: Version message to process
            sender_addr: Sender's address as (host, port) tuple
            
        Returns:
            True if this was a version message and was processed
        """
        if not message.get("_version"):
            return False
            
        peer_version = message.get("version")
        if not peer_version:
            logger.warning(f"Received invalid version message from {sender_addr}")
            return True
            
        logger.info(f"Peer {sender_addr} is using protocol version {peer_version}")
        common_features = self.set_peer_version(sender_addr, peer_version)
        
        logger.debug(f"Common features with {sender_addr}: {common_features}")
        
        return True
        
    def remove_peer(self, addr: Tuple[str, int]) -> None:
        """
        Remove a peer from the version tracking.
        
        Called when a peer disconnects to clean up resources.
        
        Args:
            addr: Peer address as (host, port) tuple
        """
        if addr in self.peer_versions:
            del self.peer_versions[addr]
        
        if addr in self.feature_cache:
            del self.feature_cache[addr]
            
    def is_compatible_with(self, peer_version: str) -> bool:
        """
        Check if the local version is compatible with a peer version.
        
        Args:
            peer_version: Version string to check compatibility with
            
        Returns:
            True if the versions are compatible
        """
        # In semantic versioning, major version changes indicate
        # incompatible API changes, so we check major version match
        our_major = semver.parse(self.version)['major']
        peer_major = semver.parse(peer_version)['major']
        
        return our_major == peer_major
        
    def get_peers(self) -> Dict[Tuple[str, int], str]:
        """
        Get all tracked peers and their versions.
        
        Returns:
            Dictionary mapping peer addresses to their protocol versions
        """
        return self.peer_versions.copy()
        
    def get_minimum_common_version(self, peers: Optional[List[Tuple[str, int]]] = None) -> str:
        """
        Get the minimum common version among peers.
        
        This is useful for determining what features can be used
        when broadcasting to multiple peers.
        
        Args:
            peers: List of peer addresses to check, or None for all tracked peers
            
        Returns:
            Minimum common version string
        """
        if not peers:
            peers = list(self.peer_versions.keys())
            
        if not peers:
            return self.version  # No peers, return our version
            
        versions = [self.peer_versions.get(peer, "1.0.0") for peer in peers]
        versions.append(self.version)  # Include our version
        
        # Sort versions with semver and return the minimum
        return sorted(versions, key=lambda v: semver.parse(v))[0]


# Example usage
def example_usage():
    """Demonstrate the usage of the ProtocolVersion class."""
    # Set up logging for the example
    logging.basicConfig(level=logging.INFO)
    
    # Create a protocol version instance using default version
    versioning = ProtocolVersion()
    
    # Get information about the current version
    version_info = versioning.get_version_info()
    print(f"Current version: {version_info['version']}")
    print(f"Supported features: {version_info['features']}")
    
    # Check if a specific feature is supported locally
    is_supported = versioning.supports_feature("compression")
    print(f"Compression supported: {is_supported}")
    
    # Simulate a peer connection
    peer_addr = ("192.168.1.100", 12345)
    peer_version = "1.1.0"
    
    # Set the peer's version
    common_features = versioning.set_peer_version(peer_addr, peer_version)
    print(f"Common features with peer: {common_features}")
    
    # Generate a version message to send to a peer
    version_message = versioning.generate_version_message()
    print(f"Version message: {version_message}")
    
    # Simulate receiving a version message from another peer
    incoming_message = {
        "_version": True,
        "version": "1.2.0",
        "features": ["basic_messaging", "reliable_udp", "compression", "encryption", "heartbeat", "qos"]
    }
    other_peer_addr = ("192.168.1.101", 12346)
    
    # Process the incoming version message
    is_version_message = versioning.process_version_message(incoming_message, other_peer_addr)
    print(f"Processed version message: {is_version_message}")
    
    # Check if a feature is supported with a specific peer
    is_qos_supported = versioning.supports_feature("qos", other_peer_addr)
    print(f"QoS supported with other peer: {is_qos_supported}")
    
    # Check version compatibility
    is_compatible = versioning.is_compatible_with("1.5.0")
    print(f"Compatible with version 1.5.0: {is_compatible}")
    
    # Get minimum common version among peers
    min_version = versioning.get_minimum_common_version()
    print(f"Minimum common version: {min_version}")

if __name__ == "__main__":
    example_usage() 