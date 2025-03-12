#!/usr/bin/env python3
"""
Encryption module for the hybrid protocol library.

This module provides encryption and SSL/TLS utilities for secure network communication,
supporting AES-GCM symmetric encryption and standard SSL/TLS implementations.

Typical usage:
    from encryption import EncryptionManager, SSLWrapper
    
    # Initialize encryption with a random key
    encryption = EncryptionManager()
    
    # Encrypt and decrypt data
    encrypted = encryption.encrypt(data)
    original = encryption.decrypt(encrypted)
    
    # Create an SSL context for a server
    ssl_context = SSLWrapper.create_ssl_context(
        cert_file="server.crt", 
        key_file="server.key", 
        server_side=True
    )
"""

import os
import ssl
import socket
import logging
from typing import Optional, Union, Any, Dict, Tuple, List, ByteString, cast
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import base64

__version__ = '1.0.0'
__all__ = ['EncryptionManager', 'SSLWrapper']

logger = logging.getLogger("HybridProtocol.Encryption")

class EncryptionType:
    """Supported encryption algorithms."""
    AES_GCM = 'aes-gcm'
    CHACHA20 = 'chacha20'  # Reserved for future use

class EncryptionManager:
    """Provides encryption functionality for the hybrid protocol."""
    
    def __init__(self, encryption_key: Optional[Union[str, bytes]] = None, 
                encryption_type: str = EncryptionType.AES_GCM):
        """
        Initialize the encryption manager.
        
        Args:
            encryption_key (str or bytes, optional): Secret key for symmetric encryption.
                                                  If None, a new random key will be generated.
            encryption_type (str): Encryption algorithm to use
            
        Raises:
            ValueError: If an invalid encryption type is provided
        """
        self.encryption_type = encryption_type
        
        if encryption_type != EncryptionType.AES_GCM:
            logger.warning(f"Unsupported encryption type: {encryption_type}. Using AES-GCM instead.")
            self.encryption_type = EncryptionType.AES_GCM
            
        if encryption_key is None:
            # Generate a random key
            salt = os.urandom(16)
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
                backend=default_backend()
            )
            self.encryption_key = kdf.derive(os.urandom(32))
        else:
            # Use provided key
            self.encryption_key = base64.b64decode(encryption_key) if isinstance(encryption_key, str) else encryption_key
            
        # Store the key in base64 format for transmission if needed
        self.encryption_key_b64 = base64.b64encode(self.encryption_key).decode('utf-8')
        
        # Track statistics
        self.bytes_encrypted = 0
        self.bytes_decrypted = 0
        self.encryption_operations = 0
        self.decryption_operations = 0
        
    def encrypt(self, data: bytes) -> bytes:
        """
        Encrypt data using AES-GCM.
        
        Args:
            data (bytes): Data to encrypt
            
        Returns:
            bytes: Encrypted data with IV and tag
            
        Raises:
            TypeError: If data is not bytes
            Exception: If encryption fails
        """
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")
            
        try:
            # Generate a random IV
            iv = os.urandom(12)
            
            # Create an encryptor
            encryptor = Cipher(
                algorithms.AES(self.encryption_key),
                modes.GCM(iv),
                backend=default_backend()
            ).encryptor()
            
            # Encrypt the data
            ciphertext = encryptor.update(data) + encryptor.finalize()
            
            # Update statistics
            self.bytes_encrypted += len(data)
            self.encryption_operations += 1
            
            # Return IV + tag + ciphertext
            return iv + encryptor.tag + ciphertext
            
        except Exception as e:
            logger.error(f"Encryption error: {e}")
            raise e
            
    def decrypt(self, data: bytes) -> bytes:
        """
        Decrypt data that was encrypted with AES-GCM.
        
        Args:
            data (bytes): Encrypted data with IV and tag
            
        Returns:
            bytes: Decrypted data
            
        Raises:
            TypeError: If data is not bytes
            ValueError: If data is too short to contain IV and tag
            Exception: If decryption fails
        """
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")
            
        if len(data) < 28:  # IV (12) + Tag (16)
            raise ValueError("Encrypted data is too short")
            
        try:
            # Extract IV and tag
            iv = data[:12]
            tag = data[12:28]
            ciphertext = data[28:]
            
            # Create a decryptor
            decryptor = Cipher(
                algorithms.AES(self.encryption_key),
                modes.GCM(iv, tag),
                backend=default_backend()
            ).decryptor()
            
            # Decrypt the data
            plaintext = decryptor.update(ciphertext) + decryptor.finalize()
            
            # Update statistics
            self.bytes_decrypted += len(plaintext)
            self.decryption_operations += 1
            
            return plaintext
            
        except Exception as e:
            logger.error(f"Decryption error: {e}")
            raise e
    
    def get_key_base64(self) -> str:
        """
        Get the encryption key as a base64 encoded string for storage or transmission.
        
        Returns:
            str: Base64 encoded encryption key
        """
        return self.encryption_key_b64
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get encryption usage statistics.
        
        Returns:
            dict: Dictionary with encryption statistics
        """
        return {
            'encryption_type': self.encryption_type,
            'bytes_encrypted': self.bytes_encrypted,
            'bytes_decrypted': self.bytes_decrypted,
            'encryption_operations': self.encryption_operations,
            'decryption_operations': self.decryption_operations
        }

class SSLWrapper:
    """Wrapper for SSL/TLS socket connections."""
    
    DEFAULT_CIPHERS = (
        'ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:'
        'ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:'
        'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:'
        'DHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256'
    )
    
    @staticmethod
    def create_ssl_context(cert_file: Optional[str] = None, 
                          key_file: Optional[str] = None, 
                          ca_file: Optional[str] = None, 
                          server_side: bool = False,
                          verify_peer: bool = True) -> ssl.SSLContext:
        """
        Create an SSL context for secure connections.
        
        Args:
            cert_file (str, optional): Path to the certificate file
            key_file (str, optional): Path to the private key file
            ca_file (str, optional): Path to the CA certificate file
            server_side (bool): Whether this is for a server
            verify_peer (bool): Whether to verify peer certificates
            
        Returns:
            ssl.SSLContext: Configured SSL context
        """
        context = ssl.create_default_context(
            ssl.Purpose.CLIENT_AUTH if server_side else ssl.Purpose.SERVER_AUTH
        )
        
        # Set secure cipher suite
        context.set_ciphers(SSLWrapper.DEFAULT_CIPHERS)
        
        # Protocol selection (TLS 1.2+)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.maximum_version = ssl.TLSVersion.TLSv1_3
        
        if server_side:
            if verify_peer:
                context.verify_mode = ssl.CERT_OPTIONAL
            else:
                context.verify_mode = ssl.CERT_NONE
                
            if cert_file and key_file:
                context.load_cert_chain(certfile=cert_file, keyfile=key_file)
        else:
            if not verify_peer:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                
        if ca_file:
            context.load_verify_locations(cafile=ca_file)
            
        return context
    
    @staticmethod
    def wrap_socket(sock: socket.socket, 
                   context: ssl.SSLContext, 
                   server_side: bool = False, 
                   hostname: Optional[str] = None,
                   verify_peer: bool = True,
                   ca_file: Optional[str] = None) -> ssl.SSLSocket:
        """
        Wrap a socket with SSL/TLS.
        
        Args:
            sock (socket.socket): Socket to wrap
            context (ssl.SSLContext): SSL context
            server_side (bool): Whether this is for a server
            hostname (str, optional): Hostname for verification (client-side)
            verify_peer (bool): Whether to verify peer certificates
            ca_file (str, optional): Path to the CA certificate file
            
        Returns:
            ssl.SSLSocket: SSL-wrapped socket
            
        Raises:
            ssl.SSLError: If SSL negotiation fails
            Exception: If socket wrapping fails for other reasons
        """
        try:
            # Apply any additional CA certificates if provided
            if ca_file and not server_side:
                context.load_verify_locations(cafile=ca_file)
            
            # Update verification mode if needed
            if not verify_peer:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            
            if server_side:
                return context.wrap_socket(sock, server_side=True)
            else:
                return context.wrap_socket(sock, server_hostname=hostname)
        except Exception as e:
            logger.error(f"SSL socket wrap error: {e}")
            raise e
    
    @staticmethod
    def get_certificate_info(ssl_socket: ssl.SSLSocket) -> Dict[str, Any]:
        """
        Get information about the peer's certificate.
        
        Args:
            ssl_socket (ssl.SSLSocket): The SSL socket connection
            
        Returns:
            dict: Certificate information (or None if no certificate)
        """
        cert = ssl_socket.getpeercert()
        if not cert:
            return {'has_cert': False}
            
        subject = dict(x[0] for x in cert['subject'])
        issuer = dict(x[0] for x in cert['issuer'])
        
        return {
            'has_cert': True,
            'subject': subject,
            'issuer': issuer,
            'version': cert.get('version'),
            'notBefore': cert.get('notBefore'),
            'notAfter': cert.get('notAfter')
        }

# Example usage
if __name__ == "__main__":
    import sys
    import time
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Generate test data
    test_data = os.urandom(1024)  # 1KB of random data
    print(f"Original data size: {len(test_data)} bytes")
    
    # Test AES encryption
    encryption = EncryptionManager()
    print(f"Generated encryption key (base64): {encryption.get_key_base64()}")
    
    # Encrypt data
    start_time = time.time()
    encrypted = encryption.encrypt(test_data)
    encrypt_time = time.time() - start_time
    
    # Decrypt data
    start_time = time.time()
    decrypted = encryption.decrypt(encrypted)
    decrypt_time = time.time() - start_time
    
    # Verify
    print(f"AES-GCM Encryption:")
    print(f"  Encrypted size: {len(encrypted)} bytes")
    print(f"  Encryption time: {encrypt_time:.4f} seconds")
    print(f"  Decryption time: {decrypt_time:.4f} seconds")
    print(f"  Data verified: {decrypted == test_data}")
    
    # Test SSL
    print("\nTesting SSL/TLS:")
    try:
        context = SSLWrapper.create_ssl_context(verify_peer=False)
        print("  Created SSL context successfully")
    except Exception as e:
        print(f"  SSL context creation failed: {e}") 