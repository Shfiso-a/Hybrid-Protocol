#!/usr/bin/env python3
"""
Compression module for the hybrid protocol library.

This module provides compression and decompression utilities for network data 
using various algorithms including zlib, gzip, lzma, and bz2. The library 
automatically selects the most efficient compression method based on data size.

Typical usage:
    from compression import Compression, CompressionType
    
    # Compress data using zlib
    compressed_data, comp_type = Compression.compress(data, CompressionType.ZLIB)
    
    # Decompress data
    original_data = Compression.decompress(compressed_data, comp_type)
"""

import zlib
import gzip
import lzma
import bz2
import logging
from typing import Tuple, Optional, Union, Any, Dict, ByteString

__version__ = '1.0.0'
__all__ = ['CompressionType', 'Compression']

logger = logging.getLogger("HybridProtocol.Compression")

class CompressionType:
    """Supported compression algorithms."""
    NONE = 'none'
    ZLIB = 'zlib'
    GZIP = 'gzip'
    LZMA = 'lzma'
    BZ2 = 'bz2'
    
    @classmethod
    def is_valid(cls, compression_type: str) -> bool:
        """
        Check if the given compression type is valid.
        
        Args:
            compression_type (str): Compression type to check
            
        Returns:
            bool: True if the compression type is valid, False otherwise
        """
        return compression_type in [cls.NONE, cls.ZLIB, cls.GZIP, cls.LZMA, cls.BZ2]

class Compression:
    """Provides compression and decompression functionality for the hybrid protocol."""
    
    def __init__(self):
        """Initialize a new compression instance with statistics tracking."""
        self.bytes_before_compression = 0
        self.bytes_after_compression = 0
        self.compression_operations = 0
        self.decompression_operations = 0
        self.compression_savings = 0
        
    def compress_data(self, data: bytes, compression_type: str = CompressionType.ZLIB, 
                   compression_level: int = 6) -> Tuple[bytes, str]:
        """
        Instance method to compress binary data and track statistics.
        
        Args:
            data (bytes): Data to compress
            compression_type (str): Type of compression to use
            compression_level (int): Compression level (1-9, with 9 being highest)
            
        Returns:
            tuple: (compressed_data, compression_type)
        """
        original_size = len(data)
        compressed_data, used_type = self.compress(data, compression_type, compression_level)
        compressed_size = len(compressed_data)
        
        # Update statistics
        self.bytes_before_compression += original_size
        self.bytes_after_compression += compressed_size
        self.compression_operations += 1
        
        # Calculate savings
        if original_size > 0:
            self.compression_savings += (original_size - compressed_size)
            
        return compressed_data, used_type
        
    def decompress_data(self, data: bytes, compression_type: str) -> bytes:
        """
        Instance method to decompress binary data and track statistics.
        
        Args:
            data (bytes): Compressed data
            compression_type (str): Type of compression used
            
        Returns:
            bytes: Decompressed data
        """
        compressed_size = len(data)
        decompressed_data = self.decompress(data, compression_type)
        decompressed_size = len(decompressed_data)
        
        # Update statistics
        self.decompression_operations += 1
        
        return decompressed_data
        
    def get_stats(self) -> Dict[str, Any]:
        """
        Get compression statistics.
        
        Returns:
            dict: Dictionary with compression statistics
        """
        compression_ratio = 0
        if self.bytes_before_compression > 0:
            compression_ratio = self.bytes_after_compression / self.bytes_before_compression
            
        return {
            "bytes_before_compression": self.bytes_before_compression,
            "bytes_after_compression": self.bytes_after_compression,
            "compression_operations": self.compression_operations,
            "decompression_operations": self.decompression_operations,
            "compression_ratio": compression_ratio,
            "bytes_saved": self.compression_savings
        }
    
    @staticmethod
    def compress(data: bytes, compression_type: str = CompressionType.ZLIB, 
                 compression_level: int = 6) -> Tuple[bytes, str]:
        """
        Compress binary data using the specified algorithm.
        
        Args:
            data (bytes): Data to compress
            compression_type (str): Type of compression to use
            compression_level (int): Compression level (1-9, with 9 being highest)
            
        Returns:
            tuple: (compressed_data, compression_type)
            
        Raises:
            TypeError: If data is not bytes
        """
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")
            
        try:
            if compression_type == CompressionType.NONE:
                return data, CompressionType.NONE
                
            elif compression_type == CompressionType.ZLIB:
                compressed = zlib.compress(data, level=compression_level)
                
            elif compression_type == CompressionType.GZIP:
                compressed = gzip.compress(data, compresslevel=compression_level)
                
            elif compression_type == CompressionType.LZMA:
                compressed = lzma.compress(data, preset=compression_level)
                
            elif compression_type == CompressionType.BZ2:
                compressed = bz2.compress(data, compresslevel=compression_level)
                
            else:
                logger.warning(f"Unknown compression type: {compression_type}, using ZLIB")
                compressed = zlib.compress(data, level=compression_level)
                compression_type = CompressionType.ZLIB
                
            # Only use compression if it actually reduces size
            if len(compressed) < len(data):
                return compressed, compression_type
            else:
                logger.debug("Compression didn't reduce size, using uncompressed data")
                return data, CompressionType.NONE
                
        except Exception as e:
            logger.error(f"Compression error: {e}")
            return data, CompressionType.NONE
    
    @staticmethod
    def decompress(data: bytes, compression_type: str) -> bytes:
        """
        Decompress data with the specified algorithm.
        
        Args:
            data (bytes): Compressed data
            compression_type (str): Type of compression used
            
        Returns:
            bytes: Decompressed data
            
        Raises:
            TypeError: If data is not bytes
            ValueError: If compression_type is unknown
            Exception: If decompression fails
        """
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")
            
        if compression_type == CompressionType.NONE:
            return data
            
        try:
            if compression_type == CompressionType.ZLIB:
                return zlib.decompress(data)
                
            elif compression_type == CompressionType.GZIP:
                return gzip.decompress(data)
                
            elif compression_type == CompressionType.LZMA:
                return lzma.decompress(data)
                
            elif compression_type == CompressionType.BZ2:
                return bz2.decompress(data)
                
            else:
                logger.error(f"Unknown compression type: {compression_type}")
                raise ValueError(f"Unknown compression type: {compression_type}")
                
        except Exception as e:
            logger.error(f"Decompression error: {e}")
            raise e

    @staticmethod
    def get_available_types() -> list:
        """
        Returns a list of all available compression types.
        
        Returns:
            list: All available compression types
        """
        return [CompressionType.NONE, CompressionType.ZLIB, 
                CompressionType.GZIP, CompressionType.LZMA, 
                CompressionType.BZ2]

    @staticmethod
    def get_recommended_type() -> str:
        """
        Returns the recommended compression type for general use.
        
        Returns:
            str: Recommended compression type
        """
        return CompressionType.ZLIB
                
# Example usage
if __name__ == "__main__":
    import sys
    import os
    import time
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Generate test data (1MB of random-ish data)
    test_data = os.urandom(1024 * 1024)
    
    print(f"Original data size: {len(test_data)} bytes")
    
    # Test all compression types
    for comp_type in Compression.get_available_types():
        if comp_type == CompressionType.NONE:
            continue
            
        start_time = time.time()
        compressed, used_type = Compression.compress(test_data, comp_type)
        compress_time = time.time() - start_time
        
        start_time = time.time()
        decompressed = Compression.decompress(compressed, used_type)
        decompress_time = time.time() - start_time
        
        compression_ratio = len(compressed) / len(test_data) * 100
        
        print(f"{comp_type.upper()}:")
        print(f"  Compressed size: {len(compressed)} bytes ({compression_ratio:.2f}% of original)")
        print(f"  Compression time: {compress_time:.4f} seconds")
        print(f"  Decompression time: {decompress_time:.4f} seconds")
        print(f"  Data verified: {decompressed == test_data}")
        print() 