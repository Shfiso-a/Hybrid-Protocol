#!/usr/bin/env python3
"""
Metrics collection and monitoring module for the hybrid protocol library.

This module provides advanced metrics collection, tracking, and analysis capabilities including:
- Message metrics (sent/received counts, bytes transferred)
- Performance metrics (latency, throughput)
- Reliability metrics (success rates, retries)
- Connection metrics (current connections, total connections)
- Feature usage metrics (compression, encryption, batching, fragmentation)
- Time series data collection for trend analysis
- Export to JSON and CSV formats for external analysis

These metrics help monitor the performance, reliability, and efficiency of the hybrid
protocol in production environments and can be used for debugging, optimization,
and capacity planning.

Typical usage:
    from metrics import MetricsManager
    
    # Create and configure metrics manager
    metrics = MetricsManager(enabled=True, metrics_dir="/path/to/metrics")
    metrics.start()
    
    # Record metrics during operation
    metrics.record_message_sent(transport_type, message_size)
    metrics.record_message_received(transport_type, message_size)
    metrics.record_latency(transport_type, latency_seconds)
    
    # Get current metrics
    current_metrics = metrics.get_current_metrics()
    print(f"TCP Messages Sent: {current_metrics['messages_sent_tcp']}")
    
    # Log a summary of all metrics
    metrics.log_metrics_summary()
    
    # Stop metrics collection when done
    metrics.stop()
"""

import time
import threading
import logging
import json
import os
import collections
from datetime import datetime
from typing import Dict, Any, Optional, List, Deque, Union, Tuple

__version__ = '1.0.0'
__all__ = ['MetricsManager']

logger = logging.getLogger("HybridProtocol.Metrics")

class MetricsManager:
    """
    Collects and manages metrics for the hybrid protocol.
    
    This class provides comprehensive metrics collection for monitoring the performance,
    reliability, and efficiency of the hybrid protocol. It tracks message counts,
    bytes transferred, latency, connection stats, and feature usage.
    """
    
    def __init__(self, enabled: bool = True, metrics_dir: Optional[str] = None, 
                 csv_export: bool = False, collection_interval: float = 60.0, 
                 retention_period: int = 86400):
        """
        Initialize the metrics manager.
        
        Args:
            enabled (bool): Whether metrics collection is enabled
            metrics_dir (str, optional): Directory to store metrics data files
            csv_export (bool): Whether to export metrics to CSV in addition to JSON
            collection_interval (float): Interval between metric snapshots in seconds
            retention_period (int): How long to keep metrics data in seconds
        """
        self.enabled = enabled
        self.metrics_dir = metrics_dir
        self.csv_export = csv_export
        self.collection_interval = collection_interval
        self.retention_period = retention_period
        
        # Ensure metrics directory exists if specified
        if enabled and metrics_dir:
            os.makedirs(metrics_dir, exist_ok=True)
        
        # Initialize metric storage with default values
        self.metrics: Dict[str, Any] = {
            # Message metrics
            'messages_sent_tcp': 0,
            'messages_sent_udp': 0,
            'messages_received_tcp': 0,
            'messages_received_udp': 0,
            'bytes_sent_tcp': 0,
            'bytes_sent_udp': 0,
            'bytes_received_tcp': 0,
            'bytes_received_udp': 0,
            'message_errors': 0,
            
            # Reliability metrics
            'reliable_udp_messages_sent': 0,
            'reliable_udp_messages_acked': 0,
            'reliable_udp_retries': 0,
            'reliable_udp_timeouts': 0,
            
            # Performance metrics
            'tcp_latency_samples': collections.deque(maxlen=100),
            'udp_latency_samples': collections.deque(maxlen=100),
            
            # Connection metrics
            'tcp_connections_current': 0,
            'tcp_connections_total': 0,
            'connection_errors': 0,
            
            # Feature metrics
            'compression_bytes_saved': 0,
            'encrypted_messages': 0,
            'batched_messages': 0,
            'fragmented_messages': 0,
            'pubsub_messages': 0,
            
            # Time series data
            'time_series': collections.deque(maxlen=retention_period // collection_interval)
        }
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Collection thread
        self.running = False
        self.collection_thread: Optional[threading.Thread] = None
        
    def start(self) -> None:
        """
        Start the metrics collection process.
        
        This begins periodic collection of metrics snapshots and exports metrics
        to disk if a metrics directory is configured.
        """
        if not self.enabled:
            return
            
        self.running = True
        self.collection_thread = threading.Thread(target=self._collection_thread)
        self.collection_thread.daemon = True
        self.collection_thread.start()
        logger.info("Metrics collection started")
        
    def stop(self) -> None:
        """
        Stop the metrics collection process.
        
        This stops the collection thread and performs a final export of metrics
        if a metrics directory is configured.
        """
        self.running = False
        
        # Export final metrics
        if self.enabled and self.metrics_dir:
            self._export_metrics()
            
        logger.info("Metrics collection stopped")
        
    def record_message_sent(self, transport_type: Any, size_bytes: int) -> None:
        """
        Record a sent message.
        
        Args:
            transport_type: Transport type used (TCP or UDP)
            size_bytes (int): Size of the message in bytes
        """
        if not self.enabled:
            return
            
        with self.lock:
            if transport_type.name.lower() == 'tcp':
                self.metrics['messages_sent_tcp'] += 1
                self.metrics['bytes_sent_tcp'] += size_bytes
            else:
                self.metrics['messages_sent_udp'] += 1
                self.metrics['bytes_sent_udp'] += size_bytes
                
    def record_message_received(self, transport_type: Any, size_bytes: int) -> None:
        """
        Record a received message.
        
        Args:
            transport_type: Transport type used (TCP or UDP)
            size_bytes (int): Size of the message in bytes
        """
        if not self.enabled:
            return
            
        with self.lock:
            if transport_type.name.lower() == 'tcp':
                self.metrics['messages_received_tcp'] += 1
                self.metrics['bytes_received_tcp'] += size_bytes
            else:
                self.metrics['messages_received_udp'] += 1
                self.metrics['bytes_received_udp'] += size_bytes
                
    def record_message_error(self) -> None:
        """
        Record a message processing error.
        
        This increments the message error counter to track error rates.
        """
        if not self.enabled:
            return
            
        with self.lock:
            self.metrics['message_errors'] += 1
            
    def record_reliable_udp_metrics(self, metric: str, count: int = 1) -> None:
        """
        Record reliable UDP metrics.
        
        Args:
            metric (str): Metric name ('sent', 'acked', 'retry', or 'timeout')
            count (int): Number to increment by
        """
        if not self.enabled:
            return
            
        with self.lock:
            if metric == 'sent':
                self.metrics['reliable_udp_messages_sent'] += count
            elif metric == 'acked':
                self.metrics['reliable_udp_messages_acked'] += count
            elif metric == 'retry':
                self.metrics['reliable_udp_retries'] += count
            elif metric == 'timeout':
                self.metrics['reliable_udp_timeouts'] += count
                
    def record_latency(self, transport_type: Any, latency_seconds: float) -> None:
        """
        Record a latency measurement.
        
        Args:
            transport_type: Transport type used (TCP or UDP)
            latency_seconds (float): Measured latency in seconds
        """
        if not self.enabled:
            return
            
        with self.lock:
            if transport_type.name.lower() == 'tcp':
                self.metrics['tcp_latency_samples'].append(latency_seconds)
            else:
                self.metrics['udp_latency_samples'].append(latency_seconds)
                
    def record_connection_metrics(self, metric: str, count: int = 1) -> None:
        """
        Record connection metrics.
        
        Args:
            metric (str): Metric name ('current', 'new', or 'error')
            count (int): Number to increment by (or absolute value for 'current')
        """
        if not self.enabled:
            return
            
        with self.lock:
            if metric == 'current':
                self.metrics['tcp_connections_current'] = count  # Set absolute value
            elif metric == 'new':
                self.metrics['tcp_connections_total'] += count
            elif metric == 'error':
                self.metrics['connection_errors'] += count
                
    def record_feature_usage(self, feature: str, count: int = 1, bytes_saved: int = 0) -> None:
        """
        Record feature usage.
        
        Args:
            feature (str): Feature name ('compression', 'encryption', 'batching', 
                          'fragmentation', or 'pubsub')
            count (int): Number of times used
            bytes_saved (int): Bytes saved (only relevant for compression)
        """
        if not self.enabled:
            return
            
        with self.lock:
            if feature == 'compression':
                self.metrics['compression_bytes_saved'] += bytes_saved
            elif feature == 'encryption':
                self.metrics['encrypted_messages'] += count
            elif feature == 'batching':
                self.metrics['batched_messages'] += count
            elif feature == 'fragmentation':
                self.metrics['fragmented_messages'] += count
            elif feature == 'pubsub':
                self.metrics['pubsub_messages'] += count
                
    def get_current_metrics(self) -> Dict[str, Any]:
        """
        Get the current metrics.
        
        Returns a dictionary containing all current metrics with computed
        derived metrics like averages and success rates.
        
        Returns:
            dict: Current metrics with derived calculations
        """
        if not self.enabled:
            return {}
            
        with self.lock:
            # Compute derived metrics
            metrics = dict(self.metrics)
            
            # Calculate average latencies
            tcp_samples = list(metrics['tcp_latency_samples'])
            udp_samples = list(metrics['udp_latency_samples'])
            
            metrics['avg_tcp_latency'] = sum(tcp_samples) / len(tcp_samples) if tcp_samples else None
            metrics['avg_udp_latency'] = sum(udp_samples) / len(udp_samples) if udp_samples else None
            
            # Calculate reliable UDP success rate
            sent = metrics['reliable_udp_messages_sent']
            acked = metrics['reliable_udp_messages_acked']
            metrics['reliable_udp_success_rate'] = acked / sent if sent > 0 else 1.0
            
            # Remove deque objects that can't be easily serialized
            del metrics['tcp_latency_samples']
            del metrics['udp_latency_samples']
            del metrics['time_series']
            
            return metrics
            
    def _collection_thread(self) -> None:
        """
        Thread that periodically collects and stores metrics.
        
        This internal method runs as a background thread to periodically collect
        metrics snapshots and export them.
        """
        last_collection_time = time.time()
        
        while self.running:
            now = time.time()
            
            # Check if it's time to collect metrics
            if now - last_collection_time >= self.collection_interval:
                last_collection_time = now
                self._collect_metrics_snapshot()
                
                # Export metrics periodically
                if self.metrics_dir:
                    self._export_metrics()
                    
            time.sleep(1.0)  # Check every second
            
    def _collect_metrics_snapshot(self) -> None:
        """
        Collect a snapshot of the current metrics.
        
        This internal method takes a snapshot of current metrics and adds it
        to the time series data.
        """
        with self.lock:
            # Get current metrics
            snapshot = self.get_current_metrics()
            
            # Add timestamp
            snapshot['timestamp'] = time.time()
            snapshot['datetime'] = datetime.now().isoformat()
            
            # Add to time series
            self.metrics['time_series'].append(snapshot)
            
    def _export_metrics(self) -> None:
        """
        Export metrics to disk.
        
        This internal method exports metrics to JSON and optionally CSV files
        in the configured metrics directory.
        """
        if not self.metrics_dir:
            return
            
        try:
            # Export current metrics as JSON
            current_metrics = self.get_current_metrics()
            current_metrics['timestamp'] = time.time()
            current_metrics['datetime'] = datetime.now().isoformat()
            
            json_path = os.path.join(self.metrics_dir, 'current_metrics.json')
            with open(json_path, 'w') as f:
                json.dump(current_metrics, f, indent=2)
                
            # Export time series data
            with self.lock:
                time_series = list(self.metrics['time_series'])
                
            if time_series:
                time_series_path = os.path.join(self.metrics_dir, 'metrics_history.json')
                with open(time_series_path, 'w') as f:
                    json.dump(time_series, f, indent=2)
                    
            # Export as CSV if enabled
            if self.csv_export:
                self._export_csv()
                
        except Exception as e:
            logger.error(f"Error exporting metrics: {e}")
            
    def _export_csv(self) -> None:
        """
        Export metrics as CSV files.
        
        This internal method exports time series metrics data to a CSV file
        for easier analysis in spreadsheet applications.
        """
        if not self.metrics_dir:
            return
            
        try:
            # Export time series data as CSV
            with self.lock:
                time_series = list(self.metrics['time_series'])
                
            if not time_series:
                return
                
            # Determine all column names
            columns = set()
            for snapshot in time_series:
                columns.update(snapshot.keys())
                
            columns = sorted(list(columns))
            
            # Write CSV file
            csv_path = os.path.join(self.metrics_dir, 'metrics_history.csv')
            with open(csv_path, 'w') as f:
                # Write header
                f.write(','.join(columns) + '\n')
                
                # Write data rows
                for snapshot in time_series:
                    row = []
                    for col in columns:
                        value = snapshot.get(col, '')
                        row.append(str(value))
                    f.write(','.join(row) + '\n')
                    
        except Exception as e:
            logger.error(f"Error exporting CSV metrics: {e}")
            
    def log_metrics_summary(self) -> None:
        """
        Log a summary of the current metrics.
        
        This method logs a concise summary of the most important metrics
        to the logger for quick diagnostics.
        """
        if not self.enabled:
            return
            
        metrics = self.get_current_metrics()
        
        # Create summary
        summary = []
        summary.append(f"Messages - TCP: {metrics['messages_sent_tcp']}/{metrics['messages_received_tcp']} (sent/recv)")
        summary.append(f"Messages - UDP: {metrics['messages_sent_udp']}/{metrics['messages_received_udp']} (sent/recv)")
        summary.append(f"Bytes - TCP: {metrics['bytes_sent_tcp']}/{metrics['bytes_received_tcp']} (sent/recv)")
        summary.append(f"Bytes - UDP: {metrics['bytes_sent_udp']}/{metrics['bytes_received_udp']} (sent/recv)")
        
        if metrics['avg_tcp_latency'] is not None:
            summary.append(f"Avg. TCP latency: {metrics['avg_tcp_latency']*1000:.2f} ms")
        if metrics['avg_udp_latency'] is not None:
            summary.append(f"Avg. UDP latency: {metrics['avg_udp_latency']*1000:.2f} ms")
            
        summary.append(f"Reliable UDP: {metrics['reliable_udp_success_rate']*100:.1f}% success")
        summary.append(f"TCP connections: {metrics['tcp_connections_current']} current, {metrics['tcp_connections_total']} total")
        summary.append(f"Errors: {metrics['message_errors']} message, {metrics['connection_errors']} connection")
        summary.append(f"Compression saved: {metrics['compression_bytes_saved']} bytes")
        summary.append(f"Feature usage: {metrics['encrypted_messages']} encrypted, {metrics['batched_messages']} batched, "
                      f"{metrics['fragmented_messages']} fragmented, {metrics['pubsub_messages']} pubsub")
        
        logger.info("Metrics Summary:\n" + "\n".join(summary))

# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create a temp directory for metrics export
    import tempfile
    import shutil
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create metrics manager with export enabled
        metrics = MetricsManager(
            enabled=True,
            metrics_dir=temp_dir,
            csv_export=True,
            collection_interval=5.0,  # 5 second interval for demonstration
            retention_period=3600     # 1 hour retention
        )
        
        # Start metrics collection
        metrics.start()
        
        # Simulate some activity
        from enum import Enum, auto
        
        class DemoTransport(Enum):
            TCP = auto()
            UDP = auto()
            
        print("Simulating protocol activity for 15 seconds...")
        
        # Simulate for 15 seconds
        start_time = time.time()
        while time.time() - start_time < 15:
            # Simulate message sending
            metrics.record_message_sent(DemoTransport.TCP, 1024)
            metrics.record_message_sent(DemoTransport.UDP, 512)
            
            # Simulate message receiving
            metrics.record_message_received(DemoTransport.TCP, 2048)
            metrics.record_message_received(DemoTransport.UDP, 256)
            
            # Simulate latency
            metrics.record_latency(DemoTransport.TCP, 0.05)  # 50ms
            metrics.record_latency(DemoTransport.UDP, 0.02)  # 20ms
            
            # Simulate reliable UDP
            metrics.record_reliable_udp_metrics('sent', 5)
            metrics.record_reliable_udp_metrics('acked', 4)
            metrics.record_reliable_udp_metrics('retry', 2)
            
            # Simulate feature usage
            metrics.record_feature_usage('compression', 2, 512)
            metrics.record_feature_usage('encryption', 3)
            metrics.record_feature_usage('batching', 1)
            
            # Sleep a bit
            time.sleep(0.5)
        
        # Log summary
        metrics.log_metrics_summary()
        
        # Stop metrics collection
        metrics.stop()
        
        # Show where files were exported
        print(f"\nMetrics exported to: {temp_dir}")
        print("Files created:")
        for f in os.listdir(temp_dir):
            print(f"  - {f}")
            
    finally:
        # Clean up the temp directory
        shutil.rmtree(temp_dir) 