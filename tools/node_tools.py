"""
etcd Node Usage Collector Module
Collects and analyzes node usage metrics for master nodes with capacity information
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.etcd_config import get_config
from tools.ocp_promql_basequery import PrometheusBaseQuery
from tools.etcd_tools_utility import mcpToolsUtility


class nodeMetricsCollector:
    """Collector for node usage metrics"""
    
    def __init__(self, ocp_auth, prometheus_config: Dict[str, Any]):
        self.ocp_auth = ocp_auth
        self.prometheus_config = prometheus_config
        self.logger = logging.getLogger(__name__)
        self.config = get_config()
        self.utility = mcpToolsUtility(ocp_auth)
    
    def _build_prometheus_config(self) -> Dict[str, Any]:
        """Normalize and enrich Prometheus configuration.
        Ensures a base URL is present and attaches auth headers if available.
        """
        cfg: Dict[str, Any] = dict(self.prometheus_config or {})
        # Accept multiple possible keys and env var for flexibility
        url = cfg.get('url') or cfg.get('base_url') or os.getenv('PROMETHEUS_URL')
        if url:
            cfg['url'] = url.rstrip('/')
        else:
            return {}
        # Attach bearer token header from ocp_auth if provided and headers not explicitly set
        token: Optional[str] = None
        if isinstance(self.ocp_auth, dict):
            token = self.ocp_auth.get('token') or self.ocp_auth.get('bearer_token')
        else:
            token = getattr(self.ocp_auth, 'token', None) or getattr(self.ocp_auth, 'bearer_token', None)
        if token:
            headers = dict(cfg.get('headers', {}))
            headers.setdefault('Authorization', f'Bearer {token}')
            cfg['headers'] = headers
        return cfg
    
    def _get_node_pattern(self, nodes: List[str]) -> str:
        """Generate regex pattern for node names
        
        For Prometheus regex patterns, we don't escape dots because:
        1. Dots in regex match any character, which is fine for FQDNs
        2. Escaping creates parse errors in PromQL
        3. The alternation (|) provides specificity
        """
        if not nodes:
            return "^$"  # Match nothing
        
        # Don't escape dots - they work fine in PromQL regex patterns
        # Just join with pipe for alternation
        return '|'.join(nodes)
    
    def _calculate_time_series_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate avg and max from time series values"""
        if not values:
            return {'avg': 0.0, 'max': 0.0}
        return {
            'avg': round(sum(values) / len(values), 2),
            'max': round(max(values), 2)
        }
    
    async def _collect_node_memory_capacity(self, prom: PrometheusBaseQuery,
                                            master_nodes: List[str]) -> Dict[str, float]:
        """Collect total memory capacity for each node"""
        try:
            node_pattern = self._get_node_pattern(master_nodes)
            query = f'node_memory_MemTotal_bytes{{instance=~"{node_pattern}"}}'
            
            self.logger.debug(f"Querying node memory capacity: {query}")
            
            # Use instant query to get current capacity
            result = await prom.query_instant(query)
            
            if result['status'] != 'success':
                self.logger.warning(f"Failed to get memory capacity: {result.get('error')}")
                return {}
            
            raw_results = result.get('data', {}).get('result', [])
            
            # Extract capacity for each node
            capacities = {}
            for item in raw_results:
                instance = item.get('metric', {}).get('instance', 'unknown')
                value = item.get('value', [None, None])
                
                if len(value) >= 2:
                    try:
                        # Convert bytes to GB
                        capacity_bytes = float(value[1])
                        capacity_gb = round(capacity_bytes / (1024**3), 2)
                        capacities[instance] = capacity_gb
                        self.logger.info(f"Node {instance} memory capacity: {capacity_gb} GB")
                    except (ValueError, TypeError, IndexError) as e:
                        self.logger.warning(f"Failed to parse capacity for {instance}: {e}")
            
            return capacities
            
        except Exception as e:
            self.logger.error(f"Error collecting node memory capacity: {e}", exc_info=True)
            return {}
        
    async def collect_all_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Collect all node usage metrics for master and worker nodes"""
        try:
            self.logger.info("Starting node usage metrics collection")

            # Get master and worker nodes
            master_nodes = await self.utility.get_master_nodes()
            worker_nodes = await self.utility.get_worker_nodes()

            if not master_nodes and not worker_nodes:
                return {
                    'status': 'error',
                    'error': 'No master or worker nodes found',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }

            self.logger.info(f"Found {len(master_nodes)} master nodes: {master_nodes}")
            self.logger.info(f"Found {len(worker_nodes)} worker nodes: {worker_nodes}")

            # Calculate time range
            prom_config = self._build_prometheus_config()
            if not prom_config.get('url'):
                return {
                    'status': 'error',
                    'error': 'Prometheus URL not configured',
                    'hint': "Provide prometheus_config['url'] or set PROMETHEUS_URL env var",
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }

            async with PrometheusBaseQuery(prom_config) as prom:
                # Get time range
                start_time, end_time = prom._get_time_range(duration)
                start_str = prom._format_timestamp(start_time)
                end_str = prom._format_timestamp(end_time)

                self.logger.info(f"Querying metrics from {start_str} to {end_str}")

                async def collect_for_group(node_group: str, nodes: list[str]):
                    """Helper to collect metrics for a given node group"""
                    if not nodes:
                        return None

                    # Collect memory capacity
                    node_capacities = await self._collect_node_memory_capacity(prom, nodes)

                    # Collect metrics
                    cpu_usage = await self._collect_node_cpu_usage(prom, nodes, start_str, end_str)
                    memory_used = await self._collect_node_memory_used(
                        prom, nodes, start_str, end_str, node_capacities=node_capacities
                    )
                    memory_cache = await self._collect_node_memory_cache_buffer(
                        prom, nodes, start_str, end_str, node_capacities=node_capacities
                    )
                    cgroup_cpu = await self._collect_cgroup_cpu_usage(prom, nodes, start_str, end_str)
                    cgroup_rss = await self._collect_cgroup_rss_usage(prom, nodes, start_str, end_str)

                    return {
                        'node_group': node_group,
                        'total_nodes': len(nodes),
                        'node_capacities': {node: {'memory': capacity} for node, capacity in node_capacities.items()},
                        'metrics': {
                            'cpu_usage': cpu_usage,
                            'memory_used': memory_used,
                            'memory_cache_buffer': memory_cache,
                            'cgroup_cpu_usage': cgroup_cpu,
                            'cgroup_rss_usage': cgroup_rss
                        }
                    }

                # Collect for both groups
                master_metrics = await collect_for_group('master', master_nodes)
                worker_metrics = await collect_for_group('worker', worker_nodes)

            # Combine results
            return {
                'status': 'success',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'duration': duration,
                'time_range': {'start': start_str, 'end': end_str},
                'node_groups': {
                    'master': master_metrics,
                    'worker': worker_metrics
                }
            }

        except Exception as e:
            self.logger.error(f"Error collecting node usage metrics: {e}", exc_info=True)
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    async def _collect_node_cpu_usage(self, prom: PrometheusBaseQuery, 
                                     master_nodes: List[str], 
                                     start: str, end: str, 
                                     step: str = '15s') -> Dict[str, Any]:
        """Collect node CPU usage by mode for master nodes"""
        try:
            metric_config = self.config.get_metric_by_name('node_cpu_usage')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            # Build query with node pattern
            node_pattern = self._get_node_pattern(master_nodes)
            query = f'sum by (instance, mode)(irate(node_cpu_seconds_total{{instance=~"{node_pattern}",job=~".*"}}[5m])) * 100'
            
            self.logger.debug(f"Querying CPU usage: {query}")
            
            # Execute range query
            result = await prom.query_range(query, 
                                           datetime.fromisoformat(start.replace('Z', '+00:00')),
                                           datetime.fromisoformat(end.replace('Z', '+00:00')),
                                           step)
            
            if result['status'] != 'success':
                self.logger.error(f"Query failed: {result.get('error')}")
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            # Process raw results
            raw_results = result.get('data', {}).get('result', [])
            self.logger.info(f"Got {len(raw_results)} CPU series")
            
            # Group by instance to collect time series
            node_time_series = {}
            for item in raw_results:
                instance = item.get('metric', {}).get('instance', 'unknown')
                mode = item.get('metric', {}).get('mode', 'unknown')
                values = item.get('values', [])
                
                if instance not in node_time_series:
                    node_time_series[instance] = {'modes': {}}
                
                # Extract numeric values from time series
                mode_values = []
                for ts, val in values:
                    try:
                        mode_values.append(float(val))
                    except (ValueError, TypeError):
                        pass
                
                if mode_values:
                    node_time_series[instance]['modes'][mode] = mode_values
            
            # Calculate per-node statistics
            nodes_result = {}
            for instance, data in node_time_series.items():
                node_data = {'modes': {}, 'unit': 'percent'}
                
                # Calculate stats for each mode
                all_mode_values = []
                for mode, mode_values in data['modes'].items():
                    stats = self._calculate_time_series_stats(mode_values)
                    node_data['modes'][mode] = {
                        **stats,
                        'unit': 'percent'
                    }
                    all_mode_values.extend(mode_values)
                
                # Calculate total CPU usage across all modes
                total_stats = self._calculate_time_series_stats(all_mode_values)
                node_data['total'] = {
                    **total_stats,
                    'unit': 'percent'
                }
                
                nodes_result[instance] = node_data
                self.logger.info(f"Collected CPU usage for {instance}: {len(data['modes'])} modes")
            
            if not nodes_result:
                self.logger.warning("No CPU usage data collected for any nodes")
                return {
                    'status': 'partial',
                    'metric': 'node_cpu_usage',
                    'description': metric_config.get('description', 'CPU usage per node and mode'),
                    'nodes': {},
                    'warning': 'No data returned from Prometheus'
                }
            
            return {
                'status': 'success',
                'metric': 'node_cpu_usage',
                'description': metric_config.get('description', 'CPU usage per node and mode'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node CPU usage: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_node_memory_used(self, prom: PrometheusBaseQuery,
                                       master_nodes: List[str],
                                       start: str, end: str,
                                       step: str = '15s',
                                       node_capacities: Dict[str, float] = None) -> Dict[str, Any]:
        """Collect node memory used for master nodes with capacity information"""
        try:
            metric_config = self.config.get_metric_by_name('node_memory_used')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            if node_capacities is None:
                node_capacities = {}
            
            node_pattern = self._get_node_pattern(master_nodes)
            query = f'(node_memory_MemTotal_bytes{{instance=~"{node_pattern}"}} - (node_memory_MemFree_bytes{{instance=~"{node_pattern}"}} + node_memory_Buffers_bytes{{instance=~"{node_pattern}"}} + node_memory_Cached_bytes{{instance=~"{node_pattern}"}}))'
            
            self.logger.debug(f"Querying memory used: {query}")
            
            result = await prom.query_range(query,
                                           datetime.fromisoformat(start.replace('Z', '+00:00')),
                                           datetime.fromisoformat(end.replace('Z', '+00:00')),
                                           step)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            raw_results = result.get('data', {}).get('result', [])
            self.logger.info(f"Got {len(raw_results)} memory series")
            
            # Calculate per-node statistics
            nodes_result = {}
            for item in raw_results:
                instance = item.get('metric', {}).get('instance', 'unknown')
                values = item.get('values', [])
                
                # Extract numeric values
                numeric_values = []
                for ts, val in values:
                    try:
                        numeric_values.append(float(val))
                    except (ValueError, TypeError):
                        pass
                
                if numeric_values:
                    stats = self._calculate_time_series_stats(numeric_values)
                    # Convert bytes to GB
                    node_info = {
                        'avg': round(stats['avg'] / (1024**3), 2),
                        'max': round(stats['max'] / (1024**3), 2),
                        'unit': 'GB'
                    }
                    
                    # Add total_capacity if available
                    if instance in node_capacities:
                        node_info['total_capacity'] = node_capacities[instance]
                        self.logger.debug(f"Added capacity {node_capacities[instance]} GB for {instance}")
                    
                    nodes_result[instance] = node_info
                    self.logger.info(f"Collected memory for {instance}: {node_info['avg']} GB avg")
            
            return {
                'status': 'success',
                'metric': 'node_memory_used',
                'description': metric_config.get('description', 'Memory used per node'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node memory used: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_node_memory_cache_buffer(self, prom: PrometheusBaseQuery,
                                               master_nodes: List[str],
                                               start: str, end: str,
                                               step: str = '15s',
                                               node_capacities: Dict[str, float] = None) -> Dict[str, Any]:
        """Collect node memory cache and buffer for master nodes with capacity information"""
        try:
            metric_config = self.config.get_metric_by_name('node_memory_cache_buffer')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            if node_capacities is None:
                node_capacities = {}
            
            node_pattern = self._get_node_pattern(master_nodes)
            query = f'node_memory_Cached_bytes{{instance=~"{node_pattern}"}} + node_memory_Buffers_bytes{{instance=~"{node_pattern}"}}'
            
            self.logger.debug(f"Querying cache/buffer: {query}")
            
            result = await prom.query_range(query,
                                           datetime.fromisoformat(start.replace('Z', '+00:00')),
                                           datetime.fromisoformat(end.replace('Z', '+00:00')),
                                           step)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            raw_results = result.get('data', {}).get('result', [])
            
            # Calculate per-node statistics
            nodes_result = {}
            for item in raw_results:
                instance = item.get('metric', {}).get('instance', 'unknown')
                values = item.get('values', [])
                
                # Extract numeric values
                numeric_values = []
                for ts, val in values:
                    try:
                        numeric_values.append(float(val))
                    except (ValueError, TypeError):
                        pass
                
                if numeric_values:
                    stats = self._calculate_time_series_stats(numeric_values)
                    node_info = {
                        'avg': round(stats['avg'] / (1024**3), 2),
                        'max': round(stats['max'] / (1024**3), 2),
                        'unit': 'GB'
                    }
                    
                    # Add total_capacity if available
                    if instance in node_capacities:
                        node_info['total_capacity'] = node_capacities[instance]
                        self.logger.debug(f"Added capacity {node_capacities[instance]} GB for {instance}")
                    
                    nodes_result[instance] = node_info
            
            return {
                'status': 'success',
                'metric': 'node_memory_cache_buffer',
                'description': metric_config.get('description', 'Memory cache and buffer per node'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting node memory cache/buffer: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_cgroup_cpu_usage(self, prom: PrometheusBaseQuery,
                                       master_nodes: List[str],
                                       start: str, end: str,
                                       step: str = '15s') -> Dict[str, Any]:
        """Collect cgroup CPU usage for master nodes"""
        try:
            metric_config = self.config.get_metric_by_name('cgroup_cpu_usage')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            node_pattern = self._get_node_pattern(master_nodes)
            query = f'sum by (id, node) (rate(container_cpu_usage_seconds_total{{job=~".*", id=~"/system.slice|/system.slice/kubelet.service|/system.slice/ovs-vswitchd.service|/system.slice/crio.service|/system.slice/systemd-journald.service|/system.slice/ovsdb-server.service|/system.slice/systemd-udevd.service|/kubepods.slice", node=~"{node_pattern}"}}[5m])) * 100'
            
            self.logger.debug(f"Querying cgroup CPU: {query}")
            
            result = await prom.query_range(query,
                                           datetime.fromisoformat(start.replace('Z', '+00:00')),
                                           datetime.fromisoformat(end.replace('Z', '+00:00')),
                                           step)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            raw_results = result.get('data', {}).get('result', [])
            self.logger.info(f"Got {len(raw_results)} cgroup CPU series")
            
            # Group by node to collect time series
            node_time_series = {}
            for item in raw_results:
                node = item.get('metric', {}).get('node', 'unknown')
                cgroup_id = item.get('metric', {}).get('id', 'unknown')
                values = item.get('values', [])
                
                if node not in node_time_series:
                    node_time_series[node] = {'cgroups': {}}
                
                # Extract numeric values
                cgroup_values = []
                for ts, val in values:
                    try:
                        cgroup_values.append(float(val))
                    except (ValueError, TypeError):
                        pass
                
                if cgroup_values:
                    node_time_series[node]['cgroups'][cgroup_id] = cgroup_values
            
            # Calculate per-node statistics
            nodes_result = {}
            for node, data in node_time_series.items():
                node_data = {'cgroups': {}, 'unit': 'percent'}
                
                # Calculate stats for each cgroup
                all_cgroup_values = []
                for cgroup_id, cgroup_values in data['cgroups'].items():
                    stats = self._calculate_time_series_stats(cgroup_values)
                    # Extract cgroup name from ID
                    cgroup_name = cgroup_id.split('/')[-1] if '/' in cgroup_id else cgroup_id
                    node_data['cgroups'][cgroup_name] = {
                        **stats,
                        'unit': 'percent'
                    }
                    all_cgroup_values.extend(cgroup_values)
                
                # Calculate total across all cgroups
                total_stats = self._calculate_time_series_stats(all_cgroup_values)
                node_data['total'] = {
                    **total_stats,
                    'unit': 'percent'
                }
                
                nodes_result[node] = node_data
                self.logger.info(f"Collected cgroup CPU for {node}: {len(data['cgroups'])} cgroups")
            
            return {
                'status': 'success',
                'metric': 'cgroup_cpu_usage',
                'description': metric_config.get('description', 'Cgroup CPU usage per node'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting cgroup CPU usage: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_cgroup_rss_usage(self, prom: PrometheusBaseQuery,
                                       master_nodes: List[str],
                                       start: str, end: str,
                                       step: str = '15s') -> Dict[str, Any]:
        """Collect cgroup RSS memory usage for master nodes"""
        try:
            metric_config = self.config.get_metric_by_name('cgroup_rss_usage')
            if not metric_config:
                return {'status': 'error', 'error': 'Metric configuration not found'}
            
            node_pattern = self._get_node_pattern(master_nodes)
            query = f'sum by (id, node) (container_memory_rss{{job=~".*", id=~"/system.slice|/system.slice/kubelet.service|/system.slice/ovs-vswitchd.service|/system.slice/crio.service|/system.slice/systemd-journald.service|/system.slice/ovsdb-server.service|/system.slice/systemd-udevd.service|/kubepods.slice", node=~"{node_pattern}"}})'
            
            self.logger.debug(f"Querying cgroup RSS: {query}")
            
            result = await prom.query_range(query,
                                           datetime.fromisoformat(start.replace('Z', '+00:00')),
                                           datetime.fromisoformat(end.replace('Z', '+00:00')),
                                           step)
            
            if result['status'] != 'success':
                return {'status': 'error', 'error': result.get('error', 'Query failed')}
            
            raw_results = result.get('data', {}).get('result', [])
            self.logger.info(f"Got {len(raw_results)} cgroup RSS series")
            
            # Group by node to collect time series
            node_time_series = {}
            for item in raw_results:
                node = item.get('metric', {}).get('node', 'unknown')
                cgroup_id = item.get('metric', {}).get('id', 'unknown')
                values = item.get('values', [])
                
                if node not in node_time_series:
                    node_time_series[node] = {'cgroups': {}}
                
                # Extract numeric values
                cgroup_values = []
                for ts, val in values:
                    try:
                        cgroup_values.append(float(val))
                    except (ValueError, TypeError):
                        pass
                
                if cgroup_values:
                    node_time_series[node]['cgroups'][cgroup_id] = cgroup_values
            
            # Calculate per-node statistics
            nodes_result = {}
            for node, data in node_time_series.items():
                node_data = {'cgroups': {}, 'unit': 'GB'}
                
                # Calculate stats for each cgroup
                all_cgroup_values = []
                for cgroup_id, cgroup_values in data['cgroups'].items():
                    stats = self._calculate_time_series_stats(cgroup_values)
                    # Extract cgroup name from ID
                    cgroup_name = cgroup_id.split('/')[-1] if '/' in cgroup_id else cgroup_id
                    node_data['cgroups'][cgroup_name] = {
                        'avg': round(stats['avg'] / (1024**3), 2),
                        'max': round(stats['max'] / (1024**3), 2),
                        'unit': 'GB'
                    }
                    all_cgroup_values.extend(cgroup_values)
                
                # Calculate total across all cgroups
                total_stats = self._calculate_time_series_stats(all_cgroup_values)
                node_data['total'] = {
                    'avg': round(total_stats['avg'] / (1024**3), 2),
                    'max': round(total_stats['max'] / (1024**3), 2),
                    'unit': 'GB'
                }
                
                nodes_result[node] = node_data
                self.logger.info(f"Collected cgroup RSS for {node}: {len(data['cgroups'])} cgroups")
            
            return {
                'status': 'success',
                'metric': 'cgroup_rss_usage',
                'description': metric_config.get('description', 'Cgroup RSS usage per node'),
                'nodes': nodes_result
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting cgroup RSS usage: {e}", exc_info=True)
            return {'status': 'error', 'error': str(e)}


async def main():
    """Main function for testing"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # This is a placeholder - you would need to initialize with actual auth
    print("Node Usage Collector module loaded successfully")
    print("Usage: collector = nodeMetricsCollector(ocp_auth, prometheus_config)")
    print("       result = await collector.collect_all_metrics(duration='1h')")


if __name__ == "__main__":
    asyncio.run(main())