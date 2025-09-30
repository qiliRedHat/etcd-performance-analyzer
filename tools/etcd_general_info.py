"""
etcd General Info Collector
Collects general etcd cluster information and health metrics with node/pod-level details
"""

import asyncio
import logging
from typing import Dict, Any, List
from datetime import datetime
import pytz
from .ocp_promql_basequery import PrometheusBaseQuery
from config.etcd_config import get_config
from .etcd_tools_utility import mcpToolsUtility


class GeneralInfoCollector:
    """Collector for general etcd cluster information"""
    
    def __init__(self, ocp_auth):
        self.ocp_auth = ocp_auth
        self.logger = logging.getLogger(__name__)
        self.config = get_config()
        self.timezone = pytz.UTC
        self.tools_utility = mcpToolsUtility(ocp_auth)
    
    async def collect_metrics(self, duration: str = "1h") -> Dict[str, Any]:
        """Collect all general info metrics with node/pod level details"""
        try:
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                general_metrics = self.config.get_metrics_by_category('general_info')
                
                collected_data = {
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'duration': duration,
                    'category': 'general_info',
                    'pod_metrics': {}
                }
                
                # Collect each metric with pod-level breakdown
                for metric in general_metrics:
                    metric_name = metric['name']
                    metric_query = metric['expr']
                    
                    self.logger.info(f"Collecting metric: {metric_name}")
                    
                    try:
                        result = await prom.query_with_stats(metric_query, duration)
                        
                        # Process result based on metric type
                        if metric_name in ['apiserver_storage_objects_max_top20', 'cluster_usage_resources_sum_top20']:
                            # Special handling for resource-based metrics
                            resources = await self._process_resource_metrics(result)
                            collected_data['pod_metrics'][metric_name] = {
                                'title': metric.get('title', metric_name),
                                'unit': metric.get('unit', 'unknown'),
                                'query': metric_query,
                                'resources': resources
                            }
                        elif metric_name == 'cpu_io_utilization_iowait':
                            # Use the dedicated method that filters to master nodes only
                            masters_result = await self.get_cpu_io_utilization_iowait_per_node(duration)
                            node_metrics = {}
                            if isinstance(masters_result, dict):
                                node_metrics = masters_result.get('nodes', {})
                            collected_data['pod_metrics'][metric_name] = {
                                'title': metric.get('title', metric_name),
                                'unit': metric.get('unit', 'unknown'),
                                'query': metric_query,
                                'nodes': node_metrics,
                                'scope': 'masters_only'
                            }
                        else:
                            # Default pod-level processing
                            pod_metrics = await self._process_metric_for_pods(result, metric_name)
                            collected_data['pod_metrics'][metric_name] = {
                                'title': metric.get('title', metric_name),
                                'unit': metric.get('unit', 'unknown'),
                                'query': metric_query,
                                'pods': pod_metrics
                            }
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        collected_data['pod_metrics'][metric_name] = {
                            'title': metric.get('title', metric_name),
                            'unit': metric.get('unit', 'unknown'),
                            'query': metric_query,
                            'error': str(e)
                        }
                
                return {
                    'status': 'success',
                    'data': collected_data
                }
                
        except Exception as e:
            self.logger.error(f"Error collecting general info metrics: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def get_cpu_usage_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get CPU usage metrics by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_pods_cpu_usage', duration)
    
    async def get_memory_usage_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get memory usage metrics by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_pods_memory_usage', duration)
    
    async def get_db_space_used_percent_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get database space usage percentage by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_db_space_used_percent', duration)
    
    async def get_db_physical_size_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get database physical size by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_db_physical_size', duration)
    
    async def get_db_logical_size_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get database logical size by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_db_logical_size', duration)
    
    async def get_proposal_failure_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get proposal failure rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('proposal_failure_rate', duration)
    
    async def get_proposal_pending_total_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get pending proposals total by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('proposal_pending_total', duration)
    
    async def get_proposal_commit_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get proposal commit rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('proposal_commit_rate', duration)
    
    async def get_proposal_apply_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get proposal apply rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('proposal_apply_rate', duration)
    
    async def get_total_proposals_committed_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get total proposals committed by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('total_proposals_committed', duration)
    
    async def get_leader_changes_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get leader changes rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('leader_changes_rate', duration)
    
    async def get_etcd_has_leader_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get etcd has leader status by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_has_leader', duration)
    
    async def get_leader_elections_per_day_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get leader elections per day by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('leader_elections_per_day', duration)
    
    async def get_slow_applies_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get slow applies by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_slow_applies', duration)
    
    async def get_slow_read_indexes_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get slow read indexes by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_slow_read_indexes', duration)
    
    async def get_put_operations_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get PUT operations rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_mvcc_put_operations_rate', duration)
    
    async def get_delete_operations_rate_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get DELETE operations rate by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_mvcc_delete_operations_rate', duration)
    
    async def get_heartbeat_send_failures_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get heartbeat send failures by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_network_heartbeat_send_failures', duration)
    
    async def get_health_failures_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get health failures by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_server_health_failures', duration)
    
    async def get_total_keys_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get total keys by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('etcd_debugging_mvcc_total_keys', duration)
    
    async def get_compacted_keys_per_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get compacted keys by etcd pod with avg and max values"""
        return await self._get_single_metric_per_pod('debugging_mvcc_db_compacted_keys', duration)
    
    async def get_cpu_io_utilization_iowait_per_node(self, duration: str = "1h") -> Dict[str, Any]:
        """Get CPU IO utilization (iowait) by master node with avg and max values - only returns master nodes"""
        try:
            # Get master nodes first
            master_nodes = await self.tools_utility.get_master_nodes()
            if not master_nodes:
                return {
                    'status': 'error',
                    'error': 'No master nodes found in cluster',
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                # Build query specifically for master nodes
                # Create regex pattern for master node names
                master_pattern = '|'.join(master_nodes)
                
                # Try different query variations to find working one
                queries = [
                    # Try with node label matching
                    f'(sum(irate(node_cpu_seconds_total{{mode="iowait"}}[2m])) by (instance) / count(node_cpu_seconds_total) by (instance)) * 100 and on(instance) label_replace(kube_node_info{{node=~"({master_pattern})"}}, "instance", "$1:9100", "node", "(.+)")',
                    # Try with instance matching directly 
                    f'(sum(irate(node_cpu_seconds_total{{mode="iowait",instance=~"({master_pattern}):.*"}}[2m])) by (instance) / count(node_cpu_seconds_total{{instance=~"({master_pattern}):.*"}}) by (instance)) * 100',
                    # Try basic query and filter results afterward
                    '(sum(irate(node_cpu_seconds_total{mode="iowait"}[2m])) by (instance) / count(node_cpu_seconds_total) by (instance)) * 100'
                ]
                
                node_metrics = {}
                
                for i, query in enumerate(queries):
                    self.logger.info(f"Trying CPU iowait query {i+1}: {query}")
                    result = await prom.query_with_stats(query, duration)
                    
                    if result['status'] == 'success':
                        all_node_metrics = await self._process_metric_for_nodes(result, 'cpu_io_utilization_iowait')
                        
                        # Filter to include only master nodes
                        node_metrics = self._filter_master_nodes_only(all_node_metrics, master_nodes)
                        
                        if node_metrics:
                            self.logger.info(f"Found CPU iowait data for {len(node_metrics)} master nodes using query {i+1}")
                            break
                        else:
                            self.logger.warning(f"Query {i+1} returned data but no master nodes matched")
                    else:
                        self.logger.warning(f"Query {i+1} failed: {result.get('error', 'Unknown error')}")
                
                return {
                    'status': 'success',
                    'metric': 'cpu_io_utilization_iowait',
                    'title': 'CPU IO Utilization IOWait (Master Nodes Only)',
                    'unit': 'percent',
                    'duration': duration,
                    'timezone': 'UTC',
                    'nodes': node_metrics,
                    'master_nodes_found': list(node_metrics.keys()),
                    'total_master_nodes': master_nodes,
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting CPU IO utilization iowait for master nodes: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'metric': 'cpu_io_utilization_iowait',
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    def _filter_master_nodes_only(self, all_node_metrics: Dict[str, Dict[str, float]], master_nodes: List[str]) -> Dict[str, Dict[str, float]]:
        """Filter node metrics to include only master nodes"""
        filtered_metrics = {}
        
        for node_key, metrics in all_node_metrics.items():
            # Direct name match
            if node_key in master_nodes:
                filtered_metrics[node_key] = metrics
                continue
                
            # Check if node_key contains any master node name
            for master_node in master_nodes:
                if master_node in node_key or node_key in master_node:
                    filtered_metrics[master_node] = metrics
                    break
                    
            # Check if it's an instance format (IP:port or hostname:port)
            if ':' in node_key:
                base_node = node_key.split(':')[0]
                if base_node in master_nodes:
                    filtered_metrics[base_node] = metrics
                    continue
                    
                # Check partial matches for the base part
                for master_node in master_nodes:
                    if master_node in base_node or base_node in master_node:
                        filtered_metrics[master_node] = metrics
                        break
        
        return filtered_metrics
    
    async def get_apiserver_storage_objects_max(self, duration: str = "1h") -> Dict[str, Any]:
        """Get API server storage objects max (top 20) with resource names and max values"""
        try:
            metric_config = self.config.get_metric_by_name('apiserver_storage_objects_max_top20')
            if not metric_config:
                return {
                    'status': 'error',
                    'error': 'Metric apiserver_storage_objects_max_top20 not found in configuration',
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                result = await prom.query_with_stats(metric_config['expr'], duration)
                
                # Process result to extract resources and their values
                resources = await self._process_resource_metrics(result)
                
                return {
                    'status': 'success',
                    'metric': 'apiserver_storage_objects_max_top20',
                    'title': metric_config.get('title', 'apiserver_storage_objects_max'),
                    'unit': metric_config.get('unit', 'count'),
                    'duration': duration,
                    'timezone': 'UTC',
                    'resources': resources,
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting API server storage objects max: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'metric': 'apiserver_storage_objects_max_top20',
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def get_cluster_usage_resources_sum_top20(self, duration: str = "1h") -> Dict[str, Any]:
        """Get cluster usage resources sum (top 20) with resource names and max values"""
        try:
            # Use direct query since this metric might not be in config yet
            metric_query = "topk(20, sum by(resource) (cluster:usage:resources:sum))"
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                result = await prom.query_with_stats(metric_query, duration)
                
                # Process result to extract resources and their values
                resources = await self._process_resource_metrics(result)
                
                return {
                    'status': 'success',
                    'metric': 'cluster_usage_resources_sum_top20',
                    'title': 'Cluster Usage Resources Sum Top 20',
                    'unit': 'usage',
                    'duration': duration,
                    'timezone': 'UTC',
                    'resources': resources,
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting cluster usage resources sum: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'metric': 'cluster_usage_resources_sum_top20',
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def get_all_metrics_grouped_by_pod(self, duration: str = "1h") -> Dict[str, Any]:
        """Get all general info metrics grouped by etcd pod name with avg and max values"""
        try:
            general_metrics = self.config.get_metrics_by_category('general_info')
            pod_grouped_data = {}
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                # Collect all metrics
                for metric in general_metrics:
                    metric_name = metric['name']
                    metric_query = metric['expr']
                    
                    try:
                        result = await prom.query_with_stats(metric_query, duration)
                        pod_metrics = await self._process_metric_for_pods(result, metric_name)
                        
                        # Group by pod name
                        for pod_name, pod_data in pod_metrics.items():
                            if pod_name not in pod_grouped_data:
                                pod_grouped_data[pod_name] = {}
                            
                            pod_grouped_data[pod_name][metric_name] = {
                                'avg': pod_data.get('avg', 0),
                                'max': pod_data.get('max', 0),
                                'unit': metric.get('unit', 'unknown')
                            }
                            
                    except Exception as e:
                        self.logger.error(f"Error collecting metric {metric_name}: {e}")
                        continue
                
                return {
                    'status': 'success',
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'duration': duration,
                    'timezone': 'UTC',
                    'pods': pod_grouped_data
                }
                
        except Exception as e:
            self.logger.error(f"Error getting all metrics grouped by pod: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def get_key_metrics_summary(self, duration: str = "1h") -> Dict[str, Any]:
        """Get summary of key etcd metrics with avg and max per pod"""
        key_metrics = [
            'etcd_pods_cpu_usage',
            'etcd_pods_memory_usage', 
            'etcd_db_space_used_percent',
            'proposal_failure_rate',
            'etcd_has_leader',
            'etcd_slow_applies',
            'etcd_debugging_mvcc_total_keys'
        ]
        
        try:
            summary_data = {}
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                for metric_name in key_metrics:
                    try:
                        metric_config = self.config.get_metric_by_name(metric_name)
                        if not metric_config:
                            continue
                        
                        result = await prom.query_with_stats(metric_config['expr'], duration)
                        pod_metrics = await self._process_metric_for_pods(result, metric_name)
                        
                        summary_data[metric_name] = {
                            'title': metric_config.get('title', metric_name),
                            'unit': metric_config.get('unit', 'unknown'),
                            'pods': pod_metrics
                        }
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting key metric {metric_name}: {e}")
                        continue
                
                return {
                    'status': 'success',
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'duration': duration,
                    'timezone': 'UTC',
                    'key_metrics': summary_data
                }
                
        except Exception as e:
            self.logger.error(f"Error getting key metrics summary: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def _get_single_metric_per_pod(self, metric_name: str, duration: str) -> Dict[str, Any]:
        """Get a single metric with avg and max values per etcd pod"""
        try:
            metric_config = self.config.get_metric_by_name(metric_name)
            if not metric_config:
                return {
                    'status': 'error',
                    'error': f'Metric {metric_name} not found in configuration',
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
            
            prometheus_config = self.ocp_auth.get_prometheus_config()
            
            async with PrometheusBaseQuery(prometheus_config) as prom:
                connection_test = await prom.test_connection()
                if connection_test['status'] != 'connected':
                    return {
                        'status': 'error',
                        'error': f"Prometheus connection failed: {connection_test.get('error')}",
                        'timestamp': datetime.now(self.timezone).isoformat()
                    }
                
                result = await prom.query_with_stats(metric_config['expr'], duration)
                
                # Process result for pod breakdown
                pod_metrics = await self._process_metric_for_pods(result, metric_name)
                
                return {
                    'status': 'success',
                    'metric': metric_name,
                    'title': metric_config.get('title', metric_name),
                    'unit': metric_config.get('unit', 'unknown'),
                    'duration': duration,
                    'timezone': 'UTC',
                    'pods': pod_metrics,
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting metric {metric_name}: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'metric': metric_name,
                'timestamp': datetime.now(self.timezone).isoformat()
            }
    
    async def _process_metric_for_pods(self, result: Dict[str, Any], metric_name: str) -> Dict[str, Dict[str, float]]:
        """Process Prometheus metric result to extract pod-level avg and max values"""
        pod_data = {}
        
        if result['status'] != 'success':
            return pod_data
        
        try:
            # Get series data from Prometheus result
            series_data = result.get('series_data', [])
            
            # Build pod to node mapping for efficient lookups
            pod_to_node = await self.tools_utility.get_pod_to_node_mapping('openshift-etcd')
            
            # Process each series
            for series in series_data:
                labels = series.get('labels', {})
                values = series.get('values', [])
                
                # Extract pod name from various label fields
                pod_name = self._extract_pod_name(labels)
                if not pod_name or pod_name == 'unknown':
                    continue
                
                # Calculate statistics from values
                numeric_values = []
                for value_data in values:
                    if isinstance(value_data, dict):
                        val = value_data.get('value')
                        if val is not None and val != 'NaN':
                            try:
                                numeric_values.append(float(val))
                            except (ValueError, TypeError):
                                continue
                
                if numeric_values:
                    pod_data[pod_name] = {
                        'avg': round(sum(numeric_values) / len(numeric_values), 4),
                        'max': round(max(numeric_values), 4),
                        'min': round(min(numeric_values), 4),
                        'count': len(numeric_values),
                        'node': pod_to_node.get(pod_name, 'unknown')
                    }
        
        except Exception as e:
            self.logger.error(f"Error processing metric data for {metric_name}: {e}")
        
        return pod_data
    
    async def _process_metric_for_nodes(self, result: Dict[str, Any], metric_name: str) -> Dict[str, Dict[str, float]]:
        """Process Prometheus metric result to extract node-level avg and max values"""
        node_data = {}
        
        if result['status'] != 'success':
            self.logger.warning(f"Metric result status is not success for {metric_name}: {result.get('error', 'Unknown error')}")
            return node_data
        
        try:
            # Get series data from Prometheus result
            series_data = result.get('series_data', [])
            self.logger.debug(f"Processing {len(series_data)} series for metric {metric_name}")
            
            # Process each series
            for series in series_data:
                labels = series.get('labels', {})
                values = series.get('values', [])
                
                self.logger.debug(f"Processing series with labels: {labels}")
                
                # Extract node name from various label fields
                node_name = self._extract_node_name(labels)
                if not node_name or node_name == 'unknown':
                    self.logger.debug(f"Could not extract node name from labels: {labels}")
                    continue
                
                # Calculate statistics from values
                numeric_values = []
                for value_data in values:
                    if isinstance(value_data, dict):
                        val = value_data.get('value')
                        if val is not None and val != 'NaN':
                            try:
                                numeric_values.append(float(val))
                            except (ValueError, TypeError):
                                continue
                    elif isinstance(value_data, list) and len(value_data) >= 2:
                        # Handle [timestamp, value] format
                        val = value_data[1]
                        if val is not None and val != 'NaN':
                            try:
                                numeric_values.append(float(val))
                            except (ValueError, TypeError):
                                continue
                
                if numeric_values:
                    if node_name not in node_data:
                        node_data[node_name] = {
                            'avg': round(sum(numeric_values) / len(numeric_values), 4),
                            'max': round(max(numeric_values), 4),
                            'min': round(min(numeric_values), 4),
                            'count': len(numeric_values)
                        }
                    else:
                        # If node already exists, combine data
                        existing = node_data[node_name]
                        all_values = [existing['avg']] * existing['count'] + numeric_values
                        node_data[node_name] = {
                            'avg': round(sum(all_values) / len(all_values), 4),
                            'max': round(max(all_values), 4),
                            'min': round(min(all_values), 4),
                            'count': len(all_values)
                        }
                else:
                    self.logger.debug(f"No valid numeric values found for node {node_name}")
        
        except Exception as e:
            self.logger.error(f"Error processing metric data for nodes {metric_name}: {e}")
        
        self.logger.info(f"Processed node data for {metric_name}: {len(node_data)} nodes found")
        return node_data
    
    async def _process_apiserver_storage_objects(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process API server storage objects result to extract resource names and their values"""
        return await self._process_resource_metrics(result)
    
    async def _process_resource_metrics(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process resource metrics result to extract resource names and their values"""
        resources = []
        
        if result['status'] != 'success':
            return resources
        
        try:
            # Get series data from Prometheus result
            series_data = result.get('series_data', [])
            
            # Extract resource name and value from each series
            for series in series_data:
                labels = series.get('labels', {})
                values = series.get('values', [])
                
                # Extract resource name from labels
                resource_name = self._extract_resource_name(labels)
                
                # Get the latest/max value from the time series
                max_value = 0
                for value_data in values:
                    if isinstance(value_data, dict):
                        val = value_data.get('value')
                        if val is not None and val != 'NaN':
                            try:
                                numeric_val = float(val)
                                max_value = max(max_value, numeric_val)
                            except (ValueError, TypeError):
                                continue
                    elif isinstance(value_data, list) and len(value_data) >= 2:
                        # Handle [timestamp, value] format
                        val = value_data[1]
                        if val is not None and val != 'NaN':
                            try:
                                numeric_val = float(val)
                                max_value = max(max_value, numeric_val)
                            except (ValueError, TypeError):
                                continue
                
                if resource_name != 'unknown' and max_value > 0:
                    resources.append({
                        'resource_name': resource_name,
                        'max_value': round(max_value, 4)
                    })
            
            # Sort by max_value in descending order (since it's topk(20))
            resources.sort(key=lambda x: x['max_value'], reverse=True)
        
        except Exception as e:
            self.logger.error(f"Error processing resource metrics: {e}")
        
        return resources
    
    def _extract_pod_name(self, labels: Dict[str, Any]) -> str:
        """Extract pod name from Prometheus metric labels"""
        # Try various label fields that might contain pod name
        pod_name_fields = [
            'pod',
            'pod_name', 
            'kubernetes_pod_name',
            'name',
            'instance'
        ]
        
        for field in pod_name_fields:
            if field in labels and labels[field]:
                pod_name = str(labels[field])
                # Filter for etcd pods only
                if 'etcd' in pod_name.lower():
                    return pod_name
        
        # Try to extract from instance field if it contains pod info
        if 'instance' in labels:
            instance = str(labels['instance'])
            if 'etcd' in instance.lower() and ':' in instance:
                # Extract pod name from instance like "pod-name:port"
                parts = instance.split(':')
                if parts and 'etcd' in parts[0].lower():
                    return parts[0]
        
        return 'unknown'
    
    def _extract_node_name(self, labels: Dict[str, Any]) -> str:
        """Extract node name from Prometheus metric labels"""
        # Try various label fields that might contain node name
        node_name_fields = [
            'node',
            'instance',
            'kubernetes_node',
            'node_name',
            'exported_instance'
        ]
        
        for field in node_name_fields:
            if field in labels and labels[field]:
                node_name = str(labels[field])
                # Clean up instance field if it contains port
                if field == 'instance' and ':' in node_name:
                    node_name = node_name.split(':')[0]
                # For CPU metrics, the instance might be an IP, try to resolve to node name
                if field == 'instance' and self._is_ip_address(node_name):
                    # Return as-is, will be resolved later if needed
                    return node_name
                return node_name
        
        return 'unknown'
    
    def _is_ip_address(self, address: str) -> bool:
        """Check if a string is an IP address"""
        try:
            import ipaddress
            ipaddress.ip_address(address)
            return True
        except (ValueError, ImportError):
            return False
    
    def _extract_resource_name(self, labels: Dict[str, Any]) -> str:
        """Extract resource name from Prometheus metric labels"""
        # Try various label fields that might contain resource information
        resource_name_fields = [
            'resource',
            'group_resource',
            'name',
            '__name__',
            'job',
            'exported_resource',
            'apiserver_storage_object'
        ]
        
        for field in resource_name_fields:
            if field in labels and labels[field]:
                resource_value = str(labels[field])
                # Clean up the resource name if needed
                if resource_value and resource_value != 'unknown':
                    return resource_value
        
        # Try to construct resource name from multiple fields
        if 'group' in labels and 'resource' in labels:
            group = str(labels.get('group', ''))
            resource = str(labels.get('resource', ''))
            if group and resource:
                return f"{group}/{resource}" if group != 'core' else resource
        
        # Fallback: use any available label that might indicate the resource
        for key, value in labels.items():
            if key not in ['instance', 'job', 'prometheus'] and value:
                return f"{key}:{value}"
        
        return 'unknown'
    
    async def get_etcd_cluster_overview(self, duration: str = "1h") -> Dict[str, Any]:
        """Get comprehensive etcd cluster overview with key metrics per pod"""
        try:
            # Get cluster member information
            cluster_info = await self.tools_utility.get_etcd_cluster_members()
            health_info = await self.tools_utility.get_etcd_cluster_health()
            
            # Get key metrics summary
            metrics_summary = await self.get_key_metrics_summary(duration)
            
            # Get etcd pods information
            etcd_pods = await self.tools_utility.get_etcd_pods('openshift-etcd')
            
            cluster_overview = {
                'status': 'success',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'duration': duration,
                'timezone': 'UTC',
                'cluster': {
                    'members': cluster_info,
                    'health': health_info,
                    'pods_count': len(etcd_pods),
                    'running_pods': len([p for p in etcd_pods if p['phase'] == 'Running'])
                },
                'metrics': metrics_summary.get('key_metrics', {}),
                'pods': {pod['name']: {
                    'phase': pod['phase'],
                    'node': pod['node_name'],
                    'ready': pod['ready']
                } for pod in etcd_pods}
            }
            
            return cluster_overview
            
        except Exception as e:
            self.logger.error(f"Error getting etcd cluster overview: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now(self.timezone).isoformat()
            }