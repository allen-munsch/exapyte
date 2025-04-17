"""
main.py
=======

Main entry point for the exapyte distributed platform.
Initializes and starts the system based on configuration.
"""

import asyncio
import argparse
import logging
import sys
import json
import os
from typing import Dict, Any, List, Optional

from src.exapyte.consensus.protocol_factory import ProtocolFactory, ConsensusType
from src.exapyte.storage.storage_factory import StorageFactory, StorageType
from src.exapyte.replication.replicator_factory import ReplicatorFactory, ReplicationType
from src.exapyte.distribution.strategy_factory import StrategyFactory, DistributionType
from src.exapyte.networking.network_manager import NetworkManager
from src.exapyte.services.control_plane import ControlPlane
from src.exapyte.services.data_plane import DataPlane
import demo.scenarios


def setup_logging(verbosity: int) -> None:
    """Configure logging based on verbosity level"""
    level = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG
    }.get(verbosity, logging.DEBUG)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a file
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Dict with configuration
    """
    if not os.path.exists(config_file):
        logging.warning(f"Config file {config_file} not found, using default configuration")
        return {}
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        logging.info(f"Loaded configuration from {config_file}")
        return config
    except Exception as e:
        logging.error(f"Error loading configuration from {config_file}: {e}")
        return {}


async def initialize_node(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Initialize a node with all components
    
    Args:
        config: Node configuration
        
    Returns:
        Dict with initialized components
    """
    node_id = config.get("node_id", "node1")
    region_id = config.get("region_id", "region1")
    
    logging.info(f"Initializing node {node_id} in region {region_id}")
    
    # Initialize storage
    storage_type = StorageType(config.get("storage", {}).get("type", "memory"))
    storage_config = config.get("storage", {}).get("config", {})
    storage_engine = StorageFactory.create_storage(storage_type, storage_config)
    
    # Initialize network manager
    network_config = config.get("network", {})
    node_address = network_config.get("address", "localhost:8000")
    cluster_config = network_config.get("cluster", {})
    
    network_manager = NetworkManager(
        node_id=node_id,
        node_address=node_address,
        cluster_config=cluster_config,
        simulation_mode=network_config.get("simulation_mode", False)
    )
    
    # Initialize consensus protocol
    consensus_type = ConsensusType(config.get("consensus", {}).get("type", "raft"))
    consensus_config = config.get("consensus", {}).get("config", {})
    
    consensus_node = ProtocolFactory.create_consensus_node(
        protocol_type=consensus_type,
        node_id=node_id,
        cluster_config=cluster_config,
        storage_engine=storage_engine,
        network_manager=network_manager,
        **consensus_config
    )
    
    # Initialize replication
    replication_type = ReplicationType(config.get("replication", {}).get("type", "async"))
    replication_config = config.get("replication", {}).get("config", {})
    
    replication_manager = ReplicatorFactory.create_replicator(
        replication_type=replication_type,
        node_id=node_id,
        network_manager=network_manager,
        storage_engine=storage_engine,
        config=replication_config
    )
    
    # Initialize distribution strategy
    distribution_type = DistributionType(config.get("distribution", {}).get("type", "sharded"))
    distribution_config = config.get("distribution", {}).get("config", {})
    
    distribution_strategy = StrategyFactory.create_strategy(
        strategy_type=distribution_type,
        node_id=node_id,
        network_manager=network_manager,
        storage_engine=storage_engine,
        config=distribution_config
    )
    
    # Initialize control plane
    control_config = config.get("control_plane", {})
    control_plane = ControlPlane(
        node_id=node_id,
        config=control_config,
        consensus_node=consensus_node,
        network_manager=network_manager
    )
    
    # Initialize data plane
    data_config = config.get("data_plane", {})
    data_plane = DataPlane(
        node_id=node_id,
        region_id=region_id,
        config=data_config,
        storage_engine=storage_engine,
        replication_manager=replication_manager,
        distribution_strategy=distribution_strategy,
        network_manager=network_manager
    )
    
    # Start all components
    await storage_engine.start()
    await network_manager.start()
    await consensus_node.start()
    await replication_manager.start()
    await distribution_strategy.start()
    await control_plane.start()
    await data_plane.start()
    
    # Return all initialized components
    return {
        "node_id": node_id,
        "region_id": region_id,
        "storage_engine": storage_engine,
        "network_manager": network_manager,
        "consensus_node": consensus_node,
        "replication_manager": replication_manager,
        "distribution_strategy": distribution_strategy,
        "control_plane": control_plane,
        "data_plane": data_plane
    }


async def shutdown_node(components: Dict[str, Any]) -> None:
    """
    Shut down all components gracefully
    
    Args:
        components: Dict with components to shut down
    """
    node_id = components.get("node_id", "unknown")
    logging.info(f"Shutting down node {node_id}")
    
    # Shutdown in reverse order of initialization
    data_plane = components.get("data_plane")
    if data_plane:
        await data_plane.stop()
    
    control_plane = components.get("control_plane")
    if control_plane:
        await control_plane.stop()
    
    distribution_strategy = components.get("distribution_strategy")
    if distribution_strategy:
        await distribution_strategy.stop()
    
    replication_manager = components.get("replication_manager")
    if replication_manager:
        await replication_manager.stop()
    
    consensus_node = components.get("consensus_node")
    if consensus_node:
        await consensus_node.stop()
    
    network_manager = components.get("network_manager")
    if network_manager:
        await network_manager.stop()
    
    storage_engine = components.get("storage_engine")
    if storage_engine:
        await storage_engine.stop()
    
    logging.info(f"Node {node_id} shut down successfully")


async def run_node(config: Dict[str, Any]) -> None:
    """
    Run a node with all components
    
    Args:
        config: Node configuration
    """
    components = None
    
    try:
        # Initialize node
        components = await initialize_node(config)
        
        # Keep node running until interrupted
        logging.info(f"Node {components['node_id']} running, press Ctrl+C to stop")
        
        # Run until interrupted
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception as e:
        logging.error(f"Error running node: {e}")
    finally:
        # Shut down node
        if components:
            await shutdown_node(components)


async def run_demo(scenario: str) -> None:
    """
    Run a demonstration scenario
    
    Args:
        scenario: Scenario to run
    """
    try:
        await demo.scenarios.run(scenario)
    except Exception as e:
        logging.error(f"Error running demo scenario {scenario}: {e}")


def main() -> int:
    """
    Main entry point
    
    Returns:
        Exit code
    """
    parser = argparse.ArgumentParser(description='Exapyte Distributed Platform')
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--node', action='store_true', help='Run as a node')
    mode_group.add_argument('--demo', choices=['consensus', 'replication', 'distribution'], 
                          help='Run a demonstration scenario')
    
    # Node options
    parser.add_argument('--config', type=str, default='config.json', 
                      help='Path to configuration file')
    
    # Verbosity
    parser.add_argument('-v', '--verbose', action='count', default=0,
                      help='Increase verbosity (can be used multiple times)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    try:
        if args.node:
            # Load configuration
            config = load_config(args.config)
            
            # Run node
            asyncio.run(run_node(config))
        elif args.demo:
            # Run demonstration
            asyncio.run(run_demo(args.demo))
        
        return 0
    except Exception as e:
        logging.error(f"Error in main: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())