"""
demo/scenarios.py
================

Demonstration scenarios for the exapyte distributed platform.
These scenarios showcase different capabilities of the system.
"""

import asyncio
import logging
import time
import random
import string
from typing import Dict, Any, List, Optional

from exapyte.services.client import ExabyteClient, create_client
from exapyte.consensus.protocol_factory import ProtocolFactory, ConsensusType
from exapyte.storage.storage_factory import StorageFactory, StorageType
from exapyte.replication.replicator_factory import ReplicatorFactory, ReplicationType
from exapyte.distribution.strategy_factory import StrategyFactory, DistributionType


logger = logging.getLogger("demo.scenarios")


async def run(scenario_name: str = "consensus"):
    """
    Run a specific demonstration scenario
    
    Args:
        scenario_name: Name of the scenario to run
    """
    logger.info(f"Running scenario: {scenario_name}")
    
    # Create a client
    client = await create_client()
    
    # Run the appropriate scenario
    if scenario_name == "consensus":
        await consensus_comparison(client)
    elif scenario_name == "replication":
        await replication_strategies(client)
    elif scenario_name == "distribution":
        await distribution_strategies(client)
    else:
        logger.error(f"Unknown scenario: {scenario_name}")
    
    # Close client connection
    await client.disconnect()


async def consensus_comparison(client: Optional[ExabyteClient] = None):
    """
    Demonstrate and compare different consensus protocols
    
    Args:
        client: ExabyteClient instance (optional)
    """
    logger.info("Starting consensus comparison demonstration")
    
    # Create client if not provided
    if not client:
        client = await create_client()
    
    # Get characteristics of each consensus protocol
    raft_characteristics = ProtocolFactory.get_protocol_characteristics(ConsensusType.RAFT)
    paxos_characteristics = ProtocolFactory.get_protocol_characteristics(ConsensusType.PAXOS)
    zab_characteristics = ProtocolFactory.get_protocol_characteristics(ConsensusType.ZAB)
    
    logger.info("Consensus protocol comparison:")
    logger.info(f"1. {raft_characteristics['name']}: {raft_characteristics['leadership']}")
    logger.info(f"2. {paxos_characteristics['name']}: {paxos_characteristics['leadership']}")
    logger.info(f"3. {zab_characteristics['name']}: {zab_characteristics['leadership']}")
    
    # Demonstrate performance differences
    protocols = ["raft", "paxos", "zab"]
    write_latencies = {}
    read_latencies = {}
    
    for protocol in protocols:
        # Create test key for this protocol
        test_key = f"consensus_test_{protocol}_{int(time.time())}"
        test_value = {"protocol": protocol, "timestamp": time.time()}
        
        logger.info(f"Testing {protocol} performance with key: {test_key}")
        
        # Simulate write performance
        start_time = time.time()
        
        # Perform write with simulated protocol
        await client.set(
            key=test_key,
            value=test_value,
            consistency="strong"  # All consensus protocols use strong consistency
        )
        
        write_latency = (time.time() - start_time) * 1000  # in ms
        write_latencies[protocol] = write_latency
        
        logger.info(f"  Write latency ({protocol}): {write_latency:.2f}ms")
        
        # Simulate read performance
        start_time = time.time()
        
        # Perform read with simulated protocol
        await client.get(
            key=test_key,
            consistency="strong"
        )
        
        read_latency = (time.time() - start_time) * 1000  # in ms
        read_latencies[protocol] = read_latency
        
        logger.info(f"  Read latency ({protocol}): {read_latency:.2f}ms")
        
        # Small delay between tests
        await asyncio.sleep(0.5)
    
    # Simulate leader election
    logger.info("Simulating leader election scenarios:")
    
    # Raft leader election (timeout-based)
    logger.info("Raft leader election:")
    logger.info("  1. Node detects leader timeout")
    logger.info("  2. Node becomes candidate and increments term")
    logger.info("  3. Node votes for itself and requests votes from peers")
    logger.info("  4. If majority votes received, becomes leader")
    logger.info("  5. New leader sends AppendEntries to establish authority")
    
    # Paxos leader acquisition
    logger.info("Paxos leader acquisition:")
    logger.info("  1. Node executes prepare phase with higher ballot number")
    logger.info("  2. Nodes promise not to accept lower proposals")
    logger.info("  3. Node executes accept phase")
    logger.info("  4. If majority accepts, value is chosen")
    logger.info("  5. Node informs all nodes of chosen value")
    
    # ZAB leader election
    logger.info("ZAB leader election:")
    logger.info("  1. Node detects leader failure")
    logger.info("  2. Node enters election phase with higher epoch")
    logger.info("  3. Nodes vote for the node with the highest zxid")
    logger.info("  4. New leader establishes epoch and synchronizes followers")
    logger.info("  5. New leader begins broadcast phase")
    
    # Summary
    logger.info("Consensus protocol comparison summary:")
    for protocol in protocols:
        logger.info(f"  {protocol.upper()}:")
        logger.info(f"    Write latency: {write_latencies[protocol]:.2f}ms")
        logger.info(f"    Read latency: {read_latencies[protocol]:.2f}ms")
    
    # Display key trade-offs
    logger.info("Key trade-offs:")
    logger.info(f"  Raft: {raft_characteristics['trade_offs']}")
    logger.info(f"  Paxos: {paxos_characteristics['trade_offs']}")
    logger.info(f"  ZAB: {zab_characteristics['trade_offs']}")
    
    # Demonstrate failure handling differences
    logger.info("Failure handling comparison:")
    
    # Simulate node failure
    logger.info("Simulating node failure scenarios:")
    
    # Raft failure handling
    logger.info("Raft failure handling:")
    logger.info("  - When follower fails: Leader continues operation with remaining followers")
    logger.info("  - When leader fails: New election occurs after timeout")
    logger.info("  - Recovery: Rejoining node receives log entries from leader")
    
    # Paxos failure handling
    logger.info("Paxos failure handling:")
    logger.info("  - No explicit leader: System continues with any majority of nodes")
    logger.info("  - Multi-Paxos optimization: Leader failure requires new leader acquisition")
    logger.info("  - Recovery: Catch-up via full state transfer or incremental updates")
    
    # ZAB failure handling
    logger.info("ZAB failure handling:")
    logger.info("  - When follower fails: Leader continues operation with remaining followers")
    logger.info("  - When leader fails: New leader elected based on highest zxid")
    logger.info("  - Recovery: Synchronization phase ensures consistency before broadcast phase")
    
    logger.info("Consensus comparison demonstration completed")


async def replication_strategies(client: Optional[ExabyteClient] = None):
    """
    Demonstrate different replication strategies
    
    Args:
        client: ExabyteClient instance (optional)
    """
    logger.info("Starting replication strategies demonstration")
    
    # Create client if not provided
    if not client:
        client = await create_client()
    
    # Get characteristics of each replication strategy
    async_characteristics = ReplicatorFactory.get_replication_characteristics(ReplicationType.ASYNC)
    sync_characteristics = ReplicatorFactory.get_replication_characteristics(ReplicationType.SYNC)
    optimized_characteristics = ReplicatorFactory.get_replication_characteristics(ReplicationType.OPTIMIZED)
    
    logger.info("Replication strategy comparison:")
    logger.info(f"1. {async_characteristics['name']}: {async_characteristics['consistency']}")
    logger.info(f"2. {sync_characteristics['name']}: {sync_characteristics['consistency']}")
    logger.info(f"3. {optimized_characteristics['name']}: {optimized_characteristics['consistency']}")
    
    # Demonstrate performance differences
    strategies = ["async", "sync", "optimized"]
    write_latencies = {}
    throughputs = {}
    
    for strategy in strategies:
        # Create test key prefix for this strategy
        test_key_prefix = f"repl_test_{strategy}_{int(time.time())}"
        
        logger.info(f"Testing {strategy} replication performance")
        
        # Measure write latency
        start_time = time.time()
        
        # Perform a single write to measure latency
        test_key = f"{test_key_prefix}_latency"
        test_value = {"strategy": strategy, "timestamp": time.time()}
        
        # Adjust consistency based on strategy
        consistency = "eventual" if strategy == "async" else "strong" if strategy == "sync" else "session"
        
        await client.set(
            key=test_key,
            value=test_value,
            consistency=consistency
        )
        
        write_latency = (time.time() - start_time) * 1000  # in ms
        write_latencies[strategy] = write_latency
        
        logger.info(f"  Write latency ({strategy}): {write_latency:.2f}ms")
        
        # Measure throughput with multiple writes
        num_ops = 10
        start_time = time.time()
        
        for i in range(num_ops):
            test_key = f"{test_key_prefix}_throughput_{i}"
            test_value = {"strategy": strategy, "index": i, "timestamp": time.time()}
            
            await client.set(
                key=test_key,
                value=test_value,
                consistency=consistency
            )
        
        elapsed = time.time() - start_time
        throughput = num_ops / elapsed if elapsed > 0 else 0  # ops/sec
        throughputs[strategy] = throughput
        
        logger.info(f"  Throughput ({strategy}): {throughput:.2f} ops/sec")
        
        # Small delay between tests
        await asyncio.sleep(0.5)
    
    # Simulate network partition for each strategy
    logger.info("Simulating network partition scenarios:")
    
    # Async replication during partition
    logger.info("Asynchronous replication during network partition:")
    logger.info("  - Nodes continue to accept writes independently")
    logger.info("  - Local reads remain available everywhere")
    logger.info("  - After partition heals: Background reconciliation of divergent updates")
    logger.info("  - Conflict resolution typically based on timestamps or vector clocks")
    
    # Sync replication during partition
    logger.info("Synchronous replication during network partition:")
    logger.info("  - Minority partition: Refuses writes to maintain consistency")
    logger.info("  - Majority partition: Continues operation normally")
    logger.info("  - After partition heals: Minority nodes catch up from majority")
    logger.info("  - No conflicts to resolve due to consistency guarantees")
    
    # Optimized replication during partition
    logger.info("Optimized replication during network partition:")
    logger.info("  - Dynamically adjusts consistency levels based on network conditions")
    logger.info("  - Prioritizes availability for critical operations")
    logger.info("  - Uses intelligent batching and compression to maximize throughput")
    logger.info("  - After partition heals: Uses delta-based synchronization")
    
    # Demonstrate replication optimizations
    logger.info("Replication optimizations in optimized strategy:")
    
    # Batching
    logger.info("1. Batching:")
    logger.info("  - Groups multiple changes into a single network operation")
    logger.info("  - Adaptive batch sizing based on load and network conditions")
    logger.info("  - Prioritization of critical updates within batches")
    
    # Compression
    logger.info("2. Compression:")
    logger.info("  - Compresses data before transmission to reduce network usage")
    logger.info("  - Selective compression based on data type and size")
    logger.info("  - Multiple compression algorithms for different data patterns")
    
    # Delta encoding
    logger.info("3. Delta encoding:")
    logger.info("  - Transmits only changes instead of complete objects")
    logger.info("  - Particularly effective for large objects with small changes")
    logger.info("  - Requires baseline synchronization between nodes")
    
    # Summary
    logger.info("Replication strategy comparison summary:")
    for strategy in strategies:
        logger.info(f"  {strategy.upper()}:")
        logger.info(f"    Write latency: {write_latencies[strategy]:.2f}ms")
        logger.info(f"    Throughput: {throughputs[strategy]:.2f} ops/sec")
    
    # Display key trade-offs
    logger.info("Key trade-offs:")
    logger.info(f"  Async: {async_characteristics['trade_offs']}")
    logger.info(f"  Sync: {sync_characteristics['trade_offs']}")
    logger.info(f"  Optimized: {optimized_characteristics['trade_offs']}")
    
    logger.info("Replication strategies demonstration completed")


async def distribution_strategies(client: Optional[ExabyteClient] = None):
    """
    Demonstrate different data distribution strategies
    
    Args:
        client: ExabyteClient instance (optional)
    """
    logger.info("Starting distribution strategies demonstration")
    
    # Create client if not provided
    if not client:
        client = await create_client()
    
    # Get characteristics of each distribution strategy
    centralized_characteristics = StrategyFactory.get_strategy_characteristics(DistributionType.CENTRALIZED)
    sharded_characteristics = StrategyFactory.get_strategy_characteristics(DistributionType.SHARDED)
    hierarchical_characteristics = StrategyFactory.get_strategy_characteristics(DistributionType.HIERARCHICAL)
    
    logger.info("Distribution strategy comparison:")
    logger.info(f"1. {centralized_characteristics['name']}: {centralized_characteristics['scalability']}")
    logger.info(f"2. {sharded_characteristics['name']}: {sharded_characteristics['scalability']}")
    logger.info(f"3. {hierarchical_characteristics['name']}: {hierarchical_characteristics['scalability']}")
    
    # Demonstrate sharding approaches
    logger.info("Sharding approaches comparison:")
    
    # Hash-based sharding
    logger.info("1. Hash-based sharding:")
    logger.info("  - Keys distributed based on hash function (e.g., MD5, MurmurHash)")
    logger.info("  - Consistent hashing minimizes redistribution during node changes")
    logger.info("  - Even distribution regardless of key values")
    logger.info("  - Efficient point queries, but range queries require scatter-gather")
    
    # Create test data for hash-based sharding demonstration
    hash_keys = [f"hash_key_{i}" for i in range(5)]
    for key in hash_keys:
        value = {"type": "hash_sharded", "timestamp": time.time()}
        await client.set(key=key, value=value)
    
    logger.info(f"  - Created {len(hash_keys)} hash-sharded test keys")
    
    # Range-based sharding
    logger.info("2. Range-based sharding:")
    logger.info("  - Keys distributed based on lexicographical ranges")
    logger.info("  - Each shard responsible for a specific key range")
    logger.info("  - Efficient range queries within a single shard")
    logger.info("  - May lead to uneven distribution (hot spots)")
    
    # Create test data for range-based sharding demonstration
    range_prefixes = ["a", "b", "c", "d", "e"]
    range_keys = []
    for prefix in range_prefixes:
        for i in range(2):
            key = f"{prefix}_range_key_{i}"
            range_keys.append(key)
            value = {"type": "range_sharded", "prefix": prefix, "timestamp": time.time()}
            await client.set(key=key, value=value)
    
    logger.info(f"  - Created {len(range_keys)} range-sharded test keys")
    
    # Demonstrate hierarchical distribution
    logger.info("Hierarchical distribution structure:")
    logger.info("  Global Tier:")
    logger.info("  - Handles cross-region coordination")
    logger.info("  - Manages global indexes and metadata")
    logger.info("  - Routes requests to appropriate regions")
    
    logger.info("  Regional Tier:")
    logger.info("  - Handles data within a geographical region")
    logger.info("  - Maintains region-specific indexes")
    logger.info("  - Implements intra-region sharding")
    
    logger.info("  Node Tier:")
    logger.info("  - Stores and processes actual data")
    logger.info("  - Implements local optimization strategies")
    logger.info("  - Handles replication within shard group")
    
    # Create test data for hierarchical distribution demonstration
    regions = ["us", "eu", "asia"]
    hierarchical_keys = []
    for region in regions:
        for i in range(3):
            key = f"{region}_hierarchical_key_{i}"
            hierarchical_keys.append(key)
            value = {"type": "hierarchical", "region": region, "timestamp": time.time()}
            await client.set(key=key, value=value)
    
    logger.info(f"  - Created {len(hierarchical_keys)} hierarchically distributed test keys")
    
    # Demonstrate scalability characteristics
    logger.info("Scalability comparison:")
    
    # Centralized
    logger.info("Centralized scalability:")
    logger.info("  - Vertical scaling only (larger machines)")
    logger.info("  - Limited by single node capacity")
    logger.info("  - Simple architecture, no coordination overhead")
    logger.info("  - Single point of failure")
    
    # Sharded
    logger.info("Sharded scalability:")
    logger.info("  - Horizontal scaling (more machines)")
    logger.info("  - Near-linear capacity increase with more nodes")
    logger.info("  - Rebalancing required when adding/removing nodes")
    logger.info("  - Cross-shard operations can be complex")
    
    # Hierarchical
    logger.info("Hierarchical scalability:")
    logger.info("  - Multi-dimensional scaling (both vertical and horizontal)")
    logger.info("  - Regional isolation improves fault tolerance")
    logger.info("  - Topology-aware request routing reduces latency")
    logger.info("  - Complex coordination across tiers")
    
    # Simulate query patterns
    logger.info("Query pattern performance comparison:")
    
    # Point queries
    logger.info("Point query performance:")
    logger.info("  - Centralized: Direct lookup, limited by single node capacity")
    logger.info("  - Sharded: Single shard lookup, efficient for known keys")
    logger.info("  - Hierarchical: Topology-aware routing, optimized for locality")
    
    # Range queries
    logger.info("Range query performance:")
    logger.info("  - Centralized: Efficient for small ranges, limited by memory")
    logger.info("  - Hash-sharded: Requires scatter-gather across many shards")
    logger.info("  - Range-sharded: Efficient if range is within one shard")
    logger.info("  - Hierarchical: Region-aware execution, parallel within regions")
    
    # Aggregation queries
    logger.info("Aggregation query performance:")
    logger.info("  - Centralized: Simple but limited by dataset size")
    logger.info("  - Sharded: Requires two-phase execution (map-reduce pattern)")
    logger.info("  - Hierarchical: Multi-level aggregation with parallel execution")
    
    # Display key trade-offs
    logger.info("Key trade-offs:")
    logger.info(f"  Centralized: {centralized_characteristics['trade_offs']}")
    logger.info(f"  Sharded: {sharded_characteristics['trade_offs']}")
    logger.info(f"  Hierarchical: {hierarchical_characteristics['trade_offs']}")
    
    logger.info("Distribution strategies demonstration completed")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run consensus comparison by default
    asyncio.run(run("consensus"))