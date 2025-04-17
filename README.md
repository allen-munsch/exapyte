# Exapyte Distributed Platform

A high-scale, multi-region distributed data platform demonstrating various things.

## Features

- **Global Distribution**: Data available across multiple geographical regions with minimal latency
- **Multiple Consensus Protocols**: Support for Raft, Paxos, and ZAB consensus algorithms
- **Pluggable Storage Backends**: In-memory, disk-based, and tiered storage options
- **Flexible Replication Strategies**: Synchronous, asynchronous, and optimized replication
- **Advanced Data Distribution**: Centralized, sharded, and hierarchical distribution strategies
- **Tunable Consistency Levels**: From eventual to strong consistency with various options in between

## Installation

```bash
# Clone the repository
git clone https://github.com/allen-munsch/exapyte.git
cd exapyte

# Install dependencies
pip install -r requirements.txt
```

## Running a Node

To run a node of the distributed platform:

```bash
python main.py --node --config your_config.json --verbose
```

The configuration file specifies all aspects of the node's behavior. See `config.json` for a template.

## Running Demonstrations

To run demonstrations that showcase different aspects of the platform:

```bash
# Consensus protocol comparison
python main.py --demo consensus

# Replication strategies comparison
python main.py --demo replication

# Distribution strategies comparison
python main.py --demo distribution
```

## Configuration

The `config.json` file allows you to configure all aspects of a node:

- **Node Identity**: `node_id` and `region_id`
- **Storage**: Type and configuration of the storage backend
- **Network**: Network address and cluster configuration
- **Consensus**: Consensus protocol type and parameters
- **Replication**: Replication strategy and configuration
- **Distribution**: Data distribution strategy and settings
- **Control Plane**: Global control plane settings
- **Data Plane**: Regional data plane settings

## Architecture

The platform consists of several key components:

1. **Consensus Layer**: Provides strong consistency guarantees where needed
2. **Storage Layer**: Manages data persistence with various backends
3. **Replication Layer**: Handles data synchronization across nodes
4. **Distribution Layer**: Manages data partitioning and routing
5. **Control Plane**: Coordinates global metadata and topology
6. **Data Plane**: Processes data operations in each region

## Extensibility

The platform is designed with extensibility in mind:

- Add new consensus protocols by implementing the consensus interface
- Create custom storage backends by extending the storage base class
- Develop new replication strategies for specific use cases
- Implement specialized distribution strategies for different workloads
