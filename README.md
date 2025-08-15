# LocalServiceDNS - Distributed Service Communication System

## Overview

LocalServiceDNS is a Python-based distributed communication system that provides service discovery and two primary communication patterns: Publisher-Subscriber and Service-Consumer (RPC-style). The system uses a central DNS server for service discovery and registration.

## Architecture Components

### 1. DNS Server (`DNS` class)
A centralized service registry that manages service locations.

**Purpose**: 
- Stores and retrieves service locations (IP:port mappings)
- Enables service discovery for distributed components

**Key Methods**:
- `open()`: Starts the DNS server
- `close()`: Shuts down the server
- Handles two operations:
  - `QUERY <service_name>`: Returns IP and port for a service
  - `PUT <service_name> <ip> <port>`: Registers a service

**Default Port**: 9160

### 2. Publisher-Subscriber Pattern

#### Publisher (`Publisher` class)
Hosts data that multiple subscribers can access concurrently.

**Use Case**: Broadcasting data to multiple consumers (e.g., sensor data, configuration)

**Key Features**:
- Stores data in a dictionary (`self.data`)
- Supports concurrent subscribers
- Auto-registers with DNS server

**Methods**:
- `start()`: Begins serving data
- `data`: Dictionary containing the data to serve
- Subscribers can request `"keys"` to get available data keys

#### Subscriber (`Subscriber` class)
Connects to a publisher to retrieve data.

**Methods**:
- `connect()`: Establishes connection to publisher
- `get(keyword)`: Retrieves data for a specific key
- `close()`: Disconnects from publisher

### 3. Service-Consumer Pattern (RPC-style)

#### Service (`Service` class)
Exposes callable methods over the network.

**Use Case**: Remote procedure calls, distributed computing

**Key Features**:
- Exposes methods that can be called remotely
- Built-in methods: `print()` and `eval()` (⚠️ Security risk!)
- Custom methods via `add_method(name, function)`

**Methods**:
- `start()`: Begins serving methods
- `add_method(name, callable)`: Adds a new callable method

#### Consumer (`Consumer` class)
Calls methods on remote services.

**Methods**:
- `get(method_name, **kwargs)`: Calls a remote method with arguments

## Usage Examples

### Basic Setup

```python
from NetOperators import *

# 1. Start DNS Server
dns = DNS(ip="127.0.0.1", port=5050)
dns.open()

# 2. Create and start a Publisher
pub = Publisher(
    ip="127.0.0.1", 
    port=6060, 
    DNS_ip="127.0.0.1", 
    DNS_port=5050,
    name="data_publisher"
)
pub.data = {"temperature": 25.5, "humidity": 60}
pub.start()

# 3. Create and use a Subscriber
sub = Subscriber(pub_name="data_publisher", DNS_ip="127.0.0.1", DNS_port=5050)
sub.connect()
temperature = sub.get("temperature")
sub.close()
```

### Service-Consumer Example

```python
# 1. Create a Service
service = Service(
    ip="127.0.0.1", 
    port=7070,
    DNS_ip="127.0.0.1", 
    DNS_port=5050,
    name="math_service"
)

# Add custom method
def factorial(n):
    return 1 if n <= 1 else n * factorial(n-1)

service.add_method("factorial", factorial)
service.start()

# 2. Use the Service
consumer = Consumer(service_name="math_service", DNS_ip="127.0.0.1", DNS_port=5050)
result = consumer.get("factorial", n=5)  # Returns 120
```

## Communication Protocols

### DNS Protocol
- **Query**: `"QUERY <service_name>"` → Returns pickled (ip, port) tuple
- **Register**: `"PUT <service_name> <ip> <port>"` → Returns status code

### Publisher Protocol
- Client sends keyword as string
- Server responds with: `<16-digit-length><pickled_data>`
- Special keyword `"keys"` returns list of available keys

### Service Protocol
- Client sends: `<16-digit-command-length><command_string><pickled_kwargs>`
- Command format: `"<method_name> <kwargs_length>"`
- Server responds: `<16-digit-length><pickled_result>`

## Key Features

### Strengths
- **Service Discovery**: Automatic service registration and lookup
- **Concurrent Handling**: Thread pools for handling multiple clients
- **Flexible Data Types**: Pickle serialization supports complex Python objects
- **Error Handling**: Comprehensive error codes and exception handling

### Security Concerns ⚠️
- **Critical**: Built-in `eval()` method in Service class allows arbitrary code execution
- **Network Security**: No authentication or encryption
- **Pickle Risks**: Pickle deserialization can execute arbitrary code

## Utility Functions

- `get_ip(DNS_ip, DNS_port, service_name)`: Looks up service location
- `publish_ip(service_name, ip, port, DNS_ip, DNS_port)`: Registers service
- `test_active_conn(port)`: Finds next available port

## Error Codes

### DNS Responses
- `"0"`: Success
- `"1"`: Invalid action
- `"2"`: Could not save IP
- `"-1"`: Service not found / Invalid data
- `"-2"`: Server processing error

## Configuration

All components support customizable:
- IP addresses (default: "localhost")
- Ports (various defaults)
- Timeouts (default: 10-20 seconds)
- Maximum concurrent connections

## Threading Model

- DNS server uses ThreadPoolExecutor for handling connections
- Publishers and Services use ThreadPoolExecutor for concurrent clients
- All network operations are handled in separate threads
- Daemon threads ensure clean shutdown

## Recommended Improvements

1. **Remove `eval()` method** for security
2. **Add authentication/authorization**
3. **Implement SSL/TLS encryption**
4. **Replace pickle with JSON** for safer serialization
5. **Add comprehensive logging**
6. **Implement health checks**
7. **Add service versioning**
8. **Improve error handling and recovery**
yes obviously this is very much AI. Credit: Claude 4 Sonnet. this is VERY old, VERY undocumented, VERY likely bad code.
still a neat idea tho, so I wanna develop it more.