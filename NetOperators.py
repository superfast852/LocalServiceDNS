import socket
import msgpack
import struct
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional, Any, Tuple
from collections import deque
from betterlog import logging, setup_enhanced_logging

# Configure logging
setup_enhanced_logging("INFO", "logs/app.log", True, True, True, 30)
logger = logging.getLogger(__name__)

# Function to publish a service into the DNS server.
ERROR_MSGS = [
    "Transaction successful.",
    "The sent data was malformed or could not be interpreted by the DNS.",
    "A bad request was sent to the server.",
    "The server ran into a problem trying to process this request."
]


def send_data_with_length(sock: socket.socket, data: bytes) -> None:
    """
    Helper function to send data with length prefix.
    NOTE: msgpack does not have a native tuple method. Therefore, all tuples get converted to lists on transfer.
    """
    length = struct.pack('<I', len(data))  # 4-byte little-endian length
    sock.sendall(length + data)


def recv_data_with_length(sock: socket.socket, timeout: Optional[float] = None) -> bytes:
    """Helper function to receive data with length prefix"""
    if timeout:
        sock.settimeout(timeout)

    # Receive length (4 bytes)
    length_data = b''
    while len(length_data) < 4:
        chunk = sock.recv(4 - len(length_data))
        if not chunk:
            raise ConnectionError("Connection closed while receiving length")
        length_data += chunk

    length = struct.unpack('<I', length_data)[0]

    # Receive data
    data = b''
    while len(data) < length:
        chunk = sock.recv(min(length - len(data), 4096))
        if not chunk:
            raise ConnectionError("Connection closed while receiving data")
        data += chunk

    return data


# Function to fetch the location of a service from the server.
def get_ip(DNS_ip: str, DNS_port: int, service: str) -> tuple:
    """Get service IP and port from DNS server"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((DNS_ip, DNS_port))
            cmd = f"QUERY {service}".encode('utf-8')
            send_data_with_length(sock, cmd)

            # Receive response code
            code_data = sock.recv(1)
            if not code_data:
                raise ConnectionError("No response from DNS server")

            code = code_data[0]
            if code != 0:
                raise ConnectionError(ERROR_MSGS[code] if code < len(ERROR_MSGS) else "Unknown error")

            # Receive and unpack response
            response_data = recv_data_with_length(sock)
            return tuple(msgpack.unpackb(response_data, raw=False))

        except socket.error as e:
            raise ConnectionError(f'Could not reach DNS: {e}')
        except Exception as e:
            logger.error(f"Failed to get IP: {e}")
            raise


def publish_ip(service_name: str, ip: str, port: int, DNS_ip: str, DNS_port: int) -> None:
    """Publish service to DNS server"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((DNS_ip, DNS_port))
            cmd = f"PUT {service_name} {ip} {port}".encode('utf-8')
            send_data_with_length(sock, cmd)

            response_data = sock.recv(1)
            if not response_data:
                raise ConnectionError("No response from DNS server")

            response = response_data[0]
            if response != 0:
                raise ConnectionError(ERROR_MSGS[response] if response < len(ERROR_MSGS) else "Unknown error")

        except socket.error as e:
            raise ConnectionError(f'Could not reach DNS: {e}')


class DNS:
    """
    DNS Server for service discovery

    Error codes:
    0: Transaction successful
    1: invalid data
    2: invalid request
    3: server error
    """

    def __init__(self, port: int = 9160, ip: str = "localhost", max_clients: int = 10, timeout: int = 10):
        self.ip = ip
        self.port = port
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.port))
        self.is_opened = False
        self.executor = ThreadPoolExecutor(max_workers=max_clients)
        self.services = {}
        self.services_lock = Lock()  # Thread safety for services dict

    def open(self) -> None:
        """Start the DNS server"""
        if not self.is_opened:
            self.sock.listen(5)
            self.is_opened = True
            Thread(target=self._conn_monitor, daemon=True).start()
            logger.info(f"DNS server started on {self.ip}:{self.port}")
        else:
            raise ConnectionError('DNS is already open')

    def _conn_monitor(self) -> None:
        """Monitor for incoming connections"""
        while self.is_opened:
            try:
                sock, address = self.sock.accept()
                self.executor.submit(self._handler, sock, address)
            except socket.error as e:
                if self.is_opened:
                    logger.error(f"DNS connection error: {e}")
                break

    def close(self) -> None:
        """Close the DNS server"""
        if self.is_opened:
            self.is_opened = False
            self.executor.shutdown(wait=True)
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # Socket might already be closed
            self.sock.close()
            logger.info("DNS server closed")

    def _handler(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        """Handle client requests"""
        conn.settimeout(self.timeout)

        try:
            # Receive command
            data = recv_data_with_length(conn, self.timeout)
            command_str = data.decode('utf-8')

            if not command_str:
                conn.send(b'\x02')  # Invalid request
                return

            parts = command_str.split()
            if not parts:
                conn.send(b'\x02')  # Invalid request
                return

            action = parts[0]

            if action == "QUERY" and len(parts) == 2:
                service_name = parts[1]
                with self.services_lock:
                    address = self.services.get(service_name)

                if address is not None:
                    response_data = msgpack.packb(address)
                    conn.send(b'\x00')  # Success
                    send_data_with_length(conn, response_data)
                else:
                    conn.send(b'\x01')  # Service not found

            elif action == "PUT" and len(parts) == 4:
                _, service_name, ip, port_str = parts
                try:
                    port = int(port_str)
                    with self.services_lock:
                        self.services[service_name] = (ip, port)
                    conn.send(b'\x00')  # Success
                    logger.info(f"Registered service {service_name} at {ip}:{port}")
                except ValueError:
                    conn.send(b'\x01')  # Invalid data
            else:
                conn.send(b'\x02')  # Invalid request

        except Exception as e:
            logger.error(f"DNS handler error: {e}")
            try:
                conn.send(b'\x03')  # Server error
            except:
                pass
        finally:
            try:
                conn.close()
            except:
                pass


class Service:
    """RPC Service that can expose methods for remote calls"""

    def __init__(self, name: str, port: int, ip: str = "localhost", max_clients: int = 10, timeout: int = 20):
        self.name = name
        self.port = port
        self.ip = ip
        self.functions = {}
        self.max_clients = max_clients
        self.timeout = timeout

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.port))
        self.is_opened = False
        self.executor = ThreadPoolExecutor(max_workers=max_clients)

    def expose(self, func: Callable) -> Callable:
        """Expose a function for remote calls"""
        if self.is_opened:
            raise RuntimeError("Cannot register new functions after service is opened")
        self.functions[func.__name__] = func
        return func

    def _register(self, dns_port: int = 9160, dns_ip: str = "localhost") -> None:
        """Register service with DNS"""
        publish_ip(self.name, self.ip, self.port, dns_ip, dns_port)

    def open(self, dns_port: int = 9160, dns_ip: str = "localhost") -> None:
        """Start the service and register with DNS"""
        if not self.is_opened:
            self.functions["list"] = lambda: list(self.functions.keys())
            self.is_opened = True
            self.sock.listen(self.max_clients)
            Thread(target=self._router, daemon=True).start()
            self._register(dns_port, dns_ip)
            logger.info(f"Service {self.name} started on {self.ip}:{self.port}")
        else:
            raise ConnectionError("Service is already open")

    def _router(self) -> None:
        """Route incoming connections to handlers"""
        while self.is_opened:
            try:
                conn, addr = self.sock.accept()
                self.executor.submit(self._handle_request, conn, addr)
            except socket.error:
                break

    def _handle_request(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        """Handle a single RPC request"""
        conn.settimeout(self.timeout)

        try:
            # Receive request
            data = recv_data_with_length(conn, self.timeout)
            request = msgpack.unpackb(data, raw=False)

            # Request format: [func_name, args, kwargs]
            if not isinstance(request, list) or len(request) != 3:
                result = ("error", "Invalid request format")
            else:
                func_name, args, kwargs = request
                if func_name not in self.functions:
                    result = ("error", f"Function {func_name} not found")
                else:
                    try:
                        result = self.functions[func_name](*args, **kwargs)
                        if result is None:
                            result = True
                    except Exception as e:
                        result = ("error", str(e))

            # Send response
            response_data = msgpack.packb(result)
            send_data_with_length(conn, response_data)

        except Exception as e:
            logger.error(f"Service request handling error: {e}")
        finally:
            try:
                conn.close()
            except:
                pass

    def close(self) -> None:
        """Close the service"""
        if self.is_opened:
            self.is_opened = False
            self.executor.shutdown(wait=True)
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
            logger.info(f"Service {self.name} closed")


class Consumer:
    """Client for consuming RPC services"""

    def __init__(self, service_name: str, dns_port: int = 9160, dns_ip: str = "localhost"):
        self.service_name = service_name
        self.service_address = get_ip(dns_ip, dns_port, service_name)

        # Get list of available functions
        try:
            self.functions = set(self._send("list"))
        except Exception as e:
            logger.error(f"Failed to get function list for {service_name}: {e}")
            self.functions = set()

    def _send(self, func_name: str, *args, **kwargs) -> Any:
        """Send RPC request to service"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(self.service_address)

                # Prepare and send request
                request = [func_name, args, kwargs]
                request_data = msgpack.packb(request)
                send_data_with_length(sock, request_data)

                # Receive response
                response_data = recv_data_with_length(sock)
                response = msgpack.unpackb(response_data, raw=False)

                # Handle error responses
                if isinstance(response, list) and len(response) == 2 and response[0] == "error":
                    raise RuntimeError(response[1])

                return response

            except socket.error as e:
                raise ConnectionError(f"Failed to connect to service {self.service_name}: {e}")

    def call(self, method: str, *args, **kwargs) -> Any:
        """Call a service method"""
        if method not in self.functions:
            raise ValueError(f"Method '{method}' not available in service '{self.service_name}'")
        return self._send(method, *args, **kwargs)

    def __getattr__(self, name: str) -> Callable:
        """Allow calling service methods directly"""
        if name in self.functions:
            def method_proxy(*args, **kwargs):
                return self._send(name, *args, **kwargs)

            return method_proxy
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}' and service has no method '{name}'")


class Publisher:
    """TCP-based publisher for data streaming"""

    def __init__(self, name: str, port: int, ip: str = "localhost",
                 dns_port: int = 9160, dns_ip: str = "localhost", max_clients: int = 10):
        self.ip = ip
        self.port = port
        self.dns_ip = dns_ip
        self.dns_port = dns_port
        self.max_clients = max_clients
        self.name = name
        self.timeout = 1

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.port))
        self.is_opened = False
        self.data = {}
        self.data_lock = Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_clients)

    def open(self) -> None:
        """Start the publisher and register with DNS"""
        if not self.is_opened:
            self.is_opened = True
            publish_ip(self.name, self.ip, self.port, self.dns_ip, self.dns_port)
            self.sock.listen(self.max_clients)
            Thread(target=self._router, daemon=True).start()
            logger.info(f"Publisher {self.name} started on {self.ip}:{self.port}")
        else:
            raise ConnectionError("Publisher is already open")

    def _router(self) -> None:
        """Route incoming connections"""
        while self.is_opened:
            try:
                conn, addr = self.sock.accept()
                self.executor.submit(self._handle_subscriber, conn, addr)
            except Exception as e:
                if self.is_opened:
                    logger.error(f"Publisher {self.name} router error: {e}")
                break

    def _handle_subscriber(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        """Handle subscriber requests"""
        if self.timeout > 0:
            conn.settimeout(self.timeout)

        try:
            with conn:
                while self.is_opened:
                    try:
                        # Receive key request (max 256 bytes)
                        key_data = conn.recv(256)
                        if not key_data:
                            break

                        key = key_data.decode('utf-8').strip()

                        if key == "keys":
                            with self.data_lock:
                                result = list(self.data.keys())
                        else:
                            with self.data_lock:
                                result = self.data.get(key)
                                if callable(result):
                                    result = result()

                        # Send response
                        response_data = msgpack.packb(result)
                        send_data_with_length(conn, response_data)

                    except socket.timeout:
                        continue
                    except Exception as e:
                        logger.error(f"Publisher subscriber handling error: {e}")
                        break
        except Exception as e:
            logger.error(f"Publisher connection error: {e}")

    def close(self) -> None:
        """Close the publisher"""
        if self.is_opened:
            self.is_opened = False
            self.executor.shutdown(cancel_futures=True)
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
            logger.info(f"Publisher {self.name} closed")

    def add_key(self, name: str, data: Any = None) -> None:
        """Add data key"""
        with self.data_lock:
            self.data[name] = data

    def __getitem__(self, item: str) -> Any:
        with self.data_lock:
            return self.data[item]

    def __setitem__(self, key: str, val: Any) -> None:
        with self.data_lock:
            self.data[key] = val

    def register_function(self, func: Callable) -> Callable:
        """Register a function to be called when requested"""
        with self.data_lock:
            self.data[func.__name__] = func
        return func


class Subscriber:
    """Client for subscribing to publisher data"""

    def __init__(self, pub_name: str, dns_port: int = 9160, dns_ip: str = "localhost"):
        self.pub_name = pub_name
        self.pub_address = get_ip(dns_ip, dns_port, pub_name)
        self.sock = None
        self.is_opened = False

    def open(self) -> None:
        """Connect to publisher"""
        if not self.is_opened:
            self.is_opened = True
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect(self.pub_address)
            logger.info(f"Subscribed to publisher {self.pub_name}")

    def __getitem__(self, item: str) -> Any:
        """Get data from publisher"""
        if not self.is_opened:
            raise ValueError("Subscriber not connected")

        try:
            # Send key request
            self.sock.send(item.encode("utf-8"))

            # Receive response
            response_data = recv_data_with_length(self.sock)
            return msgpack.unpackb(response_data, raw=False)

        except Exception as e:
            self.is_opened = False
            if self.sock:
                self.sock.close()
            raise ConnectionError(f"Connection lost: {e}")

    def close(self) -> None:
        """Close subscriber connection"""
        if self.is_opened:
            try:
                self.sock.send(b'')  # Signal disconnect
            except:
                pass
            self.sock.close()
            self.is_opened = False
            logger.info(f"Unsubscribed from publisher {self.pub_name}")


class UDPPublisher:
    """UDP-based publisher for high-frequency data broadcasting"""

    def __init__(self, name: str, port: int, ip: str = "localhost",
                 dns_port: int = 9160, dns_ip: str = "localhost", auto_publish: bool = False):
        self.name = name
        self.ip = ip
        self.port = port
        self.dns_ip = dns_ip
        self.dns_port = dns_port
        self.auto_publish = auto_publish

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.port))

        self.is_opened = False
        self.subscribers = set()
        self.subscribers_lock = Lock()
        self.data_source = None

    def set_data_source(self, data_source: Callable) -> None:
        """Set function to provide data for publishing"""
        self.data_source = data_source

    def add_subscriber(self, subscriber_ip: str, subscriber_port: int) -> None:
        """Manually add subscriber"""
        with self.subscribers_lock:
            self.subscribers.add((subscriber_ip, subscriber_port))

    def remove_subscriber(self, subscriber_ip: str, subscriber_port: int) -> None:
        """Remove subscriber"""
        with self.subscribers_lock:
            self.subscribers.discard((subscriber_ip, subscriber_port))

    def open(self) -> None:
        """Start publisher and register with DNS"""
        if not self.is_opened:
            self.is_opened = True
            publish_ip(self.name, self.ip, self.port, self.dns_ip, self.dns_port)
            Thread(target=self._handle_subscriptions, daemon=True).start()

            if self.auto_publish and self.data_source:
                Thread(target=self._auto_publish, daemon=True).start()

            logger.info(f"UDP Publisher {self.name} started on {self.ip}:{self.port}")
        else:
            raise ConnectionError("UDP Publisher is already open")

    def _handle_subscriptions(self) -> None:
        """Handle subscription requests"""
        while self.is_opened:
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode('utf-8').strip()

                if message == "SUBSCRIBE":
                    self.sock.sendto(b"SUBSCRIBED", addr)
                    with self.subscribers_lock:
                        self.subscribers.add(addr)
                    logger.info(f"UDP Publisher {self.name}: Added subscriber {addr}")

                elif message == "UNSUBSCRIBE":
                    self.sock.sendto(b"UNSUBSCRIBED", addr)
                    with self.subscribers_lock:
                        self.subscribers.discard(addr)
                    logger.info(f"UDP Publisher {self.name}: Removed subscriber {addr}")

            except socket.error as e:
                if self.is_opened:
                    logger.error(f"UDP Publisher subscription handling error: {e}")
                break

    def publish(self, data: Any = None) -> None:
        """Publish data to all subscribers"""
        if not self.is_opened:
            raise ConnectionError("Publisher is not open")

        if data is None and self.data_source:
            try:
                data = self.data_source()
            except Exception as e:
                logger.error(f"Error getting data from source: {e}")
                return

        if data is not None:
            try:
                serialized_data = msgpack.packb(data)

                with self.subscribers_lock:
                    disconnected = set()
                    for subscriber_addr in self.subscribers:
                        try:
                            self.sock.sendto(serialized_data, subscriber_addr)
                        except socket.error as e:
                            logger.error(f"Failed to send to {subscriber_addr}: {e}")
                            disconnected.add(subscriber_addr)

                    # Remove disconnected subscribers
                    self.subscribers -= disconnected
            except Exception as e:
                logger.error(f"Error publishing data: {e}")

    def _auto_publish(self) -> None:
        """Automatically publish data from data source"""
        while self.is_opened:
            try:
                self.publish()
            except Exception as e:
                logger.error(f"Auto-publish error: {e}")

    def close(self) -> None:
        """Close the UDP publisher"""
        if self.is_opened:
            self.is_opened = False
            self.sock.close()
            logger.info(f"UDP Publisher {self.name} closed")


class UDPSubscriber:
    """Client for receiving UDP broadcasts"""

    def __init__(self, publisher_name: str, buffer_size: int = 100,
                 dns_port: int = 9160, dns_ip: str = "localhost"):
        self.publisher_name = publisher_name
        self.buffer_size = buffer_size
        self.dns_ip = dns_ip
        self.dns_port = dns_port

        self.pub_loc = get_ip(dns_ip, dns_port, publisher_name)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', 0))  # Bind to any available port
        self.local_port = self.sock.getsockname()[1]

        self.is_opened = False
        self.message_buffer = deque(maxlen=buffer_size)
        self.buffer_lock = Lock()

    def subscribe(self) -> None:
        """Subscribe to UDP publisher"""
        if not self.is_opened:
            try:
                self.sock.sendto(b"SUBSCRIBE", self.pub_loc)

                # Wait for acknowledgment
                self.sock.settimeout(5.0)
                ack, addr = self.sock.recvfrom(1024)
                if ack == b"SUBSCRIBED":
                    self.pub_loc = addr
                    self.is_opened = True
                    self.sock.settimeout(None)
                    Thread(target=self._listen_for_data, daemon=True).start()
                    logger.info(f"Subscribed to UDP publisher {self.publisher_name}")
                else:
                    raise ConnectionError("Failed to receive subscription acknowledgment")

            except socket.timeout:
                raise ConnectionError("Subscription request timed out")
            except Exception as e:
                raise ConnectionError(f"Failed to subscribe: {e}")
        else:
            raise ConnectionError("Already subscribed")

    def _listen_for_data(self) -> None:
        """Listen for incoming UDP data"""
        while self.is_opened:
            try:
                data, addr = self.sock.recvfrom(65536)
                # print(data, addr)
                if addr == self.pub_loc:  # Verify sender
                    try:
                        deserialized_data = msgpack.unpackb(data, raw=False)
                        with self.buffer_lock:
                            self.message_buffer.append(deserialized_data)
                    except Exception as e:
                        logger.error(f"Failed to deserialize UDP data: {e}")

            except socket.error as e:
                if self.is_opened:
                    logger.error(f"UDP Subscriber data reception error: {e}")
                break

    def get_latest_message(self) -> Optional[Any]:
        """Get most recent message"""
        with self.buffer_lock:
            return self.message_buffer[-1] if self.message_buffer else None

    def get_all_messages(self) -> list:
        """Get all buffered messages"""
        with self.buffer_lock:
            return list(self.message_buffer)

    def get_buffer_size(self) -> int:
        """Get current buffer size"""
        with self.buffer_lock:
            return len(self.message_buffer)

    def clear_buffer(self) -> None:
        """Clear message buffer"""
        with self.buffer_lock:
            self.message_buffer.clear()

    def unsubscribe(self) -> None:
        """Unsubscribe from publisher"""
        if self.is_opened:
            try:
                self.sock.sendto(b"UNSUBSCRIBE", self.pub_loc)
                self.is_opened = False
                logger.info(f"Unsubscribed from UDP publisher {self.publisher_name}")
            except Exception as e:
                logger.error(f"Error during unsubscription: {e}")
            finally:
                self.sock.close()


# Example usage and testing
if __name__ == '__main__':
    import os
    import random
    import time

    # Start DNS server
    dns = DNS()
    dns.open()

    # Create and start services
    service = Service("test", 9161)
    udp_pub = UDPPublisher("sensor_data", 9200)
    pub = Publisher("sensor_store", 9201)


    @service.expose
    def get_current_dir():
        return os.getcwd()


    @service.expose
    def harmonic_mean(first: float, second: float = 2.0) -> float:
        if first + second == 0:
            raise ValueError("Sum cannot be zero")
        return (2 * first * second) / (first + second)


    def get_sensor_data():
        return {
            "temperature": random.uniform(20, 30),
            "humidity": random.uniform(40, 80),
            "timestamp": time.time()
        }


    udp_pub.set_data_source(get_sensor_data)
    pub["counter"] = 0
    pub["random"] = lambda: random.random()
    pub["timestamp"] = lambda: time.time()

    # Start all services
    service.open()
    udp_pub.open()
    pub.open()

    # Create clients
    consumer = Consumer("test")  # consumers don't need to open, as they don't hold a persistent connection.
    udp_sub = UDPSubscriber("sensor_data", buffer_size=50)
    subscriber = Subscriber("sensor_store")

    # Test the services
    subscriber.open()
    udp_sub.subscribe()
    time.sleep(3)
    print("Testing RPC service (consumer):")
    print("Current directory:", consumer.call("get_current_dir"))
    print("Harmonic mean:", consumer.harmonic_mean(4.0, 8.0))

    try:
        print("Testing error handling:", consumer.harmonic_mean(4.0, -4.0))
    except Exception as e:
        print("Caught expected exception:", e)

    print("\nTesting UDP publisher:")
    for i in range(3):
        udp_pub.publish()
        time.sleep(0.5)

    print("Latest UDP message:", udp_sub.get_latest_message())
    print("UDP buffer size:", udp_sub.get_buffer_size())

    print("\nTesting TCP publisher:")
    print("Counter value:", subscriber["counter"])
    print("Random value:", subscriber["random"])
    print("Current timestamp:", subscriber["timestamp"])

    # Cleanup
    print("\nCleaning up...")
    subscriber.close()
    udp_sub.unsubscribe()

    pub.close()
    service.close()
    udp_pub.close()
    dns.close()

    print("All services shut down successfully!")
