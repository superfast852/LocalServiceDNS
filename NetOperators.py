# TODO: Migrate to JSON. First check if it works. Also add proper logging. Add a Reset method (reinstantiating sockets)

import socket
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from _pickle import dumps, loads
from typing import Callable

SERVICES = {}
flag = "congrats! you got access to globals."

class DNS(object):
    def __init__(self, ip: str = "localhost", port: int = 9160, max_clients: int = 10, timeout: int = 10):
        self.ip = ip
        self.port = port
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        # self.sock.settimeout(10)
        self.sock.bind((self.ip, self.port))
        self.is_opened = False
        self.executor = ThreadPoolExecutor(max_workers=max_clients)

    def open(self):
        if not self.is_opened:
            self.sock.listen(5)
            self.is_opened = True
            Thread(target=self._conn_monitor, daemon=True).start()
        else:
            raise ConnectionError('DNS is already open')

    def _conn_monitor(self):
        while self.is_opened:
            try:
                sock, address = self.sock.accept()
                self.executor.submit(self.handler, sock, address)
                # Thread(target=self.handler, args=(sock, address), daemon=True).start()
            except socket.error as e:
                print(f"[DNS ERROR] {e}")
                self.close()

    def close(self):
        if self.is_opened:
            self.is_opened = False
            self.executor.shutdown(wait=True)
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()

    def handler(self, conn: socket.socket, addr: tuple):
        conn.settimeout(self.timeout)
        sent_data = 0
        mode = ""  # If empty, means a process errored out.
        try:
            print(f"[DNS]Handling connection from {addr}")
            info = conn.recv(1024).decode('utf-8')
            if not info:
                return False
            info = info.split(" ")
            action, data = info[0], info[1:]
            if action == "QUERY":
                # In this case, data should be a single item, so:
                address = SERVICES.get(data[0], ("-1", 0))
                if address != ("-1", 0):
                    mode = action
                    address = dumps(address)
                    conn.send(str(len(address)).zfill(16).encode('utf-8'))
                    sent_data += 1
                    conn.send(address)
                    sent_data += 1
                else:
                    conn.send("-1".encode('utf-8'))
                    sent_data += 1
            elif action == "PUT":
                name, ip, port = data
                try:
                    SERVICES[name] = (ip, int(port))
                    mode = action
                    conn.send("0".encode('utf-8'))
                    sent_data += 1
                except ValueError:
                    conn.send("-1".encode('utf-8'))
                    sent_data += 1
                except Exception as e:
                    print(f"[ERROR] Could not save IP: {data}\n\tException: {e}")
                    conn.send("2".encode('utf-8'))
                    sent_data += 1
            else:
                conn.send("1".encode('utf-8'))
        except Exception as e:
            print("[DNS ERROR]", e, sent_data, mode)
            if mode == "QUERY":
                if sent_data == 0:
                    conn.send("-2".encode('utf-8'))
                elif sent_data == 1:
                    conn.send(dumps(-2))
            elif mode == "PUT":
                if sent_data == 0:
                    conn.send("-2".encode('utf-8'))
            else:
                conn.send("-2".encode('utf-8'))
        finally:
            try:
                conn.close()
            except Exception as e:
                pass


# Active connection objects
class Publisher(object):
    def __init__(self, ip: str = "localhost", port: int = 7070, DNS_ip: str = "localhost", DNS_port: int = 9160,
                 name: str = "test_pub", max_subs: int = 10, timeout=10):
        self.ip = ip
        self.port = port
        self.DNS_ip = DNS_ip
        self.DNS_port = DNS_port
        self.max_subs = max_subs
        self.name = name
        self.timeout = 10

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        self.sock.bind((self.ip, self.port))
        self.is_opened = False
        self.data = {}  # The data to make available to subscribers
        self.running_clients = []  # A list containing all the active subscribers, for bookkeeping, and safe closing.
        self.executor = ThreadPoolExecutor(max_workers=max_subs)

    def start(self):
        if not self.is_opened:
            self.is_opened = True
            publish_ip(self.name, self.ip, self.port, self.DNS_ip, self.DNS_port)  # Open the publisher to the public
            Thread(target=self._mtr_conns, daemon=True).start()  # start the connection rerouter.
        else:
            raise ConnectionError('Publisher is already open')

    def _mtr_conns(self):
        self.sock.listen(self.max_subs)  # Open the server
        while self.is_opened:
            try:
                conn, addr = self.sock.accept()  # Get a connection and send it to its own thread.
                self.executor.submit(self.host_data, conn, addr)
            except socket.error:
                break

    def host_data(self, conn: socket, addr: tuple):
        conn.settimeout(self.timeout)
        try:
            self.running_clients.append(addr)
            while self.is_opened:
                kwrd = conn.recv(1024).decode()  # get the wanted operation
                if kwrd == '':  # if empty, close.
                    break
                elif kwrd == "keys":
                    out = dumps(list(self.data.keys()))
                else:
                    out = dumps(self.data.get(kwrd, []), -1)
                conn.send(str(len(out)).zfill(16).encode('utf-8'))
                conn.send(out)
        except Exception as e:
            print("[DNS ERROR]", e, addr)
        finally:  # try a clean exit.
            self.running_clients.remove(conn)
            conn.close()

    def close(self):
        if self.is_opened:
            self.is_opened = False  # set all threads to leave
            self.executor.shutdown()
            # clean shutdown
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()


class Subscriber(object):
    def __init__(self, pub_name: str = "test_pub", DNS_ip: str = "localhost", DNS_port: int = "9160", timeout=10):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.is_opened = False
        self.ip, self.port = get_ip(DNS_ip, DNS_port, pub_name)
        if (self.ip, self.port) == ("-1", 0):
            raise ConnectionError('Publisher does not exist.')
        print(f"[Subscriber: {pub_name}] IP: {self.ip} Port: {self.port}")
        self.sock.settimeout(timeout)

    def connect(self):
        if not self.is_opened:
            print(f"[SUBSCRIBER] Available services: {SERVICES}")
            self.is_opened = True
            self.sock.connect((self.ip, self.port))

    def get(self, keyword: str):
        if self.is_opened:
            self.sock.send(keyword.encode('utf-8'))
            size = self.sock.recv(16).decode('utf-8')
            return loads(self.sock.recv(int(size)))

    def close(self):
        if self.is_opened:
            try:
                self.sock.send(''.encode('utf-8'))
            except BrokenPipeError:
                pass
            self.is_opened = False
            self.sock.close()


# Instance-Connection objects
class Service(object):
    '''
    This is a class meant to be inherited. The idea is for a class to inherit this, and expose many functions.
    For closure reasons, all methods must return something. Also make sure it's hashable for pickle.
    '''

    def __init__(self, ip: str = "localhost", port: int = 6060, DNS_ip: str = "localhost", DNS_port: int = 9160,
                 name: str = "test_service", max_subs: int = 10, timeout=20):
        print("[WARNING!!!] EVAL IS STILL IN METHODS.")
        self.ip = ip
        self.port = port
        self.DNS_ip = DNS_ip
        self.DNS_port = DNS_port
        self.max_subs = max_subs
        self.name = name
        self.timeout = timeout

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        self.sock.bind((self.ip, self.port))
        self.is_opened = False
        self.methods = {"print": lambda text: print(text), "eval": lambda x: eval(x, {'self': self})}
        self.running_clients = []  # A list containing all the active subscribers, for bookkeeping, and safe closing.
        self.executor = ThreadPoolExecutor(max_workers=max_subs)

    def add_method(self, name: str, method: Callable):
        self.methods[name] = method

    def start(self):
        if not self.is_opened:
            self.is_opened = True
            publish_ip(self.name, self.ip, self.port, self.DNS_ip, self.DNS_port)  # Open the publisher to the public
            Thread(target=self._mtr_conns, daemon=True).start()  # start the connection rerouter.

        else:
            raise ConnectionError('Publisher is already open')

    def _mtr_conns(self):
        self.sock.listen(self.max_subs)  # Open the server
        while self.is_opened:
            try:
                conn, addr = self.sock.accept()  # Get a connection and send it to its own thread.
                #conn.settimeout(self.timeout)
                self.executor.submit(self.host_data, conn, addr)
            except socket.error:
                break

    def void(self, **kwargs):
        return "Function does not exist."

    def host_data(self, conn: socket, addr: tuple):  # Requests are in 2 parts. First is name and size, second is kwargs
        try:
            conn.settimeout(self.timeout)
            self.running_clients.append(addr)
            command_size = conn.recv(16).decode('utf-8')
            data = conn.recv(int(command_size)).decode('utf-8')
            kwrd, size = data.split(" ")  # get the wanted operation
            if kwrd == '':  # if empty, close.
                return -1
            else:
                out = self.methods.get(kwrd, self.void)  # get the corresponding method
                kwargs = loads(conn.recv(int(size)))  # get the kwargs
                try:
                    result = dumps(out(**kwargs))  # process the function
                except Exception as e:
                    result = dumps(str(e))
                conn.send(str(len(result)).zfill(16).encode('utf-8'))  # and send anyway a result packet
                conn.send(result)
        except Exception as e:
            print(f"[SERVICE ERROR] {e}")
        finally:  # try a clean exit.
            self.running_clients.remove(addr)
            conn.close()

    def close(self):
        if self.is_opened:
            self.is_opened = False  # set all threads to leave
            self.executor.shutdown()
            # clean shutdown
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()


class Consumer(object):
    def __init__(self, service_name: str = "test_service", DNS_ip: str = "localhost", DNS_port: int = 9160  ):
        self.ip, self.port = get_ip(DNS_ip, DNS_port, service_name)
        if (self.ip, self.port) == ("-1", 0):
            raise ConnectionError('Service does not exist.')

    def get(self, method: str, **kwargs):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.ip, self.port))  # connect to service

        sendable_args = dumps(kwargs)  # should be a dict.
        command = f'{method} {len(sendable_args)}'
        len_command = str(len(command)).zfill(16).encode('utf-8')
        sock.send(len_command)  # send length
        sock.send(command.encode('utf-8'))
        sock.send(sendable_args)  # send data

        length = sock.recv(16).decode('utf-8')
        data = loads(sock.recv(int(length)))  # receive and de-serialize
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
        return data


# Function to fetch the location of a service from the server.
def get_ip(DNS_ip: str, DNS_port: int, service: str) -> tuple:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((DNS_ip, DNS_port))
        sock.send(f"QUERY {service}".encode('utf-8'))
        size = int(sock.recv(16).decode('utf-8'))
        # Size error handling
        if size == -1:
            return "-1", 0
        elif size == -2:
            raise ConnectionError("Could not dump data from server.")
        elif size == 1:
            raise ConnectionError("Could not process data. Please check action header.")

        response = sock.recv(size)
        data = loads(response)

        # Error handling
        if data == -2:
            raise ConnectionError("Could not receive data. The pickle dump did not make it through.")

        return data
    except socket.error:
        raise ConnectionError('Could not reach DNS.')
    except Exception as e:
        print(f"[get-ip] Failed to get ip: {e}")
    finally:
        sock.close()


# Function to publish a service into the DNS server.
def publish_ip(service_name, ip, port, DNS_ip, DNS_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((DNS_ip, DNS_port))
        sock.send(f"PUT {service_name} {ip} {port}".encode('utf-8'))
        response = sock.recv(1024).decode('utf-8')
        if response == "0":
            pass
        else:
            if response == "1":
                raise ConnectionError('Request could not be processed.')
            elif response == "2":
                raise ConnectionError('DNS could not save the IP.')
            elif response == "-1":
                raise ConnectionError('Erroneous data was sent. Ensure the service name is compatible.')
            elif response == "-2":
                raise ConnectionError('An error occured on the DNS end processing the request.')
            else:
                raise ConnectionError(response)
    except socket.error:
        raise ConnectionError('Could not reach DNS.')
    finally:
        sock.close()


def test_active_conn(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while True:
        try:
            s.bind(('', port))
            s.close()
            del s
            return port
        except OSError:
            port += 1

