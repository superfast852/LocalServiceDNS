from time import sleep
from NetOperators import *
from atexit import register

DNS_PORT = test_active_conn(5050)
is_open = True

def run_dns():
    dns_server = DNS(ip="127.0.0.1", port=DNS_PORT)
    register(dns_server.close)
    dns_server.open()
    print("[RUN_DNS] DNS server running at 127.0.0.1:5050")
    return dns_server


def run_publisher(publisher_name, port):
    publisher = Publisher(ip="127.0.0.1", port=port, DNS_ip="127.0.0.1", DNS_port=DNS_PORT, name=publisher_name)
    register(publisher.close)
    publisher.data = {
        "key1": "value1",
        "key2": [1, 2, 3],
        "key3": {"subkey": "subvalue"}
    }
    publisher.start()
    print(f"[RUN_PUBLISHER] Publisher '{publisher_name}' started at 127.0.0.1:{port}")
    return publisher


def run_subscriber(publisher_name):
    print(f"Querying {publisher_name}")
    subscriber = Subscriber(pub_name=publisher_name, DNS_ip="127.0.0.1", DNS_port=DNS_PORT)
    subscriber.connect()
    print(f"[RUN_SUBSCRIBER] Subscriber connected to {publisher_name}")
    return subscriber


def subscriber_loop(subscriber):
    while is_open:
        try:
            print(f"[SUBSCRIBER] Keys: {subscriber.get('keys')}")
            print(subscriber.get("key1"), subscriber.get("key2"), subscriber.get("key3"))
        except Exception as e:
            print(f"[SUBSCRIBER ERROR] {e}")
            break
        sleep(1)  # Adjust frequency as needed


def run_service(service_name, port):
    service = Service("127.0.0.1", port, "127.0.0.1", DNS_PORT, service_name)

    def fact(x):
        if x == 0:
            return 1
        return fact(x - 1) * x

    def print_text(text):
        return f"Printed: {text}"

    register(service.close)
    service.add_method("fact", fact)
    service.add_method("print", print_text)
    service.start()
    print(f"[RUN_SERVICE] Service '{service_name}' started at 127.0.0.1:{port}")
    return service


def run_consumer(service_name):
    consumer = Consumer(service_name=service_name, DNS_ip="127.0.0.1", DNS_port=DNS_PORT)

    return consumer


def consumer_loop(consumer):
    while is_open:
        try:
            result = consumer.get("print", text="Hello from consumer")
            print(f"[CONSUMER] Result: {result}")
            result = consumer.get("fact", x=90)
            print(f"[CONSUMER] Factorial result: {result}")
        except Exception as e:
            print(f"[CONSUMER ERROR] {e}")
            break
        sleep(1)  # Adjust frequency as needed


# Main test script
if __name__ == "__main__":
    # Start the DNS server in a separate thread
    dns = run_dns()
    sleep(1)  # Give DNS time to start
    print("Started DNS")

    publishers = []
    services = []
    subscribers = []
    consumers = []


    pub_threads = []
    sub_threads = []
    service_threads = []


    for i in range(10):
        s = run_service(f"service_{i}", DNS_PORT+1010+i)
        services.append(s)
    sleep(3)
    for i in range(10):
        pub = run_publisher(f"publisher_{i}", DNS_PORT+2020+i)
        publishers.append(pub)
    sleep(1)
    for i in range(10):
        c = run_consumer(f"service_{i}")
        consumers.append(c)
        Thread(target=consumer_loop, args=(consumers[i], )).start()
    sleep(1)
    for i in range(10):
        sub = run_subscriber(f"publisher_{i}")
        subscribers.append(sub)
        Thread(target=subscriber_loop, args=(subscribers[i], )).start()
    sleep(10)


    dns.close()
    is_open = False
    for i in range(10):
        # Consumers are single handshake objects.
        services[i].close()
        publishers[i].close()
        subscribers[i].close()