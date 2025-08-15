from time import sleep
from NetOperators import *
from atexit import register

DNS_PORT = test_active_conn(5050)

# Setting up the DNS server
def run_dns():
    dns_server = DNS(ip="127.0.0.1", port=DNS_PORT)
    register(dns_server.close)
    dns_server.open()
    print("[RUN_DNS] DNS server running at 127.0.0.1:5050")
    return dns_server


# Setting up the Publisher
def run_publisher():
    publisher = Publisher(ip="127.0.0.1", port=test_active_conn(6060), DNS_ip="127.0.0.1", DNS_port=DNS_PORT, name="test_publisher")
    register(publisher.close)
    publisher.data = {
        "key1": "value1",
        "key2": [1, 2, 3],
        "key3": {"subkey": "subvalue"}
    }
    publisher.start()
    print("[RUN_PUBLISH] Publisher started at 127.0.0.1:6060")
    return publisher


# Setting up the Subscriber
def run_subscriber():
    subscriber = Subscriber(pub_name="test_publisher", DNS_ip="127.0.0.1", DNS_port=DNS_PORT)
    register(subscriber.close)
    subscriber.connect()
    print("[RUN_SUBSCRIB] Subscriber connected.")
    # Test retrieval of different keys
    print(f"[SUB] Keys: {subscriber.get('keys')}")
    print(subscriber.get("key1"), subscriber.get("key2"), subscriber.get("key3"))
    sleep(1)
    subscriber.close()


def run_service():
    service = Service("127.0.0.1", test_active_conn(7070), "127.0.0.1", DNS_PORT, "test_service")

    def fact(x):
        if x==0:
            return 1
        return fact(x-1)*x

    register(service.close)
    service.add_method("fact", fact)
    service.start()


# Setting up the Consumer
def run_consumer():
    consumer = Consumer(service_name="test_service", DNS_ip="127.0.0.1", DNS_port=DNS_PORT)
    # Test calling a method from the service (if implemented)
    result = consumer.get("print", text="Hello from consumer")
    print("First Result: " + str(result))
    result = consumer.get("fact", x=5)
    print("Second Result: " + str(result))
    print(consumer.get("slam", lidar="RP"))


# Main test script
if __name__ == "__main__":
    # Start the DNS server in a separate thread
    dns_thread = Thread(name="DNS", target=run_dns, daemon=True)
    dns_thread.start()
    sleep(1)  # Give DNS time to start
    print("Started DNS")

    # Start the publisher in a separate thread
    publisher_thread = Thread(name="Publisher", target=run_publisher, daemon=True)
    publisher_thread.start()
    sleep(1)  # Give publisher time to register
    print("Started Publisher")

    # Run the subscriber in a separate thread
    run_subscriber()
    print("Running subscriber")
    sleep(1)

    service_thread = Thread(name="Service", target=run_service, daemon=True)
    service_thread.start()
    print("\nStarted Service\n")
    sleep(1)
    print("Starting Consumer \n")
    # Run the consumer test
    run_consumer()
    print("\nStopping DNS\n")


    # Give everything time to complete
    sleep(1)

    print("Test completed.")
