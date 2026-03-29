import threading
import subprocess

def run_sender():
    subprocess.run(['python3', 'sender.py'])

threads = []
for i in range(1000):
    t = threading.Thread(target = run_sender)
    threads.append(t)

for t in threads:
    t.start()

for t in threads:
    t.join()