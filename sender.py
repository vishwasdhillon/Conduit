import socket
import json
import random
import time
import struct
from faker import Faker
fake = Faker()
addr = socket.gethostbyname(socket.gethostname()) #Replace with actual IP address when not running as localhost.
port = 6000 #Receiver's port number
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((addr, port)) #connect to receiver's port
name , message = (fake.name(),fake.sentence())
log = {
    "Entity_Name": name ,
    "Message": message ,
    "Level": random.choice(["INFO", "WARNING", "ERROR"]) ,
    "Time": int(time.time())
}
json_data = json.dumps(log).encode('utf-8')
header = struct.pack('!I', len(json_data))
client.send(header + json_data)
print("Message sent to Receiver.")
client.shutdown(socket.SHUT_WR)
chunks = []
while True:
    msg = client.recv(1024)
    if not msg:
        break
    chunks.append(msg)
response = b''.join(chunks).decode('utf-8')
print(f"Response from Receiver: {response}")
client.close()
