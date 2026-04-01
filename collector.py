import asyncio
import logging
import json
import socket
import struct
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler("conduit_log.txt")
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
log_queue = None
Megabytes = 100 * 1024 * 1024

file_id = 0
file_id_path = "file_id"
if os.path.exists(file_id_path):
    with open(file_id_path, 'r') as id_file:
        file_id = int(id_file.read().strip() or 0)
else:
    with open(file_id_path, 'w') as id_file:
        id_file.write('0')

for dirc in ["Logs" , "Index"]:
        if not os.path.exists(dirc):
            os.makedirs(dirc)
def rotate_logs(current_file):
    global file_id
    new_name = f"Logs/archive_{file_id}.bin"
    os.rename(current_file, new_name)
    file_id += 1
    with open(file_id_path, 'w') as id_file:
        id_file.write(str(file_id))


def write_process(level,Timestamp,Entity_Name,Message):
    level_mapping = {
        "INFO": 1,
        "WARNING": 2,
        "ERROR": 3
    }
    Entity_Name = Entity_Name.encode('utf-8')[:32].ljust(32, b'\00')
    Message = Message.encode('utf-8')[:128].ljust(128, b'\00')
    record = struct.pack('!BI32s128s' , level_mapping[level], Timestamp, Entity_Name, Message)
    try:
        if os.path.exists("conduit_log.bin"):
            if os.path.getsize("conduit_log.bin") > Megabytes :
                rotate_logs("conduit_log.bin")
    except FileNotFoundError:
        pass
    with open("conduit_log.bin", "ab") as bin_file:
        position = bin_file.tell()
        bin_file.write(record)
    with open("conduit.index",'ab') as index_file:
        binary_index = struct.pack('!IIQ', Timestamp , file_id , position)
        index_file.write(binary_index)
    with open(f"Index/{level}.idx" , 'ab') as index_file:
        ID_data = struct.pack('>IQ', int(file_id), position)
        index_file.write(ID_data)

async def processor():
    while True:
        level , Timestamp , Entity_name , message = await log_queue.get()
        getattr(logger, level.lower())(message)
        write_process(level,Timestamp,Entity_name,message)
        log_queue.task_done()
        
async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    logger.info(f"Connection established with {addr}")
    logger.info("Waiting for connection...")
    header = await reader.readexactly(4)
    message_length = struct.unpack('!I', header)[0]
    json_chunk = await reader.readexactly(message_length)

    log_dict = json.loads(json_chunk.decode('utf-8'))
    await log_queue.put((log_dict['Level'], log_dict['Time'], log_dict['Entity_Name'], log_dict['Message']))
    writer.write(f"Message received successfully.".encode('utf-8'))
    await writer.drain()
    writer.close()
    await writer.wait_closed()

async def main():
    global log_queue
    log_queue = asyncio.Queue(maxsize = 1000) 
    asyncio.create_task(processor())
    addr = socket.gethostbyname(socket.gethostname())
    server = await asyncio.start_server(handle_client, addr, 6000, reuse_port=True)
    logger.info("Conduit collector running...")
    async with server:
        await server.serve_forever()



try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Conduit stopped.")