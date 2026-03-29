import asyncio
import logging
import json
import socket
import struct

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler("conduit_log.txt")
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

log_queue = None

def write_process(level,Timestamp,Entity_Name,Message):
    level_mapping = {
        "INFO": 1,
        "WARNING": 2,
        "ERROR": 3
    }
    Entity_Name = Entity_Name.encode('utf-8')[:32].ljust(32, b'\00')
    Message = Message.encode('utf-8')[:128].ljust(128, b'\00')
    record = struct.pack('!BI32s128s' , level_mapping[level], Timestamp, Entity_Name, Message)
    with open("conduit_log.bin", "ab") as bin_file:
        position = bin_file.tell()
        bin_file.write(record)
    with open("conduit.index",'ab') as index_file:
        binary_index = struct.pack('!IQ', Timestamp , position)
        index_file.write(binary_index)
    with open("level_index",'r') as level_file:
        level_data = json.load(level_file)
    level_data[level].append(position)
    with open("level_index",'w') as level_file:
        json.dump(level_data, level_file)

async def processor():
    try:
        with open("level_index",'r') as level_file:
            pass
    except FileNotFoundError:
        with open("level_index",'w') as level_file:
            json.dump({"INFO": [], "WARNING": [], "ERROR": []}, level_file)
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