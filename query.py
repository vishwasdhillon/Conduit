import json
import struct
from datetime import datetime
import time

FORMAT = '!IQ'
RECORD_SIZE = 12

Level_mapping = {
    1: "INFO",
    2: "WARNING", 
    3: "ERROR"
}

def convert_to_timestamp(Timestamp):
    format = "%Y-%m-%d %H:%M:%S"
    try:
        dt = datetime.strptime(Timestamp, format)
        return int(dt.timestamp())
    except ValueError:
        raise ValueError("Invalid timestamp format. Expected format: YYYY-MM-DD HH:MM:SS")
    
def processing_logs(positions):    
    logs = []
    with open("conduit_log.bin", "rb") as f:
        for pos in positions:
            f.seek(pos)
            record = f.read(165)
            level , Timestamp, Entity_Name , Message = struct.unpack('!BI32s128s', record)
            date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(Timestamp))
            logs.append((Level_mapping[level], date_time, Entity_Name.rstrip(b'\x00').decode('utf-8'), Message.rstrip(b'\x00').decode('utf-8')))
    printing_logs(logs)  # Moved outside the loop
def printing_logs(logs):
    for Level, Timestamp, Entity_Name, Message in logs:
        print(f" Level: {Level} , \n Timestamp: {Timestamp} , \n Entity Name: {Entity_Name} , \n Message: {Message} \n ------------------------------\n")

def query_by_range(start_time,end_time):
    start_ts = convert_to_timestamp(start_time)
    end_ts = convert_to_timestamp(end_time)
    offset = []
    print(f"Querying logs from {start_time} to {end_time}")
    with open("conduit.index",'rb') as index_bin:
        index_bin.seek(0 , 2)
        total_records = index_bin.tell() // RECORD_SIZE
        low = 0
        high = total_records - 1
        start_index = 0  # Initialize to 0 in case no suitable index is found
        while low <= high:
            mid = (low + high) // 2
            index_bin.seek(mid * RECORD_SIZE)
            timestamp , position = struct.unpack('!IQ', index_bin.read(RECORD_SIZE))
            if timestamp >= start_ts:
                high = mid - 1
                start_index = mid
            else:
                low = mid + 1
        index_bin.seek(start_index * RECORD_SIZE)  # Fixed: multiply by RECORD_SIZE
        while True:
            data = index_bin.read(RECORD_SIZE)
            if not data:
                break
            ts , pos = struct.unpack(FORMAT , data)
            if ts > end_ts:
                break
            offset.append(pos)
    return processing_logs(offset)
            



    
def query_by_timestamp(Timestamp):
    Time = convert_to_timestamp(Timestamp)
    RECORD_SIZE = 12
    with open("conduit.index",'rb') as index_bin:
        index_bin.seek(0 , 2)
        total_records = index_bin.tell() // RECORD_SIZE
        low = 0
        high = total_records - 1
        while low <= high:
            mid = (low + high) // 2
            index_bin.seek(mid * RECORD_SIZE)
            timestamp , position = struct.unpack(FORMAT, index_bin.read(RECORD_SIZE))
            if timestamp == Time:
                return processing_logs([position])
            elif timestamp < Time:
                low = mid + 1
            else:
                high = mid - 1
    return None


def query_by_level(Level):
    with open("level_index",'r') as level_file:
        level_data = json.load(level_file)
    positions = level_data.get(Level.upper(),[])
    return processing_logs(positions)

def query_by_entity_name(Entity_Name):
    with open("conduit_log.bin", "rb") as f:
        logs = []
        while True:
            record = f.read(165)
            if len(record) < 165:
                break
            level , Timestamp, Entity , Message = struct.unpack('!BI32s128s', record)
            if Entity.rstrip(b'\x00').decode('utf-8') == Entity_Name:
                date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(Timestamp))
                logs.append((Level_mapping[level], date_time, Entity.rstrip(b'\x00').decode('utf-8'), Message.rstrip(b'\x00').decode('utf-8')))
        printing_logs(logs)

def search(level = None , Timestamp = None , Entity_Name = None , Range = None):
    if Timestamp:
        query_by_timestamp(Timestamp)
    
    if level:
        query_by_level(level)

    if Entity_Name:
        query_by_entity_name(Entity_Name)

    if Range:
        query_by_range(Range[0],Range[1])

search(Range = ["2026-03-30 3:40:00", "2026-03-30 4:40:00"])