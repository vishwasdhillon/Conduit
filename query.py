import struct
from datetime import datetime
import time
import os

FORMAT = '!IIQ'
RECORD_SIZE = 16

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
    
def processing_logs(data):
    logs = []
    for file_id , pos in data:
        with open("file_id" , 'r') as f_id:
            check = f_id.read().strip()
            print(f"{file_id} : {check}")
            if file_id == int(check):
                print("arrives")
                file_name = "conduit_log.bin"
            else:
                file_name = f"Logs/archive_{file_id}.bin"
               
            with open(f"{file_name}", "rb") as f:
                f.seek(pos)
                record = f.read(165)
                level , Timestamp, Entity_Name , Message = struct.unpack('!BI32s128s', record)
                date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(Timestamp))
                logs.append((Level_mapping[level], date_time, Entity_Name.rstrip(b'\x00').decode('utf-8'), Message.rstrip(b'\x00').decode('utf-8')))
    printing_logs(logs)
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
            timestamp , file_id , position = struct.unpack('!IIQ', index_bin.read(RECORD_SIZE))
            if timestamp >= start_ts:
                high = mid - 1
                start_index = mid
            else:
                low = mid + 1
        index_bin.seek(start_index * RECORD_SIZE)
        while True:
            data = index_bin.read(RECORD_SIZE)
            if not data:
                break
            ts , f_id , pos = struct.unpack(FORMAT , data)
            if ts > end_ts:
                break
            offset.append((f_id , pos))
    return processing_logs(offset)
            



    
def query_by_timestamp(Timestamp):
    Time = convert_to_timestamp(Timestamp)
    with open("conduit.index",'rb') as index_bin:
        index_bin.seek(0 , 2)
        total_records = index_bin.tell() // RECORD_SIZE
        low = 0
        high = total_records - 1
        data = []
        while low <= high:
            mid = (low + high) // 2
            index_bin.seek(mid * RECORD_SIZE)
            timestamp , file_id , position = struct.unpack(FORMAT, index_bin.read(RECORD_SIZE))
            if timestamp == Time:
                print("arrives")
                result = mid
                high = mid - 1
            elif timestamp < Time:
                low = mid + 1
            else:
                high = mid - 1
        try:
            index_bin.seek(result * RECORD_SIZE)
            while True:
                b_data = index_bin.read(RECORD_SIZE)
                if len(b_data) < RECORD_SIZE:
                    break
                timestamp , f_id , pos = struct.unpack(FORMAT , b_data)
                if timestamp == Time:
                    data.append((f_id , pos))
                else:
                    break
            return processing_logs(data)
        except UnboundLocalError:
               print("No record found")

def query_by_level(Level):
    data = []
    with open(f"Index/{Level}.idx" , 'rb') as level_file:
        level_file.seek( 0 , 2)
        total_record =  level_file.tell() // 12
        level_file.seek(0)
        for i in range(total_record):
            b_data = level_file.read(12)
            data.append(struct.unpack('>IQ' , b_data))
        processing_logs(data)
    
def query_by_entity_name(Entity_Name):
    all_files = [f"Logs/{f}" for f in os.listdir('Logs')]
    all_files.append("conduit_log.bin")
    for curr_file in all_files:
        with open(curr_file , "rb") as f:
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

