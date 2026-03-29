# Conduit

A TCP log collection pipeline with binary storage and indexed querying.

Built to understand how logging infrastructure actually works under the hood —
not just calling logging.info() but building the thing that receives, stores,
and retrieves logs at scale.

---

## What it does

Receives log streams from thousands of simultaneous senders over TCP,
processes them asynchronously, stores them in a binary file format,
and retrieves them via indexed queries without scanning the full dataset.

---

## Architecture

**Collector (collector.py)**

Asyncio-based TCP server. Handles concurrent connections without threads —
each connection is a coroutine, the event loop switches between them during
I/O waits. A bounded queue (1000 items) sits between the network receiver
and the disk writer. When the disk falls behind during a burst, the receiver
pauses automatically instead of exhausting RAM.

Uses length-prefixed TCP framing — sender transmits a 4-byte header
containing message length, then the payload. Receiver reads exactly that
many bytes. Prevents partial read bugs caused by TCP fragmentation.

**Storage (two layers)**

Main binary file (conduit_log.bin) — each log stored as a fixed-width
165-byte struct:
- 1 byte: severity level
- 4 bytes: unix timestamp  
- 32 bytes: entity name (null-padded)
- 128 bytes: message (null-padded)

Fixed width means any log's position is predictable without scanning.

Chronological index (conduit.index) — 12-byte binary records mapping
timestamp to byte offset in the main file. Written in arrival order,
enabling binary search on disk.

Level index (level_index) — JSON mapping each severity level to a list
of byte offsets. Enables O(1) retrieval by level without touching the
main file.

**Query engine (query.py)**

- By level: reads level_index, seeks directly to each offset. O(1) lookup.
- By timestamp: binary search on conduit.index using f.seek(). O(log N).
- By range: binary search to find start position, linear read forward
  until end timestamp. Minimal disk I/O.
- By entity: full sequential scan. No entity index currently.

**Simulator (simulate.py + sender.py)**

1000 threads each launching a sender subprocess simultaneously.
Faker generates realistic names and messages. Used to stress-test
the collector's concurrency and verify no logs are dropped under load.

---

## How to run

Start the collector:
```
python collector.py
```

In a separate terminal, generate traffic:
```
python simulate.py
```

Query the results:
```python
search(level="ERROR")
search(Range=["2026-03-29 18:10:12", "2026-03-29 18:20:20"])
search(Entity_Name="John Smith")
```

---

## What I learned building this

TCP doesn't guarantee message boundaries. readexactly() with a length
header is the correct solution — recv() alone causes silent data corruption
under load.

Fixed-width binary records make seek()-based access possible. This is
the same principle underlying real database storage engines.

asyncio handles I/O-bound concurrency cleanly in one thread. The event
loop only stalls if you put CPU-heavy work in a coroutine without awaiting.
Threads are for CPU-bound work. The simulator uses threads because
subprocess.run() is blocking — correct tool for that job.

Backpressure is not optional at scale. Without the bounded queue,
a 10,000 sender burst would exhaust memory before a single log hit disk.

---

## What's missing (known limitations)

- No entity index — entity queries scan the full binary file
- No log rotation — single binary file grows indefinitely  
- No authentication on the TCP endpoint
- write_process is synchronous — under extreme load this could
  block the processor coroutine noticeably
- query_by_timestamp returns only the first match — multiple logs 
  with identical timestamps (common under high load) are not all returned