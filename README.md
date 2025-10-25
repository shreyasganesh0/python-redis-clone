Python Redis Clone (PYDIS)

[![progress-banner](https://backend.codecrafters.io/progress/redis/f0493a36-5fcd-4a1f-ad72-44c9d538c026)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a high-performance, asynchronous clone of Redis built from scratch in Python using `asyncio`. It's not just a simple key-value store; it implements core Redis features including the RESP protocol, master-replica replication, and RDB file parsing for persistence.

## Why This Project?

I built this project to get a deep, hands-on understanding of how a modern distributed in-memory database works. My goal was to move beyond theory and write the code for:
* **Asynchronous Networking:** How does a server handle thousands of concurrent clients without threads?
* **Replication Protocols:** How does a master node propagate writes to replicas?
* **Data Persistence:** How is an in-memory database saved to and restored from disk?
* **Custom Protocols:** How do you parse a byte-level protocol like RESP?

This project was my "one step closer" to becoming a great distributed systems engineer.

## Features Implemented

* **Core Commands:** `PING`, `ECHO`, `GET`, `SET`
* **Key Expiry:** Full support for `SET ... PX <milliseconds>`
* **Configuration:** `CONFIG GET` for server parameters.
* **Async TCP Server:** Built on `asyncio` to handle many concurrent clients on a single thread.
* **RESPv2 Parser:** A custom parser for the Redis Serialization Protocol.
* **RDB Persistence:**
    * Parses the `dump.rdb` file on startup.
    * Decodes opcodes, length encodings, and expiry timestamps.
    * Loads the on-disk data into the in-memory `kvstore`.
* **Single-Leader Replication:**
    * **Handshake:** Full replica handshake (`PING`, `REPLCONF listening-port`, `REPLCONF capa`, `PSYNC`).
    * **Full Resync:** Master sends its full RDB file to a new replica.
    * **Write Propagation:** Master forwards all write commands (`SET`, etc.) to its connected replicas.
    * **Info:** `INFO replication` command reports `role` (master/slave), `master_replid`, and `master_repl_offset`.

## Technical Deep Dive

### 1. Asynchronous Server Core

The server runs on a single-threaded `asyncio` event loop. The `client_req_resp` coroutine is the heart of the server, managing the entire lifecycle of a client connection. It uses `asyncio.wait_for` for timeouts and `await reader.read()` for non-blocking I/O.

```python
# From app/main.py
async def client_req_resp(self, reader, writer) -> None:
    while True:
        print("here in client", self.port)
        
        try:
            # Non-blocking read with a 30s timeout
            data = await asyncio.wait_for(reader.read(1000), timeout=30)
            if not data:
                break
            
            # ... (RESP parsing logic) ...

            # Dynamically call the correct command (GET, SET, etc.)
            command_method = getattr(CommandExecutor, command)
            resp = command_method(self, bulk_string_data)
            
            writer.write(resp.encode())

            # If the command was a write, propagate it to all replicas
            if command in self.propogate_to_replica:
                print("Propogating to replicas", resp)
                for i in self.replicas_list:
                    temp_writer = self.replica_connection_obj_pool[i]
                    temp_writer.write(data) # Forward the raw command

        except asyncio.TimeoutError:
            print("Client request timeout.")
            break
        # ... (other error handling) ...
    
    writer.close()
    await writer.wait_closed()
```
2. Replication Handshake
When the server starts as a replica, it initiates a complex handshake with the master. This logic, in replica_handshake, proves a deep understanding of distributed protocols.

```Python
# From app/main.py
async def replica_handshake(self):
    try:
        master_host, master_port = self.replicaof.split(" ")                  
        reader, writer = await asyncio.open_connection(master_host,master_port)
        
        # 1. PING
        writer.write(b"*1\r\n$4\r\nPING\r\n")
        response = await reader.read(100) # +PONG

        # 2. REPLCONF listening-port
        replconf1 = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(self.port))}\r\n{self.port}\r\n"
        writer.write(replconf1.encode()) 
        response = await reader.read(100) # +OK

        # 3. REPLCONF capa
        replconf2 = f"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        writer.write(replconf2.encode()) 
        response = await reader.read(100) # +OK

        # 4. PSYNC
        psync = f"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        writer.write(psync.encode()) 
        response = await reader.read(56) # +FULLRESYNC ...
        
        return reader, writer
        
    except Exception as e:
        print(f"Error during handshake: {e}")
```
## How to Run
1. Clone the repository:
```Bash
git clone [https://github.com/shreyasganesh0/python-redis-clone.git](https://github.com/shreyasganesh0/python-redis-clone.git)
cd python-redis-clone
```

2. Run as a master server:
```Bash
python3 -m app.main --port 6379
```
3. Run as a replica of the master:
```
```Bash
python3 -m app.main --port 6380 --replicaof localhost 6379
```

4. Connect with redis-cli:
```Bash
redis-cli -p 6379

127.0.0.1:6379> SET foo bar
+OK
127.0.0.1:6379> GET foo
$3
bar
127.0.0.1:6379> INFO replication
$82
role:master
master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a9900eb4
master_repl_offset:0
```

## Design and Learning Journal
I maintained a live document of my learnings, design decisions, and bugs I encountered while building this project. You can read it here: "Implementation Detailed Doc"
