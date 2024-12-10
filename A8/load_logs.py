from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel
from datetime import datetime
import gzip, os, sys, re

input_dir = sys.argv[1]
keyspace = sys.argv[2]
tablename = sys.argv[3]
# somehost = sys.argv[4]

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
datetime_format = '%d/%b/%Y:%H:%M:%S'
counter = 0
batch_size = 100

cluster = Cluster(['node1.local', 'node2.local'])
session = cluster.connect(keyspace)
insert_log = session.prepare("INSERT INTO nasalogs (host, bytes, datetime, path, id) VALUES (?, ?, ?, ?, uuid())")
batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

for f in os.listdir(input_dir):
    with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
        for line in logfile:
            match = line_re.search(line)
            if match:
                host = match.group(1)
                datetime_str = match.group(2)
                path = match.group(3)
                num_bytes = int(match.group(4))
                datetime = datetime.strptime(datetime_str, datetime_format)
                batch.add(insert_log, (host, num_bytes, datetime, path))
                counter += 1
                
            if counter >= batch_size:
                session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                counter = 0
                
if counter > 0:
    session.execute(batch)
