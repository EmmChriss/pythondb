import time
import os
import random
from projekt_szerver import *

server = Server(os.getcwd())
server.drop_database('allatok')
server.create_database('allatok')
server.use_database('allatok')
server.create_table('hazi')
server.create_column('hazi', 'id', 'int', 'primary-key')
server.create_column('hazi', 'faj', 'string', 'none')
server.create_column('hazi', 'age', 'int', 'index')
server.create_table('vad')
server.create_column('vad', 'id', 'int', 'primary-key')
server.create_column('vad', 'mutat', 'int', 'foreign-key=hazi.id')

# benchmark inserting rows

# last_time = time.time_ns()
# insert_count = 0
# time_count = 0
# last_id = 0
# while True:
#     last_id += 1
#     faj = 'a'
#     age = random.randrange(50)
#     server.insert('hazi', [str(last_id), faj, str(age)])
#     insert_count += 1

#     time_count += 1
#     if time_count == 100:
#         time_count = 0
#         new_time = time.time_ns()
#         if new_time - last_time > 1000000000:
#             elapsed_sec = (new_time - last_time) / 1000000000
#             print(insert_count / elapsed_sec)
#             insert_count = 0
#             last_time = new_time


for i in range(80000):
    faj = random.choice(['kuty', 'macs', 'fecs', 'kecs', 'szöcs', 'orángután'])
    age = random.randrange(50)
    server.insert('hazi', [str(i), faj, str(age)])
    if i % 1000 == 0:
        print(i)

for i in range(20000):
    mutat = random.randrange(20000)
    server.insert('vad', [str(i), str(mutat)])
    if i % 1000 == 0:
        print(i)

