'''
Created on Jun 26, 2014

@author: hustnn
'''
import multiprocessing
from multiprocessing.managers import SyncManager
import Queue
import threading

import time
import sys
import math

IP = "127.0.0.1"
PORTNUM = 58000
AUTHKEY = "hustnn"
CLIENTNUM = 2
FILEPORTNUM = 59000
NODEINFOS = {"client0": ["10.1.255.254", FILEPORTNUM],
             "client1": ["10.1.255.253", FILEPORTNUM],
             "client2": ["10.1.255.252", FILEPORTNUM],
             "client3": ["10.1.255.251", FILEPORTNUM],
             "client4": ["10.1.255.250", FILEPORTNUM],
             "client5": ["10.1.255.249", FILEPORTNUM],
             "client6": ["10.1.255.248", FILEPORTNUM],
             "client7": ["10.1.255.247", FILEPORTNUM],
             "client8": ["10.1.255.246", FILEPORTNUM]}

job_q = Queue.Queue()
result_q = Queue.Queue()
connected_client_q = Queue.Queue()
all_clients_connected_e = threading.Event()

    
class JobQueueManager(SyncManager):
    pass


def get_job_q():
    return job_q


def get_result_q():
    return result_q


def get_connected_clients():
    return connected_client_q


def get_conn_event():
    return all_clients_connected_e


JobQueueManager.register("get_job_q", get_job_q)
JobQueueManager.register("get_result_q", get_result_q)
JobQueueManager.register("get_clients_q", get_connected_clients)
JobQueueManager.register("get_conn_e", get_conn_event)


class ServerQueueManager(SyncManager):
        pass


ServerQueueManager.register("get_job_q")
ServerQueueManager.register("get_result_q")
ServerQueueManager.register("get_clients_q")
ServerQueueManager.register("get_conn_e")


def worker_func(job_q, result_q, clients_q, conn_e):
    name = multiprocessing.current_process().name
    clients_q.put(name)
    
    print("wait for others to connect")
    conn_e.wait()
    print("hi, I am %s" % name)
    
    while True:
        try:
            job = job_q.get_nowait()
            outdict = {n: math.sqrt(n) for n in job}
            result_q.put(outdict)
        except Queue.Empty:
            return
        
        
def mp_worker(shared_job_q, shared_result_q, shared_clients_q, shared_conn_e, client_name, nprocs):
    procs = []
    for i in range(nprocs):
        p = multiprocessing.Process(name = client_name,
                                    target = worker_func,
                                    args = (shared_job_q, shared_result_q, shared_clients_q, shared_conn_e))
        procs.append(p)
        p.start()
        
    for p in procs:
        p.join()

        
def make_server_manager(port, authkey):
    manager = JobQueueManager(address=(IP, port), authkey=authkey)
    manager.start()
    print("Server started at port %s" % port)
    return manager


def make_nums(n):
    nums = [999999999999]
    for i in range(n):
        nums.append(nums[-1] + 2)
    return nums

def run_server():
    manager = make_server_manager(PORTNUM, AUTHKEY)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    shared_client_q = manager.get_clients_q()
    shared_conn_event = manager.get_conn_e()
    
    N = 999
    nums = make_nums(N)
    
    chunksize = 43
    print(len(nums))
    for i in range(0, len(nums), chunksize):
        #print("putting chunk %s:%s in job Q" % (i, i + chunksize))
        shared_job_q.put(nums[i: i + chunksize])
        
    #wait for all clients connect
    while(shared_client_q.qsize() < CLIENTNUM):
        time.sleep(1)
        print("waiting for all clients connect...")
        
    print("all clients connected.")
    shared_conn_event.set()
        
    num_results = 0
    result_dict = {}
    while num_results < N:
        #print(shared_result_q.empty())
        #shared_result_q will block if it is empty
        out_dict = shared_result_q.get()
        #print("finish " + str(len(out_dict)) + " numbers, finished " + str(num_results))
        result_dict.update(out_dict)
        #print("current finished: %s, length of out_dict: %s" % (num_results, len(out_dict)))
        num_results += len(out_dict)
    
    print("____ DONE ____")
    time.sleep(2)
    manager.shutdown()
    

def make_client_manager(ip, port, authkey):
    manager = ServerQueueManager(address = (ip, port), authkey = authkey)
    manager.connect()
    print("client connected to %s:%s" % (ip, port))
    
    return manager


def run_client(client_name):
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    clients_q = manager.get_clients_q()
    conn_q = manager.get_conn_e()
    mp_worker(job_q, result_q, clients_q, conn_q, client_name, 1)
    
    
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "client":
        run_client(sys.argv[2])
    else:
        run_server()

    