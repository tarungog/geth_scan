from multiprocessing import Process
from multiprocessing.queues import SimpleQueue as Queue
import sys
import time
import logging

import requests


def _query(ip):
    url = 'http://{}:8545'.format(ip)
    logging.debug("Querying URL: %s", url)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    payload = '{"jsonrpc":"2.0","method":"eth_hashrate","params":[],"id":71}'
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=1.0)
        logging.debug(resp.status_code)
        if resp.status_code == 200:
            logging.critical("Server response VALID: %s", ip)
            return True
        logging.info("Server response invalid: %s", ip)
        return False
    except requests.ConnectionError as conn:
        logging.error("%s", str(conn))
        return False


def query(read, write):
    while True:
        if read.empty():
            logging.info("Query Thread sleeping: empty queue")
            time.sleep(0.5)
        else:
            ip = read.get()
            if ip == "End of File":
                logging.warning("Query Thread exiting")
                return
            if _query(ip):
                write.put((ip, True))
            else:
                write.put((ip, False))


def write_file(q):
    invalid = []
    while True:
        if q.empty():
            logging.warning("Write Thread sleeping")
            time.sleep(0.5)
        else:
            query, valid = q.get()
            logging.debug(
                "Write Thread received query: {}, {}".format(query, valid))
            if query == "End of File":
                logging.warning("Write Thread exiting")
                with open('invalid_response.out', 'a') as fp:
                    logging.info("Writing invalid server response IPs")
                    fp.writelines(invalid)
                    invalid = []
                return
            if valid:
                with open('valid_response.out', 'a') as fp:
                    fp.write(query + '\n')
            else:
                invalid.append(query + '\n')
                if len(invalid) >= 2000:
                    with open('invalid_response.out', 'a') as fp:
                        logging.info("Writing invalid server response IPs")
                        fp.writelines(invalid)
                        invalid = []


def main():
    FORMAT = '%(levelname)s	%(asctime)-15s %(threadName)s %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    with open(sys.argv[1]) as fp:
        procs = []
        read_queue = Queue()
        write_queue = Queue()
        for i in xrange(0, 10):
            procs.append(Process(target=query, args=(read_queue, write_queue)))
        write_proc = Process(target=write_file, args=(write_queue,))
        map(lambda proc: proc.start(), procs)
        write_proc.start()
        try:
            for x in fp:
                read_queue.put(x.rstrip('\n'))
        except Exception as e:
            print e
        finally:
            for i in xrange(0, 10):
                read_queue.put("End of File")
            map(lambda proc: proc.join(), procs)
            write_queue.put(("End of File", False))
            write_proc.join()


if __name__ == '__main__':
    main()
