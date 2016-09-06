import threading
import zipfile
import sys
import sqlite3
import os
from csv import DictReader
from urllib.request import urlretrieve
from concurrent.futures import ThreadPoolExecutor
from queue import Queue


q = Queue()  # Main task queue


def retrieve_item(url):
    """Download stats file, unzip and pass it to the csv parser"""
    filename, _headers = urlretrieve(url)
    files = unzip(filename)
    for datafile in files:
        parse_csv(datafile)


def parse_csv(filename):
    """Open csv stats file, and filter out needed data to the queue"""
    with open(filename) as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            if all(row['user:scalr-meta'].split(':')) and row['user:scalr-meta'].startswith('v1:'):
                q.put([row['user:scalr-meta'], row['Cost']])
    os.remove(filename)  # Clean-up


def run(urls):
    # Starting consumer thread
    thr = threading.Thread(target=main_loop)
    thr.start()

    # Starting pool of producer threads for each file
    with ThreadPoolExecutor(max_workers=4) as pool:
        pool.map(retrieve_item, urls)

    # Signaling main_loop that all producer threads finished
    q.put(None)


def main_loop():
    """Consumer thread loop"""
    rows = {}  # Store items here
    types = ('env', 'farm', 'farm_role', 'server')  # object_types
    while True:
        message = q.get()
        if message:
            ids, cost = message
            _, *payload = ids.split(':')
            keys = zip(types, payload)
            cost = float(cost)
            for key in keys:  # key = (object_type, object_id)
                if key in rows:
                    rows[key] += cost
                else:
                    rows[key] = cost
        # Writing data to db and exiting if there is None at task queue
        else:
            items = items_gen(rows)  # Create insert values generator from rows
            try:
                with sqlite3.connect('data.db') as conn:
                    conn.execute('create table costs (object_type text, object_id varchar(32), cost float)')
                    conn.executemany('insert into costs values (?, ?, ?)', items)
                conn.commit()
            except:
                print('oops, something gone wrong')
            break


def unzip(filename):
    with zipfile.ZipFile(filename) as zf:
        zf.extractall()
    os.remove(filename)  # Clean-up
    return zf.namelist()


def items_gen(dct):
    while dct:
        k, v = dct.popitem()
        yield k + (v,)


def main():
    urls = []

    if len(sys.argv) == 2:
        try:
            with open(sys.argv[1]) as links:
                for url in links:
                    urls.append(url.strip())
        except FileNotFoundError:
            print('file not found')
            exit(2)
    else:
        print('input filename must be given')
        exit(2)

    run(urls)


if __name__ == '__main__':
    main()
