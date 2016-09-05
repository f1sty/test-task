import threading
import zipfile
import sys
import urllib.request
import sqlite3
import os
from csv import DictReader
from concurrent.futures import ThreadPoolExecutor
from queue import Queue


q = Queue()  # main task queue


def retrieve_item(url):
    """Downloads stats file, unzip and pass it to the """
    filename, _headers = urllib.request.urlretrieve(url)
    files = unzip(filename)
    for datafile in files:
        parse_csv(datafile)


def parse_csv(filename):
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
            try:
                _, env, farm, farm_role, server = ids.split(':')
                keys = zip(types, (env, farm, farm_role, server))
                cost = float(cost)
            except:
                # Ignoring malformed rows
                continue
            for key in keys:
                if key in rows:
                    rows[key] += cost
                else:
                    rows[key] = cost
        # Writing data to db and exiting if there is None at task queue
        else:
            items = []
            # Converting from dict to list of tuples to use executemany
            for k, v in rows.items():
                items.append(k + (v,))
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
