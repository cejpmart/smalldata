#!/usr/bin/env python3

import http.server

import credentials
from db_mysql import DB_MySQL

TYPE_REAL = 'REAL'
TYPE_TEXT = 'TEXT'

class DB:
    def __init__(self, path):
        import sqlite3

        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row

    def init_dataset(self, dataset, type):
        c = self.conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS `%s` (timestamp INTEGER, value %s)' % (dataset, type))

    def insert_into_dataset(self, dataset, value):
        c = self.conn.cursor()
        c.execute('INSERT INTO `%s` VALUES (CURRENT_TIMESTAMP, ?)' % dataset, (value,))
        self.conn.commit()

    def get_dataset(self, dataset, limit):
        c = self.conn.cursor()
        c.execute('SELECT * FROM `%s` LIMIT ?' % (dataset), (limit,))
        return c.fetchall()

class MyHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    def __init__(self, request, client_address, server):
        http.server.BaseHTTPRequestHandler.__init__(self, request, client_address, server)
        self.timeout = 10

    def do_GET(self):
        path = self.path[1:]
        dataset = self.server.db.get_dataset(path, 100)

        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()

        if len(dataset) > 0:
            heading = ','.join(dataset[0].keys()) + '\n'
            self.wfile.write(heading.encode())

            for row in dataset:
                heading = ','.join([str(col) for col in row.values()]) + '\n'
                self.wfile.write(heading.encode())

    def do_POST(self):
        path = self.path[1:]
        body = self.rfile.read(int(self.headers.get('Content-Length')))

        if path == 'BULK':
            # 1/Status,2018-03-23 02:13:30,OK
            # 1/BatteryVoltage,2018-03-23 02:13:30,3.101 \n
            # 1/Temp,2018-03-23 02:13:30,21.20 \n
            # (time is always UTC)

            print('BULK DATA', len(body), body)
            lines = body.decode().split('\n')

            for line in lines:
                line = line.strip()

                if line == '' or line[0] == '#':
                    continue

                [path, timestamp, value] = line.split(',')
                self.server.db.insert_into_dataset(path, value)
        elif path == 'ECHO':
            print('ECHO DATA', len(body), body)
        else:
            value = body.decode()
            self.server.db.insert_into_dataset(path, value)

        self.send_response(200)
        self.end_headers()

def run(db, server_class=http.server.HTTPServer, handler_class=MyHTTPRequestHandler):
    server_address = ('', 8000)
    httpd = server_class(server_address, handler_class)
    httpd.db = db
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()

db = DB_MySQL(credentials.HOST, credentials.USER, credentials.PASSWORD, credentials.DB)
db.init_dataset('1/BatteryVoltage', TYPE_REAL)
db.init_dataset('1/Humidity', TYPE_REAL)
db.init_dataset('1/Temp', TYPE_REAL)
db.init_dataset('1/TempInternal', TYPE_REAL)
db.init_dataset('1/Pressure', TYPE_REAL)

db.init_dataset('1/Status', TYPE_TEXT)
run(db)
