import http.server

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

class DB_MySQL:
    def __init__(self, host, user, password, db):
        import mysql.connector

        self.conn = mysql.connector.connect(host=host, user=user, password=password, database=db)

    def init_dataset(self, dataset, type):
        c = self.conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS `%s` (timestamp TIMESTAMP, value %s)' % (dataset, type))

    def insert_into_dataset(self, dataset, value):
        c = self.conn.cursor()
        c.execute('INSERT INTO `' + dataset + '` VALUES (CURRENT_TIMESTAMP(), %s)', (value,))
        self.conn.commit()

    def get_dataset(self, dataset, limit):
        c = self.conn.cursor(dictionary=True)
        c.execute('SELECT * FROM `' + dataset + '` LIMIT %s', (limit,))
        return c.fetchall()

class MyHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
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
        value = body.decode()
        self.server.db.insert_into_dataset(path, value)

        self.send_response(200)
        self.end_headers()

def run(db, server_class=http.server.HTTPServer, handler_class=MyHTTPRequestHandler):
    server_address = ('', 8000)
    with server_class(server_address, handler_class) as httpd:
        httpd.db = db
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        httpd.server_close()
