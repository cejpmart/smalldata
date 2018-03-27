#!/usr/bin/env python3

import mysql.connector

class DB_MySQL:
    def __init__(self, host, user, password, db):
        self.conn = mysql.connector.connect(host=host, user=user, password=password, database=db)

    def cursor(self, *args, **kwargs):
        try:
            return self.conn.cursor(*args, **kwargs)
        except mysql.connector.errors.OperationalError:
            self.conn.reconnect()
            return self.conn.cursor(*args, **kwargs)

    def init_dataset(self, dataset, type):
        c = self.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS `%s` (timestamp TIMESTAMP, value %s)' % (dataset, type))

    def insert_into_dataset(self, dataset, value):
        c = self.cursor()
        c.execute('INSERT INTO `' + dataset + '` VALUES (CURRENT_TIMESTAMP(), %s)', (value,))
        self.conn.commit()

    def get_dataset(self, dataset, limit=None):
        c = self.cursor(dictionary=True)
        if limit is None:
            c.execute('SELECT * FROM `' + dataset + '`')
        else:
            c.execute('SELECT * FROM `' + dataset + '` LIMIT %s', (limit,))
        return c.fetchall()
