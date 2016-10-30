#!/usr/bin/env python3
#! -*- encoding: utf-8 -*-
"""
Execute SQL on multiple servers
"""
import os
import sys
import json

import psycopg2
import psycopg2.extras
import argparse


class App(object):
    """
    Application class
    """

    def __init__(self):
        "Constructor"
        parser = argparse.ArgumentParser(description='Execute SQL statements on multiple servers.')
        parser.add_argument('--file', type=str, help='File with SQL statements', required=True)
        parser.add_argument('--group', type=str, help='Server group name', required=True)
        self.args = parser.parse_args()
        del parser

    def load_config(self):
        "Load config from .multisqlrc"
        if not os.path.exists('.multisqlrc'):
            self.die('Configuration file .multisqlrc not found')

        with open('.multisqlrc') as config_file:
            self.config = json.load(config_file)

    def load_sql(self):
        "Load SQL from file"
        if not os.path.exists(self.args.file):
            self.die('Input file %s was not found' % self.args.file)
        with open(self.args.file) as sql_file:
            self.sql = sql_file.read()

    def execute(self, server):
        "Executes SQL from self.sql on server `server`"
        conn = psycopg2.connect(server)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(self.sql)
        except psycopg2.InternalError as err:
            print('Could not execute statement: %s' % str(err))
        for row in cursor.fetchall():
            print(row)

        cursor.close()
        conn.close()

    def die(self, message=None):
        "Print message and exit with code 1"
        print(message)
        sys.exit(1)

    def run(self, argv):
        "Program entry point"
        self.load_config()
        self.load_sql()
        if self.args.group not in self.config:
            self.die('Group %s was not found' % self.args.group)
        for server in self.config[self.args.group]:
            print('Executing on server "%s"...' % server)
            self.execute(server)


if __name__ == "__main__":
    app = App()
    sys.exit(app.run(sys.argv))
