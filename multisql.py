#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
"""
Execute SQL on multiple servers
"""
import os
import sys
import json
import argparse
import subprocess

import psycopg2
import psycopg2.extras

COLORS = {'red': 31, 'green': 32, 'yellow': 33, 'blue': 34}


class App(object):
    """
    Application class
    """

    def __init__(self):
        "Constructor"
        parser = argparse.ArgumentParser(description='Execute SQL statements on multiple servers.')
        parser.add_argument('--file', type=str, help='File with SQL statements', required=True)
        parser.add_argument('--group', type=str, help='Server group name', required=True)
        parser.add_argument('--use-psql', action='store_true', default=False)
        self.args = parser.parse_args()
        del parser
        self.sql = None
        self.config = None

    @staticmethod
    def with_color(color, string):
        "Prints message colorized"
        if sys.platform in ('win32', 'cygwin'):
            return string
        if isinstance(color, str):
            color = COLORS.get(color, 39)
        else:
            color = color
        return "\x1b[%dm%s\x1b[0m" % (color, string)

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
            print(App.with_color('red', 'Could not execute statement: %s' % str(err).strip()))
        except psycopg2.DataError as err:
            print(App.with_color('red', 'Data error: %s' % str(err).strip()))

        if cursor.rowcount <= 0:
            print(App.with_color('green', '<EMPTY SET>'))
        else:
            for row in cursor.fetchall():
                print(App.with_color('green', row))

        cursor.close()
        conn.close()

    def execute_psql(self, server):
        "Invokes psql and passes file with SQL code to it"
        command = ["psql", '%s' % server, '-f', '%s' % self.args.file]

        process = subprocess.Popen(command,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        out, _ = process.communicate()
        print(out.decode('utf-8'))

    @staticmethod
    def die(message=None):
        "Print message and exit with code 1"
        print(message)
        sys.exit(1)

    def run(self):
        "Program entry point"
        self.load_config()
        self.load_sql()
        if self.args.group not in self.config:
            self.die('Group %s was not found' % self.args.group)
        for server in self.config[self.args.group]:
            print('Executing on server "%s"...' % server)
            if self.args.use_psql is True:
                self.execute_psql(server)
            else:
                self.execute(server)


if __name__ == "__main__":
    APPLICATION = App()
    sys.exit(APPLICATION.run())
