# Volatility
# Copyright (C) 2008-2015 Volatility Foundation
#
# This file is part of Volatility.
#
# Volatility is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Volatility is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Volatility.  If not, see <http://www.gnu.org/licenses/>.
#

from volatility.renderers.basic import Renderer, Bytes
from volatility import debug
from configparser import ConfigParser
import psycopg2

class PostgresRenderer(Renderer):

    def __init__(self, plugin_name, config):
        self._plugin_name = plugin_name
        self._config = config
        self._db = None
        self._accumulator = [0,[]]

    def config(self,filename='volatility/renderers/database.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)
     
        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
     
        return db

    column_types = [(str, "TEXT"),
                    (int, "TEXT"),
                    (float, "TEXT"),
                    (Bytes, "BLOB")]


    def _column_type(self, col_type):
        for (t, v) in self.column_types:
            if issubclass(col_type, t):
                return v
        return "TEXT"

    def _sanitize_name(self, name):
        return name

    def render(self, outfd, grid):
         
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
             # read connection parameters
             params = self.config()
             
             # connect to the PostgreSQL server
             print('Connecting to the PostgreSQL database...')
             conn = psycopg2.connect(**params)
 
             # create a cursor
             self._db = conn.cursor()
        
             # execute a statement
             print 'PostgreSQL database version: '
             self._db.execute('SELECT version()')
 
             # display the PostgreSQL database server version
             db_version = self._db.fetchone()
             print db_version

             create = "CREATE TABLE IF NOT EXISTS " + self._plugin_name + "( id INTEGER, " + \
                 ", ".join(['"' + self._sanitize_name(i.name) + '" ' + self._column_type(i.type) for i in grid.columns]) + ")"

             self._db.execute(""+create+"")
             #self._db.close()
             conn.commit()
             
             def _add_multiple_row(node, accumulator):
                 accumulator[0] = accumulator[0] + 1 #id
                 accumulator[1].append([accumulator[0]] + [str(v) for v in node.values])
                 if len(accumulator[1]) > 20000:
                     insert = "INSERT INTO " + self._plugin_name + " (id, " + \
                         ", ".join(['"' + self._sanitize_name(i.name) + '"' for i in grid.columns]) + ") " + \
                         " VALUES (%s, " + ", ".join(["%s"] * len(node.values)) + ")"
                     self._db.executemany(insert, accumulator[1])
                     accumulator = [accumulator[0], []]
                     conngir.commit()
                 self._accumulator = accumulator
                 return accumulator
             
             grid.populate(_add_multiple_row, self._accumulator)
             print self._accumulator

             #Insert last nodes
             if len(self._accumulator[1]) > 0:
                 """INSERT INTO PSList (id, ....,) VALUES (..,..,..) """
                 insert = "INSERT INTO " + self._plugin_name + " (id, " + \
                          ", ".join(['"' + self._sanitize_name(i.name) + '"' for i in grid.columns]) + ") " + \
                          " VALUES (%s, " + ", ".join(["%s"] * (len(self._accumulator[1][0])-1)) + ")"
                 self._db.executemany(insert, self._accumulator[1])
                 conn.commit()  

             # close the communication with the PostgreSQL
             self._db.close()
             
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
