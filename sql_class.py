#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# sql class, but treat more like a source for code snippets since it originally served a different purpose
# thus the references to pubsub, configparser etc.
#

from psycopg2 import extensions, connect, InterfaceError, sql
import psycopg2.errors

# import time


class SQL_instance():
	
	def __init__(self):
		self.isConnected = 0
		self.read_sql_settings()
        
        self.sql_server = '127.0.0.1:5432'
        self.sql_database = 'postgres'
        self.sql_user = 'postgres'
        self.sql_password = 'postgres'

	def return_isConnected(self):
		try:
			self.check_poll_status()
			return 1
		except:
			return 0

	def start_client(self):
		self.connect_database()
		# self.start_time = time.time()

	# def read_sql_settings(self):
		# sqlsettings = configparser.ConfigParser()
		# sqlsettings.read(getenv("APPDATA") + "\\sqlclass\\paths.ini")
		# self.sql_server = sqlsettings['SQL']['server']
		# self.sql_port = sqlsettings['SQL']['port']
		# self.sql_user = sqlsettings['SQL']['user']
		# self.sql_password = sqlsettings['SQL']['password']
		# self.sql_database= sqlsettings['SQL']['db']

	def connect_database(self):

		try:
			self.db_connection = connect(host=self.sql_server, database=self.sql_database, user=self.sql_user, password=self.sql_password)
			self.db_connection.autocommit = True
			self.cursor = self.db_connection.cursor()
			self.isConnected = 1
		except:
			self.isConnected = 0
		return self.isConnected


	def get_row_number(self, table):
		try:
            q1 = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))
			self.cursor.execute(q1)
			return self.cursor.fetchone()[0]
		except:
			self.check_poll_status()
			self.get_transaction_status()
			self.db_connection.rollback()
			self.cursor.execute("select pg_sleep(1)")
			self.check_poll_status()
			return 0


	def write_to_db(self, table="", **kwargs):
		keys = []
		values = []

		print(len(kwargs))

		for k,v in kwargs.items():
			keys.append(str(k))
			values.append(v)

		try:
			if len(kwargs) > 1:
				q1 = sql.SQL("INSERT INTO {} ({}) VALUES ({});").format(sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, keys)),sql.SQL(', ').join(sql.Placeholder() * len(keys)))
			else:
				q1 = sql.SQL("INSERT INTO {} {} VALUES {};").format(sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, keys)),sql.SQL(', ').join(sql.Placeholder() * len(keys)))
			self.cursor.execute(q1.as_string(self.cursor), values)
		except:
			# pub.sendMessage('sql_to_gui', arg1=1, arg2='Fehler bei der SQL-Verbindung - Schreiben fehlgeschlagen!')
            print('Fehler beim Schreiben')


	def update_value_in_table(self, table="", **kwargs):
		keys = []
		values = []

		for k,v in kwargs.items():
			keys.append(str(k))
			values.append(v)

		try:
			if len(kwargs) > 1:
				q1 = sql.SQL("UPDATE {} SET ({}) = ({});").format(sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, keys)),sql.SQL(', ').join(sql.Placeholder() * len(keys)))
			else:
				q1 = sql.SQL("UPDATE {} SET {} = {};").format(sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, keys)),sql.SQL(', ').join(sql.Placeholder() * len(keys)))
			self.cursor.execute(q1.as_string(self.cursor), values)
		except:
			# pub.sendMessage('sql_to_gui', arg1=1, arg2='Fehler bei der SQL-Verbindung - Schreiben fehlgeschlagen!')
            print('Fehler beim Schreiben')



	def read_last_value_in_table(self, column, table="", ):
		q1 = sql.SQL("SELECT {} FROM {} WHERE {} is not null ORDER BY unixtime DESC LIMIT 1").format(sql.Identifier(column), sql.Identifier(table), sql.Identifier(column))
		self.cursor.execute(q1)
		retval = self.cursor.fetchone()
		if not retval:
			retval = [0]
		return retval[0]


	def read_last_n_values_in_table(self, column, table="", i=5):
		q1 = sql.SQL("SELECT {} FROM {} WHERE {} is not null ORDER BY unixtime DESC LIMIT (%s)").format(sql.Identifier(column), sql.Identifier(table), sql.Identifier(column))
		self.cursor.execute(q1,(i,))
		retval = self.cursor.fetchmany(i)
		print(retval)
		return retval[0]
        

	def check_poll_status(self):
		"""
		extensions.POLL_OK == 0
		extensions.POLL_READ == 1
		extensions.POLL_WRITE == 2
		"""
		if self.db_connection.poll() == extensions.POLL_OK:
			poll = "POLL: POLL_OK"
		if self.db_connection.poll() == extensions.POLL_READ:
			poll = "POLL: POLL_READ"
		if self.db_connection.poll() == extensions.POLL_WRITE:
			poll = "POLL: POLL_WRITE"
		print(poll)
		return poll

        
	# define a function that returns the PostgreSQL connection status
	def get_transaction_status(self):
		# print the connection status
		print ("\nconn.status:", self.db_connection.status)

		# evaluate the status for the PostgreSQL connection
		if self.db_connection.status == extensions.STATUS_READY:
			print ("psycopg2 status #1: Connection is ready for a transaction.")

		elif self.db_connection.status == extensions.STATUS_BEGIN:
			print ("psycopg2 status #2: An open transaction is in process.")

		elif self.db_connection.status == extensions.STATUS_IN_TRANSACTION:
			print ("psycopg2 status #3: An exception has occured.")
			print ("Use tpc_commit() or tpc_rollback() to end transaction")

		elif self.db_connection.status == extensions.STATUS_PREPARED:
			print ("psycopg2 status #4:A transcation is in the 2nd phase of the process.")
		return self.db_connection.status


	def close_connection(self):
		try:
			self.cursor.close()
			self.db_connection.close()
			self.isConnected = 0

		except:
			pass