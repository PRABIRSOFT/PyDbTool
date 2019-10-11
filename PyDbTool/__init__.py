#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2019-07-16
# @Author  : Prabir Ghosh
# @Version : 0.1
#
# PyDbTool
__version__ = "2.0.1"

from cassandra.cluster import Cluster as __Cluster__
from cassandra.auth import PlainTextAuthProvider as __PlainTextAuthProvider__
import psycopg2 as __psycopg2__
import pandas as __pd__
import sqlite3 as __sql__
import pymongo as __pm__


def __determine_query_type__(query):    
    
    try:
        query.lower().index('select')
        return "select"
    except:
        return "create"
    
class connect():

    __driver__ = None
    __config__ = None

    def __init__(self, d_type, config=None):
        if(d_type.lower() == "postgres"):
            self.__config__ = {"host":"localhost", "user":"postgres", "password":"postgres", "dbname":"postgres"}
            if config != None:
                try:
                    for i in config:
                        self.__config__[i] = config[i]
                except Exception as e:
                    print(e)
            self.__driver__ = __PGDriver__(self.__config__)
                
        elif(d_type.lower() == "cassandra"):
            self.__config__ = {'hostname':'127.0.0.1', 'username':'cassandra', 'password':'cassandra', 'default_fetch_size':10000000}
            if(config != None):
                try:
                    for i in config:
                        self.__config__[i] = config[i]
                except Exception as e:
                    print(e)
            self.__driver__ = __CassandraDriver__(self.__config__)
        else:
            print("no driver found ... ")

    def execute(self, query):
        return self.__driver__.execute(query)

    
class __PGDriver__():
    
    __conn_string__ = None
    __conn__        = None
    
    def __init__(self, config):
        self.__conn_string__ = config
        print("connection string updated ...")
        
        
    def execute(self, query):
        self.__conn__ = __psycopg2__.connect(**self.__conn_string__)
        
        try:
            if(__determine_query_type__(query) == "select"):
                return __pd__.read_sql_query(query,con=self.__conn__)
            else:
                self.__conn__.cursor().execute(query)
                self.__conn__.commit()
                
        except Exception as e:
            print(e)
            
        finally:
            self.__conn__.close()
            
        
class __CassandraDriver__():
    __cluster__ = None
    __session__ = None
    
    def __init__(self, config):
        try:
            self.__cluster__           = __Cluster__(
                contact_points = [config['hostname']],
                auth_provider  = __PlainTextAuthProvider__(username=config['username'], password=config['password'])
            )
            
            self.__default_fetch_size__ = config['default_fetch_size']
            print("connection established ...")
            
        except Exception as e:
            print(e)
            
    def execute(self, query):
        self.__session__                    = self.__cluster__.connect()
        self.__session__.row_factory        = self.__pandas_factory__
        self.__session__.default_fetch_size = self.__default_fetch_size__
            
        try:
            if(__determine_query_type__(query) == "select"):
                return self.__session__.execute(query)._current_rows
            else:
                self.__session__.execute(query)  
                
        except Exception as e:
            print(e)
            
        finally:
            self.__session__.shutdown()
            
    def __pandas_factory__(self, colnames, rows):
        return __pd__.DataFrame(rows, columns=colnames)