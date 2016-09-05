__author__ = 'zhangps'


import psycopg2

global_sql_get_db_all_tables = "select schemaname,tablename from pg_tables where tablename not like 'pg%' and tablename not like 'sql_%' order by tablename;"
global_sql_get_db_all_views = "SELECT schemaname,viewname from pg_views where schemaname != 'information_schema'and viewname not like 'pg%' and viewname not like '_pg_%' order by viewname;"
global_sql_get_table_all_clumns = "select  l.ordinal_position,l.column_name,l.udt_name,l.is_nullable,l.column_default,d.description from information_schema.columns as l INNER JOIN pg_attribute As a ON l.column_name = a.attname INNER JOIN pg_class As c ON  a.attrelid = c.oid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace LEFT JOIN pg_description As d ON (d.objoid = c.oid AND d.objsubid = a.attnum) where l.table_schema =  %s  and l.table_name =  %s and n.nspname = %s AND c.relname = %s;"
global_sql_get_table_primary_key = "select pg_attribute.attname as colname from pg_constraint inner join pg_class on pg_constraint.conrelid = pg_class.oid inner join pg_attribute on pg_attribute.attrelid = pg_class.oid and pg_attribute.attnum = ANY(pg_constraint.conkey) where pg_class.relname =%s and pg_constraint.contype='p';"


def get_connection():
    # return psycopg2.connect(host="192.168.52.132", port="5432", database="smarthome", user="zhangps", password="123456")
    return psycopg2.connect(host="10.2.10.88", port="5432", database="smarthome", user="smarthome", password="smarthome")


def get_cursor(conn):
    return conn.cursor()


def close_cursor(cur):
    cur.close()


def close_connection(conn):
    conn.close()
