# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/11/1 19:10
# software: PyCharm

"""
文件说明：
    计算实体
"""
import sqlite3

global conn


def init():
    global conn
    conn = sqlite3.connect("./database_file/rule.db", check_same_thread=False)


def computeInstance(data):
    print(data["device_id"])


def create_table(rule_id):
    cur = conn.cursor()
    sql_text = '''CREATE TABLE IF NOT EXISTS "''' + rule_id + '''" (type TEXT, key_name TEXT, value TEXT);'''
    cur.execute(sql_text)
    conn.commit()
    cur.close
