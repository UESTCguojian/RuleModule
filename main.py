# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/11/1 19:10
# software: PyCharm

"""
文件说明：
    主函数
"""
import time
import MecInnerCommunication


def init():
    MecInnerCommunication.init()
    while True:
        time.sleep(10)


if __name__ == '__main__':
    init()
