# -*- coding:UTF-8 -*-

# author:郭健
# contact: guojian_emails@163.com
# datetime:2021/10/28 10:35
# software: PyCharm

"""
文件说明：
    实现内部模块的注册
"""
import json
import time
import paho.mqtt.client as mqtt

import compute

SOUTH_HOST = "47.106.121.88"
PORT = 1884
KEEP_ALIVE = 600

global liveCommunication

global module_communicate_client


def on_publish(client, userdata, mid):
    print("Publish Success\n")


def on_disconnect(client, userdata, rc):
    print("The mqtt connection is down, try reconnect")
    reconnect_error = client.reconnect()
    while reconnect_error != 0:
        print("Reconnect fail, the error code is" + reconnect_error)
    print("Reconnect Success")


# 构建内部通信的消息
def generate_mec_json(type, data, src_module):
    # 输入是一个字典
    json_message = json.loads(json.dumps({}))
    json_message['TimeStamp'] = int(round(time.time() * 1000))
    json_message['Type'] = type
    json_message['Src'] = src_module
    json_message['Data'] = data
    return json_message


def module_register(module_name, message_hook, connect_hook):
    client = mqtt.Client()
    client.on_connect = connect_hook
    client.on_message = message_hook
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.username_pw_set("mosquitto", "mosquitto")
    will_message = generate_mec_json('status', {"module": module_name, "status": "off"}, "RuleModule")
    client.will_set("MecInnerCommunication/service", str(will_message), 0, False)
    client.connect(SOUTH_HOST, PORT, KEEP_ALIVE)
    client.loop_start()
    print(module_name + "Register Communication Success")
    return client


def init():
    global module_communicate_client, liveCommunication
    print("INFO(RuleModule MODULE): INIT SUCCESS")
    module_communicate_client = module_register("RuleModule", message_hook, on_connect)
    module_communicate_client.subscribe("RuleModule/#", qos=0)
    module_communicate_client.subscribe("AllModule/#", qos=0)
    liveCommunication = True
    time.sleep(1)
    print("INFO(RuleModule MODULE): INIT SUCCESS")


def on_connect(client, userdata, flags, rc):
    message = generate_mec_json('status', {"module": "RuleModule", "status": "on"}, "RuleModule")
    module_communicate_client.publish("MecInnerCommunication/service", str(message))
    print("Info(MessageManager MODULE REGISTER): Connect Success")


# 收到内部通信消息的回调函数
def message_hook(client, userdata, msg):
    print("Module RuleModule Message Received\n")
    if str(msg.topic).split('/')[0] == "AllModule":
        print("Receive a broadcast message")
        print(str(msg.payload))
        broadcast_message_handler(msg.payload)
    else:
        if str(msg.topic).split('/')[-1] == "service":
            service_message_handler(msg.payload)
        else:
            print("INFO(RuleModule MODULE): Unknown topic type")


def service_message_handler(payload):
    global liveCommunication
    dic_message = eval(str(payload, 'utf-8'))
    if dic_message["Type"] == "calculate.req":
        dic_message["Data"]['device_id'] = "test_devcie"
        dic_message["Data"]['key'] = "test_key"
        dic_message["Data"]['value'] = "test_value"
        dic_message["Src"] = "RuleModule"
        dic_message["Type"] = "calculate.rep"
        print(str(dic_message))
        if liveCommunication:
            module_communicate_client.publish("SouthCommunication/service", str(dic_message))
        else:
            print("ERROR: Can't link MecInnerCommunication")
        print("finish compute")
    else:
        print("ERROR(ModuleManager MODULE): Unknown message type, drop message")

def broadcast_message_handler(message):
    dic_message = eval(message)
    if dic_message["Type"] == "status":
        parse_result = dic_message["Data"]
        if (parse_result['status'] == "off") & (parse_result['module'] == "MecInnerCommunication"):
            liveCommunication = False
        elif (parse_result['status'] == "on") & (parse_result['module'] == "MecInnerCommunication"):
            liveCommunication = True
            message = generate_mec_json('status', {"module": "RuleModule", "status": "on"}, "RuleModule")
            module_communicate_client.publish("MecInnerCommunication/service", str(message))
    else:
        print("ERROR(MecInnerCommunication MODULE): Unknown message type, drop message")