#!/usr/bin/python3
# -*- coding: utf-8 -*-

'''
@Desc:      logstash日志操作类
@Author:    wangzs@ct108.com
@Create:    2017/05/16
@Update:    2018/04/18
'''

from ctdc.convert import json
import logging
import socket

from ctdc.config import ini
from ctdc.const import encoding
from resource import const
from utils import RuntimeTrace as trace

LOGSTASH_CONFIG_NAME = const.LOGSTASH_CONFIG_NAME
LOGSTASH_SECTION = "log"
CONFIG = ini.Config(config_name=LOGSTASH_CONFIG_NAME)
LOGSTASH_CONFIGS = CONFIG.getconfigs(LOGSTASH_SECTION)
LOGSTASH_HOST = LOGSTASH_CONFIGS["host"]
LOGSTASH_PORT = int(LOGSTASH_CONFIGS["port"])
LOGSTASH_EMAIL = LOGSTASH_CONFIGS["email"]
LOGSTASH_LEVEL = LOGSTASH_CONFIGS["level"].upper()
LOGSTASH_ON = LOGSTASH_CONFIGS["on"].strip().lower() == 'true'
LOGSTASH_ADDR = (LOGSTASH_HOST, LOGSTASH_PORT)
LOGSTASH_APP = LOGSTASH_CONFIGS["application"]

REPLACE_LIST = {
    "'":"\\\'",
    '"':'\\\"'
}

class Logger:
    '''
    logstash端的日志发送
    '''

    __monitor = True

    def __init__(self, monitor=True):
        if not LOGSTASH_ON:
            return

        try:
            self.__socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        except:
            trace.printtraceback()


    def send(self, level, logcontent):
        '''
        发送日志

        Args:
            level   str     日志级别
            log     obj     日志内容
        '''

        if not LOGSTASH_ON:
            return
        if not self.__socket:
            return


        message = {
            "email":LOGSTASH_EMAIL,
            "app":LOGSTASH_APP,
            "level":level
        }
        if isinstance(logcontent, str):
            message["content"] = logcontent
        elif isinstance(logcontent, dict):
            message.update(logcontent)


        if "content" in message:
            for old, new in REPLACE_LIST.items():
                message["content"] = message["content"].replace(old, new)
        self.__socket.sendto(json.dumps(message).encode(encoding.UTF8), LOGSTASH_ADDR)
