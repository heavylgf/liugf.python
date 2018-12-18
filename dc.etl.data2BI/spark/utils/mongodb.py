'''
@Author:    wangzs@ct108.com
@Create:    2016/06/19
@Update:    2017/04/19
'''

import time
import uuid

import pymongo
from pymongo import monitoring

from ctdc.const import encrypt
from ctdc.convert import datetime, json
from ctdc.db.error import AuthenticationError
from ctdc.encrypt import aes
from ctdc.tool import counter


SESSIONS = dict()

COUNTER_MONGO = {
    "create_ts": time.time()
}

CMD_EXECLUDE = {}

EVENT_SLOW = 'mongodb.op.slow'
EVENT_COUNT = 'mongodb.op.count'

global ON_MONITOR_MONGO
global INTERVAL_MONITOR_MONGO
global SLOWMS_MONGO

ON_MONITOR_MONGO = True
INTERVAL_MONITOR_MONGO = 600
SLOWMS_MONGO = 100


def set_options(monitor=True, interval=600, slowms=100):
    '''
    设置高级选项

    Args:
        monitor     bool    是否开启监听
        interval    int     监听循环间隔（秒）
        showms      int     慢操作线（毫秒）
    '''
    global ON_MONITOR_MONGO
    global INTERVAL_MONITOR_MONGO
    global SLOWMS_MONGO
    ON_MONITOR_MONGO = monitor
    INTERVAL_MONITOR_MONGO = interval
    SLOWMS_MONGO = slowms


def auto_report_op_count():
    '''
    自动上报操作计数
    '''
    def __report_op_count(func):
        def __report(*args, **kwargs):
            if not ON_MONITOR_MONGO or ("create_ts" not in COUNTER_MONGO) or len(COUNTER_MONGO) < 2:
                return func(*args, **kwargs)
            ts1 = COUNTER_MONGO.get("create_ts")
            ts2 = time.time()
            duration = ts2 - ts1
            if duration > INTERVAL_MONITOR_MONGO:
                counter = COUNTER_MONGO.copy()
                del counter["create_ts"]
                COUNTER_MONGO.clear()
                COUNTER_MONGO["create_ts"] = time.time()

            return func(*args, **kwargs)
        return __report
    return __report_op_count


class CommandLogger(monitoring.CommandListener):
    @auto_report_op_count()
    def started(self, event):
        if event.command_name.lower() in CMD_EXECLUDE:
            return

        @counter.auto_count(COUNTER_MONGO, event.command_name, on=ON_MONITOR_MONGO)
        def set_session():
            SESSIONS[event.request_id] = {
                "content": str(event.command),
                "database": event.database_name,
                "cmd": event.command_name,
                "uuid": str(uuid.uuid1())
            }
        set_session()

    def succeeded(self, event):
        session = SESSIONS.get(event.request_id)
        if not session:
            return
        del SESSIONS[event.request_id]



    def failed(self, event):
        session = SESSIONS.get(event.request_id)
        if not session:
            return
        del SESSIONS[event.request_id]



class HeartbeatLogger(monitoring.ServerHeartbeatListener):
    def started(self, event):
        pass

    def failed(self, event):
        pass

    def succeeded(self, event):
        pass


class MongoDB:
    '''
    mongodb帮助类
    '''

    def __init__(self, addr, username, passwd, adminauth=True, document_class=dict, tz_aware=False, connect=True, **kwargs):
        self.__adminauth = adminauth
        self.__username = username
        self.__password = aes.decrypt(passwd, encrypt.MONGODB).strip()

        self.__client = pymongo.MongoClient(
            addr,
            event_listeners=[CommandLogger(), HeartbeatLogger()],
            document_class=document_class, tz_aware=tz_aware, connect=connect,
            **kwargs
        )
        if self.__adminauth:
            admin = self.__client["admin"]
            if not admin.authenticate(self.__username, self.__password):
                raise AuthenticationError()

    def usedb(self, dbname):
        '''
        根据database name获取数据库对象

        Args：
            dbname  string  数据库名称
        Returns:
            数据库对象
        '''

        database = self.__client[dbname]
        if not self.__adminauth:
            if database.authenticate(self.__username, self.__password):
                return database
            else:
                raise AuthenticationError()
        else:
            return database

    def get_client(self):
        '''获取客户端链接对象'''

        return self.__client


json.add_objs_execlude(MongoDB)