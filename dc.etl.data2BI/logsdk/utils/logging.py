#coding=utf-8


from ctdc.convert import json
from utils import LogError as error
from utils import RuntimeTrace as trace



class logging:

    def __init__(self,sparkSession,logLevel="WARN"):

        sparkSession.sparkContext.setLogLevel(logLevel)

        sparkLoggerInit = sparkSession._jvm.org.apache.log4j.Logger
        self.sparkLogger = sparkLoggerInit.getLogger(__name__)

        try:
            from utils import Logstash
            self.__logstash = Logstash.Logger()
        except:
            trace.printtraceback()


    def __fill_trace_info(self,  msg, event, **kwargs):
        '''
        补充当前跟踪信息
        '''

        infos = {}

        if event:
            if not isinstance(event, str):
                raise error.InvalidMessageError("非法的event信息")
            else:
                infos["event"] = event

        if msg:
            if isinstance(msg, dict):
                infos["content"] = json.dumps(msg)
            elif isinstance(msg, list):
                infos["content"] = json.dumps(msg)
            elif isinstance(msg, tuple):
                infos["content"] = json.dumps(msg)
            elif not isinstance(msg, str):
                raise error.InvalidMessageError("非法的message信息")
            else:
                infos["content"] = msg
        else:
            infos["content"] = infos.get("event")

        if "content" in infos:
            if isinstance(infos["content"], dict) or \
                    isinstance(infos["content"], list) or \
                    isinstance(infos["content"], tuple):
                infos["content"] = json.dumps(infos["content"])

        for key in kwargs:
            filed = str(key).lower()
            if filed in ["num", "duration"] and (
                    isinstance(kwargs.get(key), int) or isinstance(kwargs.get(key), float)):
                infos[filed] = kwargs.get(key)
            else:
                infos["extends.%s" % filed] = kwargs.get(key)

        log_logstash = infos

        co_filename, f_lineno, co_name = trace.findcaller(self.__init__.__code__.co_filename)

        log_logstash["pyfile"] = co_filename
        log_logstash["lineno"] = f_lineno
        log_logstash["function"] = co_name

        return log_logstash



    def debug(self, msg=None, event=None, **kwargs):
        log_logstash = self.__fill_trace_info( msg, event, **kwargs)
        if self.__logstash:
            self.__logstash.send("debug", log_logstash)

        self.sparkLogger.debug("event:%s  msg:%s"%(event,msg))

    def info(self, msg=None, event=None, **kwargs):
        log_logstash = self.__fill_trace_info(msg, event, **kwargs)
        if self.__logstash:
            self.__logstash.send("info", log_logstash)

        self.sparkLogger.info("event:%s  msg:%s"%(event,msg))

    def warn(self, msg=None, event=None, **kwargs):
        log_logstash = self.__fill_trace_info(msg, event, **kwargs)
        if self.__logstash:
            self.__logstash.send("warn", log_logstash)

        self.sparkLogger.warn("event:%s  msg:%s"%(event,msg))

    def error(self, msg=None, event=None, **kwargs):
        log_logstash = self.__fill_trace_info(msg, event, **kwargs)
        if self.__logstash:
            self.__logstash.send("error", log_logstash)

        self.sparkLogger.error("event:%s  msg:%s"%(event,msg))

