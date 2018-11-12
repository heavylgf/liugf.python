#coding=utf-8

from ctdc.log.dclog import DcLogger
from ctdc.config import ini
from resource import const
import os,sys


class logging:

    def __init__(self,sparkSession,logLevel="WARN"):

        sparkSession.sparkContext.setLogLevel(logLevel)

        sparkLoggerInit = sparkSession._jvm.org.apache.log4j.Logger
        self.sparkLogger = sparkLoggerInit.getLogger(__name__)

        self.dclog = DcLogger()



    def debug(self, msg=None, event=None):
        self.dclog.debug(msg,event)
        self.sparkLogger.debug("event:%s  msg:%s"%(event,msg))

    def info(self, msg=None, event=None):
        self.dclog.info(msg,event)
        self.sparkLogger.info("event:%s  msg:%s"%(event,msg))

    def warn(self, msg=None, event=None):
        self.dclog.warn(msg,event)
        self.sparkLogger.warn("event:%s  msg:%s"%(event,msg))

    def error(self, msg=None, event=None):
        self.dclog.error(msg,event)
        self.sparkLogger.error("event:%s  msg:%s"%(event,msg))

