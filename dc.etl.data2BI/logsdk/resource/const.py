#coding=utf-8

'''
@Author:    wangzs@ct108.com
@Create:    2016/06/19
@Update:    2016/07/03
'''

CONFIG_PATH="configs/config.ini"
LOGSTASH_CONFIG_NAME = "configs/logstash.ini"

#远程接口配置section
CONFIG_SECTION_URLS = "urls"

#http user_agent头信息
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64;) Gecko/20100101 DC_Source_Api/1.0.0"

#webservice http访问头
WS_HTTP_HEADERS = {
    'Accept-Encoding': 'gzip, deflate, compress',
    'Content-Type': 'text/xml; charset=utf-8'
}

#编码
ENCODING = 'utf8'

#未知信息
UNKOWN = 'unkown'

FLUSH_INTERAL_MS = 600000