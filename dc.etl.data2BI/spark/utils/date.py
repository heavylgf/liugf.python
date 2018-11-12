#coding=utf-8

from datetime import date, datetime, timedelta


def dateIntervalIterator(start, end, delta):

    '''
    :param start:开始时间
    :param end: 结束时间
    :param delta: 累加天数
    :return: 时间区间中的每一天
    '''

    start_date = datetime.strptime(start, '%Y%m%d')
    end_date = datetime.strptime(end, '%Y%m%d')
    interval = timedelta(days=delta)

    curr = start_date
    while curr < end_date:
        yield curr.strftime("%Y%m%d")
        curr += interval