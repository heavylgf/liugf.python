import datetime
import sys
import time

from perform.MongoExecute import mongoExecute
from perform.SparkInit import sparkInitialize
from utils.HiveAlter import partition
from utils.SubmitArguments import arguments
from utils.logging import logging


sparkSession = sparkInitialize()\
    .setAppName("StreamingListenerDemo")

spark = sparkSession.getOrCreate()

# val sc = new SparkContext(conf)
#     val ssc = new StreamingContext(sc, Seconds(10))





if __name__ == "__main__":
    argv = arguments(sys.argv)
    # if argv["start_date"] is None or argv["end_date"] is None:
    #     logic()
    # else:
    #     logic(argv["start_date"], argv["end_date"])
