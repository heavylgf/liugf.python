#coding=utf-8

from utils.Date import dateIntervalIterator

class partition():

    def __init__(self,spark,logger):
        self.spark = spark
        self.logger = logger

    def dropPartition(self, table, colume, start_date, end_date):

        # show_partition_statement = "show partitions %s" % (table)
        #
        # table_partition = self.spark.sql(show_partition_statement)

        for exectue_date in dateIntervalIterator(start_date, end_date, 1):

            alter_partition_drop_statement = "alter table %(table_name)s drop partition(%(partition_col)s='%(date)s')" %{"table_name":table,
                                                                                                       "partition_col":colume,
                                                                                                       "date":exectue_date}

            self.logger.warn(alter_partition_drop_statement, "hive_alter")
            try:
                self.spark.sql(alter_partition_drop_statement)
            except:
                self.logger.warn("No partition %(date)s in %(table_name)s" %{"date": exectue_date, "table_name":table},"hive_alter")



