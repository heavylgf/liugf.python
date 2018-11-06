from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 1)

    lines = ssc.socketTextStream("localhost", 9999)
    # words = lines.flatMap( _.split("") )
    words = lines.flatMap( lambda line: line.split(" "))

    # pairs = words.map( word => (word, 1) )  
    pairs = words.map( lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y : x + y)

    wordCounts.pprint()

    ssc.start()
    ssc.awaitTermination()

# 注意：在linux上运行的时候，首先要装nc
# 执行命令：nc -L -p 9999 -v

# 再就是运行我们的python项目，这时候回到nc的界面会看到连接的相关进程
# web界面可以通过4040端口看到我们streaming运行的一些相关信息
# 再切换到nc界面 输入相关的单词
# 在python项目运行的命令行应该可以看到统计的相关信息



    # scala
    # val conf = new SparkConf()
    #     .setMaster("local[2]")
    #     .setAppName("WordCount")
    
    # val ssc = new StreamingContext(conf, Seconds(1))
    
    # val lines = ssc.socketTextStream("localhost", 9999)
    # val words = lines.flatMap { _.split(" ") }   
    # val pairs = words.map { word => (word, 1) }  
    # val wordCounts = pairs.reduceByKey(_ + _)  
    
    # Thread.sleep(5000)  
    # wordCounts.print()
    
    # ssc.start()
    # ssc.awaitTermination()


