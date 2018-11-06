import pymongo
from pymongo import MongoClient



client_50001 = MongoClient('localhost',27017)
client_50002 = MongoClient('localhost',27017)
client_50003 = MongoClient('localhost',27017)
client_50004 = MongoClient('localhost',27017)
client_50005 = MongoClient('localhost',27017)


db = client_50001.test
users = db.users








