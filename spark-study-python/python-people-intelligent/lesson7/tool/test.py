import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# import bussiness.model1
# import bussiness.model2

import bussiness.model3
import tool.model1

# from bussiness import *
bussiness.model3.test1()
bussiness.model3.test2()
bussiness.model3.test3()

# 在bussiness 的init() 中加入了  __all__ = ["model3"]  代表只允许bussiness包下的model3被引用
# 所以调用如下会报错
# bussiness.model1.project_info()





