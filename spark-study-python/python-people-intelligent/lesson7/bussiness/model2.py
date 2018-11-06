import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import bussiness.model1
import tool.model1
# from bussiness import model1
# model1.project_info()

bussiness.model1.project_info()
tool.model1.tool_info()



