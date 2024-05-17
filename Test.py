#!/usr/bin/env python

import sys

sys.path.insert(0,'/home/humaira/CODE/Raccoon-AI-Engine/app/ml_models/bigdata')

from bigdata_preprocessor import *
from classification_eval import *

spark = SparkSession.builder.appName('IRIS').getOrCreate()

df = spark.read.csv('/home/humaira/CODE/Raccoon-AI-Engine/kafka-python/output/part-00000-8e354e2a-3255-453c-9cca-922fc3193f99-c000.csv', header = True, inferSchema = True)
obj = SmartEncoder(spark,df,'species',False,True,False,False)
df_fin = obj.dataAssembler('')
df_fin.show(5)

