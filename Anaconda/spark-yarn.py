from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
#conf.setMaster('yarn-client')
conf.setAppName('spark-yarn')
sc = SparkContext(conf=conf)


def mod(x):
    import numpy as np
    return (x, np.mod(x, 2))

rdd = sc.parallelize(range(1000)).map(mod).take(10)
print rdd

#command to run this job : PYSPARK_PYTHON=/opt/anaconda/bin/python spark-submit --master yarn-cluster --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/anaconda/bin/python spark-yarn.py
