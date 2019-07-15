# -*- coding: utf-8 -*-
'''
评论数据表shop转为parquet
'''
# from pyspark import SparkConf, SparkContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext

if __name__ == "__main__":

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    sqlContext = SQLContext(sc)
    pg_prop = {'user': 'postgres', 'password': 'postgres', 'driver': 'org.postgresql.Driver'}

    # column : 依门店shop_id作为分区标准
    # lowerBound 以select min(shop_id) from shop得知最小id是500061
    # upperBound 以select max(shop_id) from shop得知最大id是107756475
    # numPartition 查询出所有评论分为 1000个分区
    df = sqlContext.read\
        .jdbc(url='jdbc:postgresql://www.parramountain.com:54360/questionnaire',
              table='gkxs_origin.c_shop',
              properties=pg_prop)
    print(type(df))
    print(df.head())

    df.write.parquet('hdfs://192.168.0.61/gaia/test/c_shop')

    sc.stop()
