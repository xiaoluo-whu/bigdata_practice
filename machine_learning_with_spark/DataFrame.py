from pyspark import SparkConf
from pyspark import SparkContext as sc
from pyspark import sql
import os

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = '/usr/share/tools/jdk1.8.0_211'
    os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.7'
    sc = sc.getOrCreate(SparkConf().setMaster("local[*]"))
    sqlContext = sql.SQLContext(sc)

    df = sqlContext.read.csv("Pokemon.csv", header=True)
    print(df.columns)
    print(df.take(10))
    print(df.count())
    print(df.describe().show())
    print(df.explain())
    print(df.explain(True))
    print("distinct attack count = ", df.select('Attack').dropDuplicates().count())
    print("not null type2 count = ", df.select('Type 2').dropna().count())
    print(df.dtypes)
    print(df.filter(df.Attack > 80).count())
    print(df.where(df.Attack == 80).count())
    print(df.groupBy("Attack").avg().collect())
    print(df.limit(5).collect())
    print(df.sort("Attack", ascending=False).collect())