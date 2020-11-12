from time import time
from pyspark import SparkConf
from pyspark import SparkContext as sc
import os

def tell_time(start, end):
    print("The time of operation is :", (end - start), "s")

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = '/usr/share/tools/jdk1.8.0_211'
    os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.7'
    sc = sc.getOrCreate(SparkConf().setMaster("local[*]"))
    start = time()
    pokemon = sc.textFile("../machine_learning_with_spark/Pokemon.csv")
    end = time()
    tell_time(start, end)

    start1 = time()
    pokemon.collect()
    end1 = time()
    tell_time(start1, end1)

    print(pokemon.first())
    print(pokemon.take(2))

    header = pokemon.first()
    without_header = pokemon.filter(lambda r : (r != header))
    print(without_header)
    print(without_header.first())

    csv_rdd = without_header.map(lambda line : line.split(','))
    print(csv_rdd.first())

    fltmp = sc.parallelize(["this is one", "this is two", "this is five"])
    print(dict(fltmp.flatMap(lambda line: line.split(' ')).countByValue()))

    numeric_attributes = csv_rdd.map(lambda row:(int(row[6]), int(row[7])))
    attack = numeric_attributes.map(lambda r:r[0])
    defense = numeric_attributes.map(lambda r:r[1])
    print("unique attack value in attack column", attack.distinct().count())
    print("unique defense value in defense column", defense.distinct().count())

    attack_list_of_defense = numeric_attributes.groupByKey().map(lambda a, b : (a, list(b)))
    attack_wise = numeric_attributes.aggregateByKey((0, 0.0),
                                                    (lambda x, y: (x[0] + y, x[1] + 1)),
                                                    (lambda x, y: (x[0] + y[0], x[1] + y[1])))
    print(attack_wise.take(2))

    test, train = csv_rdd.randomSplit([3,7], 1234)
    print("total count", csv_rdd.count())
    print("test count", test.count())
    print("train count", train.count())

    print(pokemon.count())

    total_attack = attack.reduce(lambda x,y:x+y)
    print("total_attack = ", total_attack)

    total_attack1 = attack.sum()
    print("total_attack1 = ", total_attack1)
