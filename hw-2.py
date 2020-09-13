import re

from pyspark.sql import SparkSession
from datetime import datetime , timedelta
from pyspark.mllib.stat import Statistics


def pp(s):
    decorator = "=" * 50
    print("\n\n{} {} {}".format(decorator, s, decorator))


if __name__ == "__main__":

    spark = SparkSession.builder.appName("hw1").getOrCreate()
    sc = spark.sparkContext

    #test = sc.textFile("./test/") \
    test = sc.textFile("/user/n.lisin/test/") \
        .map(lambda line: line.split(",")) \
        .filter(lambda line: line[0]!="date")

    for f in test.take(5):
        print(f)

    max_map = test.map(lambda line: [line[6], max(float(line[1] or -999),
                                                      float(line[2] or -999),
                                                      float(line[3] or -999),
                                                      float(line[4] or -999))])

    max_reduce = max_map.reduceByKey(lambda a,b: max(a,b))

    min_map = test.map(lambda line: [line[6], min(float(line[1] or 9999),
                                                    float(line[2] or 9999),
                                                    float(line[3] or 9999),
                                                    float(line[4] or 9999))])

    min_reduce = min_map.reduceByKey(lambda a,b: min(a,b))

    all_reduce = max_reduce.join(min_reduce)

    #for f in all_reduce.take(5):
    #    print(f)


    #for f in all_reduce.map(lambda line: (line[0], line[1][0]-line[1][1])) \
    #        .sortBy(lambda x: x[1], ascending=False).map(lambda line: line[0]).take(5):
    #    print(f)

    task_2_a = all_reduce.map(lambda line: (line[0], line[1][0]-line[1][1])) \
            .sortBy(lambda x: x[1], ascending=False).map(lambda line: line[0])

    def FDPars(a, b):
        try:
            datetime.strptime(a, "%Y-%m-%d")
            float(b)
            return True
        except:
            return False


    def DateDiffer(arr):
        res = []
        for i in range(len(arr)):
            try:
                k = (arr[i][0] - arr[i - 1][0], arr[i][1] / arr[i - 1][1] - 1)
                res += [k]
            except:
                res += [(timedelta(days=5), arr[i][1])]

        return res


    def DateFilter(elem):
        return timedelta(days=1) == elem[0]


    task_2_b = test.filter(lambda x: FDPars(x[0], x[4])) \
        .map(lambda x: (x[6], (datetime.strptime(x[0], "%Y-%m-%d"), float(x[4])))) \
        .groupByKey() \
        .map(lambda x: (x[0], list(x[1]))) \
        .map(lambda x: (x[0], sorted(x[1]))) \
        .map(lambda x: (x[0], DateDiffer(x[1]))) \
        .map(lambda x: (x[0], list(filter(DateFilter, x[1])))) \
        .map(lambda x: (x[0], max(x[1])[1])) \
        .sortBy(keyfunc=lambda x: x[1], ascending=False)

    print("___________2________________")
    for f in task_2_b.take(5):
        print(f)

    t2_a = task_2_a.take(3)
    t2_b = task_2_b.take(3)

    result = []

    result += ["2a-{},{},{}\n".format(t2_a[0], t2_a[1], t2_a[2])]
    result += ["2b-{},{},{}\n".format(t2_b[0][0], t2_b[1][0], t2_b[2][0])]

    #print(result)
    sc.parallelize(result).repartition(1).saveAsTextFile("/user/n.lisin/hw1/result.txt")
    
    spark.stop()