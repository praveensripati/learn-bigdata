
export SPARK_HOME=/home/ubuntu/spark-2.4.7-bin-hadoop2.7
export PATH=$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH


head -n1000 1987.csv > sample.csv


spark-shell

val df = spark.read.option("header",true).format("csv").load("s3://airline-dataset-praveen/airline-csv-subset/1987-subset-1000.csv")
val df = spark.read.option("header",true).format("csv").load("/FileStore/tables/1987_subset_1000.csv")

df.show(10)
df.printSchema()

df.createOrReplaceTempView("ontime")

## Find the best and worst airports by infering the number of flights delayes

df.filter($"DepTime" > $"CRSDepTime").groupBy("Origin").count().orderBy($"count".desc).show()

val sqlDF1 = spark.sql("select Origin, count(*) as Delays from ontime where DepTime > CRSDepTime group by Origin order by Delays DESC")
sqlDF1.show()

df2.write.option("header","true").csv("s3://airline-dataset-praveen/output")

## Find which day of the week most of the flights depart

1 (Monday) - 7 (Sunday)

df.groupBy("DayOfWeek").count().orderBy($"count".desc).show()

val sqlDF2 = spark.sql("select DayOfWeek, count(*) as WeekDayCount from ontime group by DayOfWeek order by WeekDayCount DESC")
sqlDF2.show()

## Find on which day of the week most og the flights get delayed

df.filter($"DepTime" > $"CRSDepTime").groupBy("DayOfWeek").count().orderBy($"count".desc).show()

val sqlDF3 = spark.sql("select DayOfWeek, count(*) as WeekDayCount from ontime where DepTime > CRSDepTime group by DayOfWeek order by WeekDayCount DESC")
sqlDF3.show()

