
# Executing Spark programs 

1. Copy the als_example.py, als_example.py, build.sbt and sample_movielens_ratings.txt to EC2.

1. Execute the Spark python program.
    >python als_example.py

1. Execute the Spark scala program.
    >sbt clean package  
    >spark-submit --class org.apache.spark.examples.ml.ALSExample target/scala-2.11/als_2.11-0.0.1.jar

# Further Reading

1. Collaborative Filtering Program
    - https://spark.apache.org/docs/2.2.0/ml-collaborative-filtering.html

1. Movie Lens Data
    - https://grouplens.org/datasets/movielens/