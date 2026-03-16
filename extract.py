from pyspark.sql import SparkSession

from schemas import name_basics_schema, title_akas_schema, title_basics_schema, title_crew_schema, title_episode_schema, \
    title_principals_schema, title_ratings_schema

spark = SparkSession.builder \
    .appName("IMDb DataFrames") \
    .getOrCreate()

name_basics_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(name_basics_schema) \
    .csv("data/name.basics.tsv.gz")

title_akas_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(title_akas_schema) \
    .csv("data/title.akas.tsv.gz")

title_basics_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(title_basics_schema) \
    .csv("data/title.basics.tsv.gz")

title_crew_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(title_crew_schema) \
    .csv("data/title.crew.tsv.gz")

title_episode_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(title_episode_schema) \
    .csv("data/title.episode.tsv.gz")

title_principals_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(title_principals_schema) \
    .csv("data/title.principals.tsv.gz")

title_ratings_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(title_ratings_schema) \
    .csv("data/title.ratings.tsv.gz")