from pyspark.sql import SparkSession

from schemas import name_basics_schema, title_akas_schema, title_basics_schema, title_crew_schema, title_episode_schema, \
    title_principals_schema, title_ratings_schema
from pyspark.sql.functions import col, split

spark = SparkSession.builder \
    .appName("IMDb DataFrames") \
    .getOrCreate()

name_basics_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(name_basics_schema) \
    .csv("data/name.basics.tsv.gz")

# Parse name_basics
name_basics_df = name_basics_df \
    .withColumn("primaryProfession", split(col("primaryProfession"), ",")) \
    .withColumn("knownForTitles", split(col("knownForTitles"), ","))

title_akas_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(title_akas_schema) \
    .csv("data/title.akas.tsv.gz")

# Parse title_akas
title_akas_df = title_akas_df \
    .withColumn("isOriginalTitle", col("isOriginalTitle").cast("boolean")) \
    .withColumn("types", split(col("types"), ",")) \
    .withColumn("attributes", split(col("attributes"), ","))

title_basics_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(title_basics_schema) \
    .csv("data/title.basics.tsv.gz")

# Parse title_basics
title_basics_df = title_basics_df \
    .withColumn("isAdult", col("isAdult").cast("boolean")) \
    .withColumn("genres", split(col("genres"), ","))

title_crew_df = spark.read \
    .option("sep", "\t") \
    .option("header", True) \
    .option("nullValue", "\\N") \
    .schema(title_crew_schema) \
    .csv("data/title.crew.tsv.gz")

# Parse title_crew
title_crew_df = title_crew_df \
    .withColumn("directors", split(col("directors"), ",")) \
    .withColumn("writers", split(col("writers"), ","))

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