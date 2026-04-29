import os
from pyspark.sql.functions import col, count, desc, row_number, avg, floor, explode, array_contains
from pyspark.sql.window import Window
from extract import (
    title_basics_df,
    title_ratings_df,
    title_episode_df
)

def t26_avg_runtime_by_title_type():
    print("Executing Tymofeiuk Q26: Середня тривалість за типом тайтлу")
    avg_runtime = title_basics_df.filter(col("titleType").isin("movie", "short", "tvMovie")) \
        .filter(col("runtimeMinutes").isNotNull()) \
        .groupBy("titleType").agg(avg("runtimeMinutes").alias("avg_runtime"))
    
    avg_runtime.toPandas().to_csv("results/t26_avg_runtime_by_title_type.csv", index=False)

def t27_runtime_rating_correlation():
    print("Executing Tymofeiuk Q27: Кореляція тривалості та рейтингу")
    movies = title_basics_df.filter(col("titleType") == "movie") \
        .filter(col("runtimeMinutes").isNotNull()) \
        .join(title_ratings_df, "tconst")
    
    buckets = movies.withColumn("runtime_bucket", floor(col("runtimeMinutes") / 30) * 30) \
        .groupBy("runtime_bucket").agg(
            avg("averageRating").alias("avg_rating"),
            count("tconst").alias("movie_count")
        ).orderBy("runtime_bucket")
    
    buckets.toPandas().to_csv("results/t27_runtime_rating_correlation.csv", index=False)

def t28_shortest_series_total_runtime():
    print("Executing Tymofeiuk Q28: Топ 10 найкоротших серіалів")
    episodes_runtime = title_episode_df.join(title_basics_df.select("tconst", "runtimeMinutes"), "tconst") \
        .filter(col("runtimeMinutes").isNotNull())
    
    series_runtime = episodes_runtime.groupBy("parentTconst").agg(sum(col("runtimeMinutes")).alias("total_runtime")) \
        .orderBy("total_runtime").limit(10)
    
    series_runtime.join(title_basics_df.withColumnRenamed("tconst", "parentTconst"), "parentTconst") \
        .select("primaryTitle", "total_runtime") \
        .toPandas().to_csv("results/t28_shortest_series_total_runtime.csv", index=False)

def t29_short_vs_movie_rating_by_year():
    print("Executing Tymofeiuk Q29: Порівняння Short та Movie за роками")
    titles = title_basics_df.filter(col("titleType").isin("movie", "short")) \
        .filter(col("startYear").isNotNull()) \
        .join(title_ratings_df, "tconst")
    
    comparison = titles.groupBy("startYear", "titleType").agg(avg("averageRating").alias("avg_rating")) \
        .orderBy("startYear")
    
    comparison.toPandas().to_csv("results/t29_short_vs_movie_rating_by_year.csv", index=False)

def t30_movies_longer_than_genre_avg():
    print("Executing Tymofeiuk Q30: Фільми, довші за середні у жанрі")
    movies = title_basics_df.filter(col("titleType") == "movie") \
        .filter(col("runtimeMinutes").isNotNull() & col("genres").isNotNull())
    
    movies_exploded = movies.withColumn("genre", explode(col("genres")))
    
    genre_avg = movies_exploded.groupBy("genre").agg(avg("runtimeMinutes").alias("genre_avg_runtime"))
    
    longer_movies = movies_exploded.join(genre_avg, "genre") \
        .filter(col("runtimeMinutes") > col("genre_avg_runtime")) \
        .select("primaryTitle", "genre", "runtimeMinutes", "genre_avg_runtime") \
        .limit(100)
    
    longer_movies.toPandas().to_csv("results/t30_movies_longer_than_genre_avg.csv", index=False)

from pyspark.sql.functions import sum

def run_tymofeiuk_questions():
    t26_avg_runtime_by_title_type()
    t27_runtime_rating_correlation()
    t28_shortest_series_total_runtime()
    t29_short_vs_movie_rating_by_year()
    t30_movies_longer_than_genre_avg()
