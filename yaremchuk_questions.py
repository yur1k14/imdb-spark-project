import os
from pyspark.sql.functions import col, count, desc, row_number, avg, when, array_contains
from pyspark.sql.window import Window
from extract import (
    title_basics_df,
    title_ratings_df,
    title_akas_df
)

def y32_top_5_movies_by_aka_count_per_year():
    print("Executing Yaremchuk Q32: Топ 5 фільмів за кількістю альтернативних назв по роках")
    akas_count = title_akas_df.groupBy("titleId").agg(count("title").alias("aka_count"))
    
    movies = title_basics_df.filter(col("titleType") == "movie") \
        .filter(col("startYear").isNotNull()) \
        .join(akas_count, col("tconst") == col("titleId"))
    
    window_spec = Window.partitionBy("startYear").orderBy(desc("aka_count"))
    top_movies = movies.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 5)
    
    top_movies.select("startYear", "primaryTitle", "aka_count", "rank") \
        .orderBy("startYear", "rank") \
        .toPandas().to_csv("results/y32_top_5_movies_by_aka_count_per_year.csv", index=False)

def y33_ua_localization_impact():
    print("Executing Yaremchuk Q33: Вплив української локалізації на рейтинг")
    ua_localized = title_akas_df.filter(col("region") == "UA").select("titleId").distinct() \
        .withColumn("has_ua", when(col("titleId").isNotNull(), True))
    
    movies_ratings = title_basics_df.filter(col("titleType") == "movie") \
        .join(title_ratings_df, "tconst") \
        .join(ua_localized, col("tconst") == col("titleId"), "left") \
        .fillna(False, subset=["has_ua"])
    
    comparison = movies_ratings.groupBy("has_ua").agg(
        avg("averageRating").alias("avg_rating"),
        count("tconst").alias("movie_count")
    )
    
    comparison.toPandas().to_csv("results/y33_ua_localization_impact.csv", index=False)

def y34_original_title_matches_localized():
    print("Executing Yaremchuk Q34: Фільми, де оригінальна назва збігається з локалізованою")
    # Join basics with akas to compare primaryTitle with title
    matches = title_basics_df.filter(col("titleType") == "movie") \
        .join(title_akas_df, col("tconst") == col("titleId")) \
        .filter(col("primaryTitle") == col("title")) \
        .groupBy("tconst", "primaryTitle").agg(count("region").alias("match_count")) \
        .orderBy(desc("match_count")).limit(20)
    
    matches.toPandas().to_csv("results/y34_original_title_matches_localized.csv", index=False)

def y35_doc_localization_languages():
    print("Executing Yaremchuk Q35: Мови локалізації документальних фільмів")
    docs = title_basics_df.filter((col("titleType") == "movie") & (array_contains(col("genres"), "Documentary")))
    
    doc_akas = docs.join(title_akas_df, col("tconst") == col("titleId")) \
        .filter(col("language").isNotNull() & (col("language") != "\\N"))
    
    lang_counts = doc_akas.groupBy("language").agg(count("titleId").alias("doc_count")) \
        .orderBy(desc("doc_count")).limit(20)
    
    lang_counts.toPandas().to_csv("results/y35_doc_localization_languages.csv", index=False)

def y36_top_3_movies_per_region():
    print("Executing Yaremchuk Q36: Топ 3 фільми для кожного регіону")
    region_movies = title_akas_df.filter(col("region").isNotNull() & (col("region") != "\\N")) \
        .join(title_ratings_df, col("titleId") == col("tconst")) \
        .join(title_basics_df.filter(col("titleType") == "movie"), "tconst")
    
    window_spec = Window.partitionBy("region").orderBy(desc("averageRating"), desc("numVotes"))
    top_per_region = region_movies.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 3)
    
    top_per_region.select("region", "primaryTitle", "averageRating", "rank") \
        .limit(300) \
        .toPandas().to_csv("results/y36_top_3_movies_per_region.csv", index=False)

def run_yaremchuk_questions():
    y32_top_5_movies_by_aka_count_per_year()
    y33_ua_localization_impact()
    y34_original_title_matches_localized()
    y35_doc_localization_languages()
    y36_top_3_movies_per_region()
