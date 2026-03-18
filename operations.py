from pyspark.sql.functions import col, split, explode, avg, count, desc


def get_high_rated_titles(title_basics_df, title_ratings_df, min_votes=10000):
    high_rated = title_basics_df.join(
        title_ratings_df,
        "tconst"
    ).filter(
        (col("averageRating") > 8) & (col("numVotes") >= min_votes) & (col("titleType") == "movie")
    ).orderBy(desc("averageRating"))

    return high_rated


def get_top_genres_by_rating(title_basics_df, title_ratings_df, min_movies=500):
    joined_df = title_basics_df.join(title_ratings_df, "tconst").filter(col("genres").isNotNull())

    exploded_df = joined_df.withColumn("single_genre", explode(split(col("genres"), ",")))

    genre_stats = exploded_df.groupBy("single_genre").agg(
        avg("averageRating").alias("avg_rating"),
        count("tconst").alias("total_movies")
    ).filter(
        col("total_movies") >= min_movies
    ).orderBy(desc("avg_rating"))

    return genre_stats


def get_most_frequent_actors_in_top_movies(title_principals_df, title_ratings_df, name_basics_df, min_rating=8.0):
    good_movies = title_ratings_df.filter(col("averageRating") >= min_rating)

    actors_in_good_movies = title_principals_df.join(
        good_movies, "tconst"
    ).filter(
        col("category").isin("actor", "actress")
    )

    actor_counts = actors_in_good_movies.groupBy("nconst").agg(
        count("tconst").alias("good_movie_count")
    )

    top_actors = actor_counts.join(
        name_basics_df, "nconst"
    ).select(
        "primaryName", "good_movie_count", "birthYear"
    ).orderBy(desc("good_movie_count"))

    return top_actors


def get_runtime_trends_by_decade(title_basics_df):
    movies = title_basics_df.filter(
        (col("titleType") == "movie") &
        col("startYear").isNotNull() &
        col("runtimeMinutes").isNotNull() &
        (col("runtimeMinutes") < 300)
    )

    movies_with_decade = movies.withColumn(
        "decade", (col("startYear") - (col("startYear") % 10))
    )

    trends = movies_with_decade.groupBy("decade").agg(
        avg("runtimeMinutes").alias("avg_runtime_minutes"),
        count("tconst").alias("movies_produced")
    ).orderBy("decade")

    return trends
