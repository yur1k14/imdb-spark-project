import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col, explode, row_number, count, avg, max as spark_max, min as spark_min, abs as spark_abs, array_contains
from pyspark.sql.window import Window

from extract import (
    title_basics_df,
    title_ratings_df,
    title_crew_df,
    name_basics_df,
    title_episode_df,
    title_akas_df
)

from vorobets_questions import run_vorobets_questions
from lirko_questions import run_lirko_questions
from poliuga_questions import run_poliuga_questions
from sydor_questions import run_sydor_questions
from tymofeiuk_questions import run_tymofeiuk_questions
from yaremchuk_questions import run_yaremchuk_questions

# Ensure output directories exist
os.makedirs("results", exist_ok=True)
os.makedirs("visualizations", exist_ok=True)


def q1_top_3_movies_per_genre():
    print("Executing Q1: Топ 3 найкращих фільмів для кожного жанру")
    # Filter for movies, min 100000 votes, explode genres
    movies = title_basics_df.filter(
        (col("titleType") == "movie") & col("genres").isNotNull()
    ).join(
        title_ratings_df.filter(col("numVotes") >= 100000), "tconst"
    )
    
    movies_exploded = movies.withColumn("genre", explode(col("genres")))
    
    window_spec = Window.partitionBy("genre").orderBy(col("averageRating").desc(), col("numVotes").desc())
    
    top_per_genre = movies_exploded.withColumn(
        "rank", row_number().over(window_spec)
    ).filter(col("rank") <= 3)
    
    with open("explain_plans.txt", "a") as f:
        f.write("\n=== Q1 Plan ===\n")
        f.write(top_per_genre._jdf.queryExecution().toString())
        f.write("\n")
    
    df = top_per_genre.select("genre", "primaryTitle", "averageRating", "numVotes", "rank").toPandas()
    df.to_csv("results/q1_top_3_movies_per_genre.csv", index=False)
    
    # Visualization: FacetGrid with horizontal bar charts per genre
    diverse_genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Sci-Fi']
    df_plot = df[df['genre'].isin(diverse_genres)].copy()
    df_plot = df_plot.sort_values(by=['genre', 'averageRating'], ascending=[True, False])

    g = sns.catplot(
        data=df_plot, kind="bar",
        x="averageRating", y="primaryTitle", col="genre", col_wrap=3,
        sharey=False, height=3.5, aspect=1.5, hue="genre", palette="Set2", legend=False
    )
    
    min_rating = df_plot['averageRating'].min() - 0.2
    for ax in g.axes.flat:
        ax.set_xlim(max(0, min_rating), 10)
        ax.set_ylabel("")
        ax.set_xlabel("Середній рейтинг")
        
    g.fig.suptitle("Топ 3 фільми за рейтингом (мін. 100к голосів) для популярних жанрів", y=1.02, fontsize=16)
    g.savefig("visualizations/q1_top_3_movies_per_genre.png", bbox_inches='tight')
    plt.close()


def q2_top_10_directors_by_rating():
    print("Executing Q2: Топ 10 режисерів з найвищим середнім рейтингом")
    movies_ratings = title_basics_df.filter(col("titleType") == "movie").join(title_ratings_df, "tconst")
    
    crew_exploded = title_crew_df.withColumn("director", explode(col("directors")))
    
    directors_movies = crew_exploded.join(movies_ratings, "tconst")
    
    director_stats = directors_movies.groupBy("director").agg(
        avg("averageRating").alias("avg_director_rating"),
        count("tconst").alias("movie_count")
    ).filter(col("movie_count") >= 5)
    
    top_directors = director_stats.join(
        name_basics_df.withColumnRenamed("nconst", "director"), "director"
    ).orderBy(col("avg_director_rating").desc()).limit(10)
    
    with open("explain_plans.txt", "a") as f:
        f.write("\n=== Q2 Plan ===\n")
        f.write(top_directors._jdf.queryExecution().toString())
        f.write("\n")
    
    df = top_directors.select("primaryName", "avg_director_rating", "movie_count").toPandas()
    df.to_csv("results/q2_top_10_directors.csv", index=False)
    
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=df, y="primaryName", x="avg_director_rating", palette="viridis", hue="primaryName", legend=False)
    plt.title("Топ 10 режисерів за середнім рейтингом (мін. 5 фільмів)")
    plt.xlabel("Середній рейтинг")
    plt.ylabel("Режисер")
    
    # Add data labels
    for p in ax.patches:
        ax.annotate(format(p.get_width(), '.2f'), 
                    (p.get_width(), p.get_y() + p.get_height() / 2.), 
                    ha = 'left', va = 'center', 
                    xytext = (5, 0), 
                    textcoords = 'offset points')
                    
    plt.xlim(0, 10.5) # Kept at 0 as requested, extended slightly to fit labels
    plt.tight_layout()
    plt.savefig("visualizations/q2_top_10_directors.png")
    plt.close()


def q3_top_5_comedy_per_decade():
    print("Executing Q3: Топ 5 найрейтинговіших фільмів жанру Comedy для кожного десятиліття")
    movies = title_basics_df.filter(
        (col("titleType") == "movie") & 
        (col("startYear").isNotNull()) &
        array_contains(col("genres"), "Comedy")
    )
    
    # Calculate decade
    movies_with_decade = movies.withColumn(
        "decade", (col("startYear") - (col("startYear") % 10))
    )
    
    # Join with ratings
    movies_ratings = movies_with_decade.join(
        title_ratings_df.filter(col("numVotes") >= 1000), "tconst"
    )
    
    window_spec = Window.partitionBy("decade").orderBy(col("averageRating").desc(), col("numVotes").desc())
    
    top_comedy = movies_ratings.withColumn(
        "rank", row_number().over(window_spec)
    ).filter(col("rank") <= 5).orderBy("decade", "rank")
    
    with open("explain_plans.txt", "a") as f:
        f.write("\n=== Q3 (Group 3: Q16) Plan ===\n")
        f.write(top_comedy._jdf.queryExecution().toString())
        f.write("\n")
    
    df = top_comedy.select("decade", "primaryTitle", "averageRating", "numVotes", "rank").toPandas()
    df.to_csv("results/q3_top_5_comedy_per_decade.csv", index=False)
    
    df = df.sort_values(by=['decade', 'averageRating'], ascending=[True, False])
    
    # Visualization: FacetGrid with horizontal bar charts per decade
    g = sns.catplot(
        data=df, kind="bar",
        x="averageRating", y="primaryTitle", col="decade", col_wrap=3,
        sharey=False, height=3.5, aspect=1.5, hue="primaryTitle", palette="viridis", legend=False
    )
    
    min_rating = df['averageRating'].min() - 0.5
    for ax in g.axes.flat:
        ax.set_xlim(max(0, min_rating), 10)
        ax.set_ylabel("")
        ax.set_xlabel("Середній рейтинг")
        
    g.fig.suptitle("Топ 5 найрейтинговіших комедій для кожного десятиліття", y=1.02, fontsize=16)
    g.savefig("visualizations/q3_top_5_comedy_per_decade.png", bbox_inches='tight')
    plt.close()


def q4_top_5_series_rating_difference():
    print("Executing Q4: Топ 5 серіалів з найбільшою різницею в рейтингу епізодів")
    episodes_ratings = title_episode_df.join(title_ratings_df, "tconst")
    
    series_stats = episodes_ratings.groupBy("parentTconst").agg(
        spark_max("averageRating").alias("max_rating"),
        spark_min("averageRating").alias("min_rating"),
        count("tconst").alias("episode_count")
    ).filter(col("episode_count") >= 10) # At least 10 episodes to make sense
    
    series_diff = series_stats.withColumn(
        "rating_diff", spark_abs(col("max_rating") - col("min_rating"))
    )
    
    top_series_diff = series_diff.join(
        title_basics_df.withColumnRenamed("tconst", "parentTconst"), "parentTconst"
    ).orderBy(col("rating_diff").desc()).limit(5)
    
    with open("explain_plans.txt", "a") as f:
        f.write("\n=== Q4 Plan ===\n")
        f.write(top_series_diff._jdf.queryExecution().toString())
        f.write("\n")
        
    df = top_series_diff.select("primaryTitle", "max_rating", "min_rating", "rating_diff").toPandas()
    df.to_csv("results/q4_top_5_series_rating_difference.csv", index=False)
    
    # Visualization: Dumbbell plot approximation using scatter and plot
    plt.figure(figsize=(12, 6))
    for i in range(len(df)):
        min_r = df['min_rating'].iloc[i]
        max_r = df['max_rating'].iloc[i]
        diff = df['rating_diff'].iloc[i]
        title = df['primaryTitle'].iloc[i]
        
        plt.plot(
            [min_r, max_r], 
            [title, title], 
            color='grey', linestyle='-', zorder=1
        )
        
        plt.text(min_r - 0.1, title, f"{min_r:.1f}", va='center', ha='right', color='darkred', fontweight='bold')
        plt.text(max_r + 0.1, title, f"{max_r:.1f}", va='center', ha='left', color='darkgreen', fontweight='bold')
        
        mid_point = (min_r + max_r) / 2
        plt.text(mid_point, title, f"Δ={diff:.1f}", va='bottom', ha='center', color='black', 
                 fontsize=10, bbox=dict(facecolor='white', edgecolor='none', alpha=0.7, pad=1))

    sns.scatterplot(data=df, x="min_rating", y="primaryTitle", color="red", label="Найгірший еп.", s=150, zorder=2)
    sns.scatterplot(data=df, x="max_rating", y="primaryTitle", color="green", label="Найкращий еп.", s=150, zorder=2)
    plt.title("Топ 5 серіалів за найбільшою різницею рейтингів епізодів")
    plt.xlabel("Рейтинг")
    plt.ylabel("Серіал")
    # Move legend outside to prevent overlap
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xlim(df['min_rating'].min() - 1, 11)
    plt.tight_layout()
    plt.savefig("visualizations/q4_top_5_series_rating_diff.png")
    plt.close()


def q5_top_5_longest_movies_per_genre():
    print("Executing Q5: Топ 5 найтриваліших фільмів у кожному жанрі (рейтинг > 7.0)")
    movies = title_basics_df.filter(
        (col("titleType") == "movie") & 
        col("runtimeMinutes").isNotNull() & 
        col("genres").isNotNull()
    ).join(
        title_ratings_df.filter((col("averageRating") > 7.0) & (col("numVotes") >= 1000)), "tconst"
    )
    
    movies_exploded = movies.withColumn("genre", explode(col("genres")))
    
    window_spec = Window.partitionBy("genre").orderBy(col("runtimeMinutes").cast("int").desc())
    
    longest_movies = movies_exploded.withColumn(
        "rank", row_number().over(window_spec)
    ).filter(col("rank") <= 5)
    
    # Convert runtime to numeric explicitly for plotting if needed
    longest_movies = longest_movies.withColumn("runtimeMinutes", col("runtimeMinutes").cast("int"))
    
    df = longest_movies.select("genre", "primaryTitle", "runtimeMinutes", "rank").toPandas()
    df.to_csv("results/q5_top_5_longest_movies_per_genre.csv", index=False)
    
    # Visualization: Stripplot with labels for the absolute longest in each genre
    top_10_genres = df['genre'].value_counts().head(10).index
    df_plot = df[df['genre'].isin(top_10_genres)].copy()
    
    plt.figure(figsize=(14, 8))
    sns.stripplot(data=df_plot, x="genre", y="runtimeMinutes", hue="genre", palette="tab10", size=8, jitter=True, legend=False, alpha=0.8)
    
    # Annotate the longest movie per genre
    for genre in top_10_genres:
        genre_data = df_plot[df_plot['genre'] == genre]
        if not genre_data.empty:
            longest = genre_data.loc[genre_data['runtimeMinutes'].idxmax()]
            plt.text(top_10_genres.tolist().index(genre), longest['runtimeMinutes'] + 5, 
                     longest['primaryTitle'], fontsize=8, ha='center', va='bottom', rotation=15)

    plt.title("Тривалість топ-5 найдовших фільмів (рейтинг > 7.0) у популярних жанрах\n(* Підписано назву найдовшого фільму в кожному жанрі)")
    plt.xlabel("Жанр")
    plt.ylabel("Тривалість (хвилини)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("visualizations/q5_top_5_longest_movies_per_genre.png")
    plt.close()


def q6_most_localized_regions_top_movies():
    print("Executing Q6: Регіони з найбільшою кількістю локалізованих назв для топ 100 найпопулярніших фільмів")
    top_100_movies = title_basics_df.filter(col("titleType") == "movie").join(
        title_ratings_df, "tconst"
    ).orderBy(col("numVotes").desc()).limit(100)
    
    localizations = top_100_movies.join(
        title_akas_df.withColumnRenamed("titleId", "tconst"), "tconst"
    ).filter(col("region").isNotNull() & (col("region") != "\\N"))
    
    region_counts = localizations.groupBy("region").agg(
        count("tconst").alias("localization_count")
    ).orderBy(col("localization_count").desc()).limit(15)
    
    df = region_counts.toPandas()
    df.to_csv("results/q6_most_localized_regions.csv", index=False)
    
    plt.figure(figsize=(12, 6))
    ax = sns.barplot(data=df, x="region", y="localization_count", palette="magma", hue="region", legend=False)
    plt.title("Топ 15 регіонів за кількістю локалізацій для 100 найпопулярніших фільмів")
    plt.xlabel("Регіон (код)")
    plt.ylabel("Кількість локалізацій")
    
    # Add data labels
    for p in ax.patches:
        ax.annotate(format(int(p.get_height()), 'd'), 
                    (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha = 'center', va = 'center', 
                    xytext = (0, 9), 
                    textcoords = 'offset points')
                    
    plt.ylim(0, df['localization_count'].max() * 1.1) # Add headroom for labels
    plt.tight_layout()
    plt.savefig("visualizations/q6_most_localized_regions.png")
    plt.close()


if __name__ == "__main__":
    # Clear the file first
    open("explain_plans.txt", "w").close()

    # Original questions with visualizations
    q1_top_3_movies_per_genre()
    q2_top_10_directors_by_rating()
    q3_top_5_comedy_per_decade()
    q4_top_5_series_rating_difference()
    q5_top_5_longest_movies_per_genre()
    q6_most_localized_regions_top_movies()

    run_vorobets_questions()
    run_lirko_questions()
    run_poliuga_questions()
    run_sydor_questions()
    run_tymofeiuk_questions()
    run_yaremchuk_questions()

    print("All tasks completed successfully!")