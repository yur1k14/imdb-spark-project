from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def main():
    # 1. Створення сесії Spark
    spark = SparkSession.builder \
        .appName("IMDB-Spark-Project") \
        .master("local[*]") \
        .getOrCreate()

    # Встановлюємо рівень логування на WARN, щоб бачити лише важливі повідомлення
    spark.sparkContext.setLogLevel("WARN")

    print("--- Тестовий DataFrame для перевірки налаштувань ---")

    # 2. Створення тестових даних
    # Використовуємо дані, схожі на структуру IMDB для підготовки до наступного етапу
    test_data = [
        ("tt0111161", "The Shawshank Redemption", 1994, 9.3),
        ("tt0068646", "The Godfather", 1972, 9.2),
        ("tt0108052", "Schindler's List", 1993, 9.0)
    ]

    # 3. Визначення простої схеми
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("startYear", IntegerType(), True),
        StructField("averageRating", DoubleType(), True)
    ])

    # 4. Створення DataFrame
    df = spark.createDataFrame(data=test_data, schema=schema)

    # 5. Відображення даних
    df.show()

    # Зупинка сесії
    spark.stop()

if __name__ == "__main__":
    main()