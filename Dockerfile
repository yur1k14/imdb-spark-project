# Використовуємо офіційний стабільний образ Python
FROM python:3.11-slim-bullseye

# Встановлюємо OpenJDK 17 безпосередньо через менеджер пакетів Debian
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Встановлюємо PySpark тієї версії, яку ви тестували локально
RUN pip install --no-cache-dir pyspark==4.1.1

# Встановлюємо робочу директорію
WORKDIR /app

# Копіюємо вміст вашого проекту в контейнер
COPY . .

# Команда для запуску вашого скрипта
CMD ["python", "main.py"]