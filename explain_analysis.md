# Аналіз планів виконання (Execution Plans Analysis)

У цьому документі наведено аналіз фізичних планів виконання (Physical Plans) для всіх шести реалізованих бізнес-запитів PySpark. Аналіз базується на виводі методу `.explain(True)`.

## Q1: Топ 3 найкращих фільмів для кожного жанру
**Ключові вузли плану:**
- **FileScan CSV & Filter**: Читання даних з фільтрацією за `titleType = 'movie'` та `numVotes >= 10000` безпосередньо на етапі сканування (Pushdown predicates).
- **BroadcastHashJoin / SortMergeJoin**: З'єднання таблиць `title.basics` та `title.ratings`. Залежно від розміру відфільтрованих даних, Spark використовує `BroadcastHashJoin` для швидшого об'єднання.
- **Generate (explode)**: Розгортання масиву `genres` у декілька рядків.
- **Window & WindowGroupLimit**: Використання віконної функції `row_number() over (partition by genre order by averageRating desc)`. На фізичному рівні Spark застосовує сортування (`Sort`) та `WindowGroupLimit` для оптимізованого вибору топ-3 фільмів без повного сортування всього датасету.

## Q2: Топ 10 режисерів з найвищим середнім рейтингом
**Ключові вузли плану:**
- **BroadcastHashJoin**: Використовується для з'єднання епізодів з їхніми рейтингами (`title.crew` -> `title.basics` -> `title.ratings`).
- **HashAggregate**: Spark застосовує двохетапну агрегацію (Partial Aggregate -> Exchange -> Final Aggregate) для підрахунку `avg(averageRating)` та `count(tconst)` по кожному `director`.
- **SortMergeJoin**: Об'єднання з `name.basics` для отримання імен режисерів виконується через `SortMergeJoin`, оскільки після агрегації дані стають більшими для широкомовної передачі.
- **TakeOrderedAndProject**: Оптимізована операція сортування (`ORDER BY avg_director_rating DESC`) та взяття `LIMIT 10` (GlobalLimit).

## Q3 (Group 3: Q16): Топ 5 найрейтинговіших комедій для кожного десятиліття
**Ключові вузли плану:**
- **FileScan CSV & DataFilters**: Ефективна фільтрація `genres` за допомогою `array_contains` (для "Comedy") на етапі читання.
- **Project**: Обчислення колонки `decade` (`startYear - startYear % 10`) безпосередньо після сканування файлу.
- **BroadcastHashJoin**: Оскільки відфільтрований набір комедій відносно невеликий, Spark використовує `BroadcastHashJoin` для з'єднання з `title.ratings`.
- **WindowGroupLimit**: Віконна операція `row_number() over (partition by decade)` реалізована як `WindowGroupLimit`, що суттєво зменшує обсяг даних на етапі шафлінгу (Exchange).

## Q4: Топ 5 серіалів з найбільшою різницею в рейтингу епізодів
**Ключові вузли плану:**
- **BroadcastHashJoin**: З'єднання даних про епізоди (`title.episode`) з рейтингами.
- **HashAggregate**: Одночасне обчислення `max()`, `min()` та `count()` з групуванням по `parentTconst`. Spark виконує часткову агрегацію перед відправкою (Shuffle), щоб зменшити трафік.
- **SortMergeJoin**: Результат агрегації з'єднується з `title.basics` (щоб отримати назви серіалів) через `SortMergeJoin`, з попереднім сортуванням по `parentTconst`.
- **TakeOrderedAndProject**: Швидке сортування за обчисленим полем `rating_diff DESC` і вибір топ-5.

## Q5: Топ 5 найтриваліших фільмів у кожному жанрі (рейтинг > 7.0)
**Ключові вузли плану:**
- **Filter**: Умови `runtimeMinutes isNotNull` та `averageRating > 7.0` зменшують обсяг даних перед Join-ом.
- **BroadcastHashJoin / SortMergeJoin**: Об'єднання `title.basics` та `title.ratings`.
- **Generate (explode)**: Розгортання жанрів для отримання всіх комбінацій.
- **Window**: Використовується `WindowGroupLimit` та `Sort` для обчислення `row_number()` в межах кожного вікна `genre` з сортуванням по спаданню тривалості.

## Q6: Регіони з найбільшою кількістю локалізованих назв для топ 100 фільмів
**Ключові вузли плану:**
- **TakeOrderedAndProject (Top 100)**: Спочатку Spark знаходить 100 найпопулярніших фільмів (сорт по `numVotes DESC`) за допомогою `LocalLimit` -> `Exchange` -> `GlobalLimit`. Це сильно обмежує дані (всього 100 рядків).
- **BroadcastHashJoin**: Ці 100 рядків широкомовно передаються (broadcasted) до екзек'юторів, що сканують `title.akas` (локалізовані назви), що робить Join надзвичайно швидким.
- **HashAggregate**: Швидке групування за `region` та підрахунок `count(tconst)`.
- **TakeOrderedAndProject**: Фінальне сортування за підрахованою кількістю і вибір топ-15 регіонів.

### Загальний висновок
Spark Catalyst Optimizer дуже ефективно використовує **BroadcastHashJoin** (коли одна таблиця може поміститися в пам'яті), просуває (pushdown) фільтри до джерела даних (FileScan DataFilters) і оптимізує `orderBy().limit()` та віконні функції з ранжируванням (`row_number() <= N`) за допомогою **WindowGroupLimit** та **TakeOrderedAndProject**.
