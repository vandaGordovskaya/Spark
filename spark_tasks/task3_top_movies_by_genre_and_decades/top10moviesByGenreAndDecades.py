"""
    Job finds top-10 movies in each genre by decades from 1950 and writes results in single file.
"""

from spark_tasks import task3_top_movies_by_genre_and_decades as task3

from spark_tasks.manageDFs import write_in_single_csv_file
from spark_tasks.task1_top_movies.top100movies import get_movies_with_rating

moviesWithRatingAndGenres = get_movies_with_rating()

moviesWithRatingAndGenres = moviesWithRatingAndGenres.filter((moviesWithRatingAndGenres.numVotes >= 100000)
                                                             & (moviesWithRatingAndGenres.startYear >= 1950))

all_sets_of_genres_in_map = moviesWithRatingAndGenres.select("genres").rdd.map(lambda r: r[0]).collect()
list_genres = ",".join(all_sets_of_genres_in_map)
list_genres = list(set(list_genres.split(',')))
genresDF = task3.sqlContext.createDataFrame([(l,) for l in list_genres], ['genre'])

topMoviesInGenre = moviesWithRatingAndGenres \
    .join(genresDF, moviesWithRatingAndGenres.genres.contains(genresDF.genre)) \
    .drop(moviesWithRatingAndGenres.genres)

topMoviesInGenreAddDecades = topMoviesInGenre \
    .withColumn("decades", task3
                .when(topMoviesInGenre.startYear.between(1950, 1959), "1950 - 1960")
                .when(topMoviesInGenre.startYear.between(1960, 1969), "1960 - 1970")
                .when(topMoviesInGenre.startYear.between(1970, 1979), "1970 - 1980")
                .when(topMoviesInGenre.startYear.between(1980, 1989), "1980 - 1990")
                .when(topMoviesInGenre.startYear.between(1990, 1999), "1990 - 2000")
                .when(topMoviesInGenre.startYear.between(2000, 2009), "2000 - 2010")
                .when(topMoviesInGenre.startYear.between(2010, 2019), "2010 - 2020")
                .when(topMoviesInGenre.startYear.between(2020, 2029), "2020 - 2030")
                .otherwise(topMoviesInGenre.startYear))

w = task3.Window.partitionBy("genre", "decades").orderBy(task3.col("averageRating").desc())

topMoviesInGenreByDecades = topMoviesInGenreAddDecades\
    .withColumn("hot_top_in_genre", task3.row_number().over(w)) \
    .filter(task3.col("hot_top_in_genre") <= 10).orderBy(task3.col("decades").desc())\
    .drop("hot_top_in_genre")

write_in_single_csv_file(topMoviesInGenreByDecades, "..\\..\\Output\\top10moviesGenresAndDecades")
