from pyspark.shell import spark


def create_df(path):
    new_df = spark.read.option("header", "true") \
        .option("sep", "\t") \
        .option("multiline", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .csv(path)
    return new_df


def write_in_csv_file(df, path):
    df.write \
        .format('csv') \
        .option('header', True) \
        .mode('overwrite') \
        .option('sep', ' | ') \
        .save(path)


def write_in_single_csv_file(df, path):
    df.repartition(1).write \
        .format('csv') \
        .option('header', True) \
        .mode('append') \
        .option('sep', ' | ') \
        .save(path)
