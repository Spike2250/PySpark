"""До этого никогда не работал и не сталкивался с
   pyspark.sql.DataFrame
    С формулировкой задания вроде разобрался,
    на сколько понимаю у нас есть 3 DataFrame:
      - продукты
      - категории
      - их связь друг с другом
    (попытался представить их в соответвующих функииях ниже:
     get_produts_df, get_categories_df, get_contacts_df)

    И вроде бы идея понятна, а как это сделать с DataFrame не понимаю. :(
"""


from pyspark.sql import SparkSession


def get_spark():
    return SparkSession \
        .builder \
        .master("local[1]") \
        .appName("test") \
        .getOrCreate()


def get_produts_df(spark):
    data = [
        (1, 'banana'),
        (2, 'milk'),
        (3, 'toilet paper'),
        (4, 'doll'),
        (5, 'beer')
    ]
    columns = ["id", "product"]
    return spark.createDataFrame(data=data, schema=columns)


def get_categories_df(spark):
    data = [
        (1, 'food'),
        (2, 'drink'),
        (3, 'toy'),
        (4, 'alcohol'),
        (5, 'other')
    ]
    columns = ["id", "category"]
    return spark.createDataFrame(data=data, schema=columns)


def get_contacts_df(spark):
    data = [
        (1, 1, 1),
        (2, 1, 2),
        (3, 2, 2),
        (4, 2, 5),
        (5, 3, 4),
        (6, 4, 5)
    ]
    columns = ["id", "category_id", "product_id"]
    return spark.createDataFrame(data=data, schema=columns)


def func():
    spark = get_spark()
    produts_df = get_produts_df(spark)
    categories_df = get_categories_df(spark)
    contacts_df = get_contacts_df(spark)

    """здесь должен быть return DataFrame получившегося в результате"""

if __name__ == '__main__':
    func()
