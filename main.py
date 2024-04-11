from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.master("local").appName("ProductCategory").getOrCreate()

data = [("продукт1", "категория1"),
        ("продукт1", "категория2"),
        ("продукт2", "категория1"),
        ("продукт3", None),
        ("продукт7", None)]

prod_df = spark.createDataFrame(data, ["Название_Продукта", "Название_категории"])
cat_df = prod_df.select("Название_категории").distinct().filter(col("Название_категории").isNotNull())

pairs_df = prod_df.join(cat_df, on="Название_Продукта", how="Не_Пон").select("Название_Продукта", "Название_категории")

no_category_df = prod_df.filter(col("Название_категории").isNull()).select("Название_Продукта")

result_df = pairs_df.union(no_category_df)

result_df.show()

# Останавливаем SparkSession
spark.stop()
