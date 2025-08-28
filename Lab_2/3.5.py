from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_list, explode
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations
from pyspark.sql.functions import udf

# Khởi tạo Spark session
spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()

# Đọc dữ liệu từ file TSV vào Spark DataFrame
df = spark.read.csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movies.tsv", header=False, sep="\t", inferSchema=True)

# Đổi tên các cột
df = df.withColumnRenamed("_c0", "actor").withColumnRenamed("_c1", "title").withColumnRenamed("_c2", "year")

# Định nghĩa UDF để tạo cặp diễn viên từ danh sách diễn viên
def create_actor_pairs(actors):
    return [tuple(sorted(pair)) for pair in combinations(actors, 2)]

# Đăng ký UDF với Spark
create_actor_pairs_udf = udf(create_actor_pairs, ArrayType(ArrayType(StringType())))

# Tạo cặp diễn viên cho mỗi bộ phim
pairs_df = df.groupBy("year", "title") \
             .agg(collect_list("actor").alias("actors")) \
             .withColumn("actor_pairs", create_actor_pairs_udf("actors")) \
             .select("year", "title", explode("actor_pairs").alias("actor_pair"))
pair_counts_df = pairs_df.groupBy("actor_pair") \
                         .agg(count("*").alias("count"), collect_list("title").alias("movies")) \
                         .orderBy(col("count").desc())
pair_counts_df.show()
spark.stop()
