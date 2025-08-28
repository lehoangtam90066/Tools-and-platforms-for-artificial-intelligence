from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, explode

# Khởi tạo Spark session
spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()

# Đọc dữ liệu từ file TSV vào Spark DataFrame
df = spark.read.csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movies.tsv", header=False, sep="\t", inferSchema=True)

# Đổi tên các cột
df = df.withColumnRenamed("_c0", "actor").withColumnRenamed("_c1", "title").withColumnRenamed("_c2", "year")

# Nhóm theo năm và phim, thu thập danh sách diễn viên
actors_list = df.groupBy("year", "title").agg(collect_list("actor").alias("actors"))

# Làm phẳng danh sách diễn viên và chỉ hiển thị cột diễn viên
actors_list_exploded = actors_list.select(explode("actors").alias("actor"))

# Hiển thị danh sách diễn viên theo cột dọc
actors_list_exploded.show(truncate=False, vertical=True)

# Dừng Spark session
spark.stop()
