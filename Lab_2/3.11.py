from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Đọc dữ liệu từ file movies (giả sử đã có file này)
movies_df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movies.tsv", header=False)
movies_df = movies_df.withColumnRenamed("_c0", "actor").withColumnRenamed("_c1", "movie").withColumnRenamed("_c2", "year")

# Lọc các movie mà Kurt Carley không tham gia sau năm 2000
filtered_movies = movies_df.filter((movies_df["actor"] != "Carley, Kurt") & (movies_df["year"] > 2000))

# Hiển thị kết quả
filtered_movies.show()

# Dừng SparkSession
spark.stop()
