from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Đọc dữ liệu từ file movies
movies_df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movies.tsv", header=False)
movies_df = movies_df.withColumnRenamed("_c0", "actor").withColumnRenamed("_c1", "movie").withColumnRenamed("_c2", "year")

# Tính toán diễn viên nổi tiếng nhất cho mỗi movie
# Lọc theo rating và sắp xếp theo rating cao nhất

# Dừng SparkSession
spark.stop()
