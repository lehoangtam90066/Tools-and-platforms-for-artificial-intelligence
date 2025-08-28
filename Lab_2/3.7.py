from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Đọc dữ liệu từ file TSV
df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movie-rating.tsv", header=False)

# Đổi tên các cột
df = df.withColumnRenamed("_c0", "rating").withColumnRenamed("_c1", "movie").withColumnRenamed("_c2", "year")

# Lọc các dòng có rating trong khoảng [4, 5]
filtered_df = df.filter((df['rating'] >= 4) & (df['rating'] <= 5))
years_df = filtered_df.select("year").distinct()
sorted_years_df = years_df.orderBy("year")
sorted_years_df.show()
spark.stop()
