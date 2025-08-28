from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MovieCount").getOrCreate()

# Đọc dữ liệu từ movies.tsv
movies = spark.read.csv("movies.tsv", sep="\t", header=True)

# Đọc dữ liệu từ movie-rating.tsv
ratings = spark.read.csv("movie-rating.tsv", sep="\t", header=True)

# Kết hợp dữ liệu từ hai tập tin
merged_data = movies.join(ratings, "movie_id")

# Tính số lượng phim theo từng năm
result = merged_data.groupBy("year").agg(count("movie_id").alias("count"))

# Sắp xếp theo count giảm dần
result = result.orderBy("count", ascending=False)

# In kết quả
result.show()

# Lưu kết quả vào tập tin (tùy chọn)
# result.write.csv("output.csv", header=True)

# Dừng SparkSession
spark.stop()