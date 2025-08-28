from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Đọc dữ liệu từ file movies
movies_df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movies.tsv", header=False)
movies_df = movies_df.withColumnRenamed("_c0", "actor").withColumnRenamed("_c1", "movie").withColumnRenamed("_c2", "year")

# Lọc phim trong khoảng 2005 - 2010
filtered_movies = movies_df.filter((movies_df["year"] >= 2005) & (movies_df["year"] <= 2010))

# Tìm cặp diễn viên cùng tham gia
from pyspark.sql import functions as F

pair_df = filtered_movies.alias('a').join(filtered_movies.alias('b'), 
                                          (F.col('a.movie') == F.col('b.movie')) & 
                                          (F.col('a.actor') < F.col('b.actor'))).select('a.actor', 'b.actor', 'a.movie')

pair_df.show()

# Tính thành công của từng diễn viên dựa trên rating của movie
# Bạn sẽ cần thêm phần mã tính trung bình rating của từng diễn viên ở đây.

# Dừng SparkSession
spark.stop()
