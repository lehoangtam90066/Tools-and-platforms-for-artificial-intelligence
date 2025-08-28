from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, max
from pyspark.sql.window import Window

# Khởi tạo Spark session
spark = SparkSession.builder.appName('MoviesAnalysis').getOrCreate()

# Đọc dữ liệu từ file TSV vào Spark DataFrame
movies_df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movies.tsv", header=False).toDF('actor', 'movie_title', 'year')
ratings_df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movie-ratings.tsv", header=False).toDF('rating', 'movie_title', 'year')

# Kết hợp dữ liệu từ hai DataFrame dựa trên tên phim và năm
merged_df = movies_df.join(ratings_df, on=['movie_title', 'year'], how='inner')

# Sử dụng Window để tìm rating cao nhất cho mỗi năm
windowSpec = Window.partitionBy('year')

# Tạo cột max_rating cho từng năm và lọc ra các dòng có rating cao nhất
max_ratings_df = merged_df.withColumn('max_rating', max('rating').over(windowSpec)) \
                          .filter(col('rating') == col('max_rating'))
result_df = max_ratings_df.groupBy('year', 'movie_title', 'rating') \
                          .agg(collect_list('actor').alias('actors'))
result_df = result_df.orderBy(col('year').desc(), col('rating').desc())
result_df = result_df.withColumn('actors', 
                                 col('actors').cast('string')) 
result_df.show(truncate=False, vertical=True)  
spark.stop()