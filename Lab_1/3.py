from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Validate SIN Numbers") \
    .getOrCreate()
def validate_sin(sin):
    sin_digits = [int(d) for d in str(sin)]
    if len(sin_digits) != 9:
        return False, "SIN phải có đúng 9 chữ số."
    check_digit = sin_digits[-1]
    sin_digits = sin_digits[:-1]
    s1 = sum(sin_digits[-1::-2])
    s2 = sum(sum(divmod(d * 2, 10)) for d in sin_digits[-2::-2])
    total = s1 + s2 + check_digit
    return total % 10 == 0
sin_numbers = [193456787, 123456782, 987654320]  # Ví dụ
rdd = spark.sparkContext.parallelize(sin_numbers)
results = rdd.map(lambda sin: (sin, validate_sin(sin))).collect()
for sin, is_valid in results:
    print(f"SIN: {sin}, Hợp lệ: {is_valid}")
