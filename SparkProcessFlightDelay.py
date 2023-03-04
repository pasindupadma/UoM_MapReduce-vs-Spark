import time
from pyspark.sql.functions import sum


df = spark.read.csv("s3://239322-uom/hive/DelayedFlights-updated.csv", header=True, inferSchema=True)
df.count()

df.createOrReplaceTempView("AirDelay")

T_start = time.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").agg(
    {"CarrierDelay": "sum", "NASDelay": "sum", "WeatherDelay": "sum","LateAircraftDelay": "sum", "SecurityDelay": "sum" }).show()
T_end = time.time()
T_difference = T_end - T_start
print(f"Elapsed time: {T_difference:.2f} seconds")

T_start = time.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").sum("CarrierDelay").show()
T_end = time.time()
T_difference = T_end - T_start
print(f"Elapsed time: {T_difference:.2f} seconds")

T_start = time.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").sum("NASDelay").show()
process_df.show()
T_end = time.time()
T_difference = T_end - T_start
print(f"Elapsed time: {T_difference:.2f} seconds")

T_start = time.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").sum("WeatherDelay").show()
T_end = time.time()
T_difference = T_end - T_start
print(f"Elapsed time: {T_difference:.2f} seconds")

T_start = time.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").sum("LateAircraftDelay").show()
T_end = time.time()
T_difference = T_end - T_start
print(f"Elapsed time: {T_difference:.2f} seconds")

T_start = time.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").sum("SecurityDelay").show()
T_end = time.time()
T_difference = T_end - T_start
print(f"Elapsed time: {T_difference:.2f} seconds")


