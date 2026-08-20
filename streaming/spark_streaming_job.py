from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, from_json, try_to_timestamp, lit, row_number, when,
    to_json, struct, current_timestamp, unix_timestamp, window
)
from datetime import datetime, timezone

spark = SparkSession.builder \
    .appName("TransactionStreamValidation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP = "broker:19092"
TOPIC = "transactions"
VALID_TOPIC = "transactions_valid"
INVALID_TOPIC = "transactions_dlq"

# Skema payload transaksi sesuai kontrak JSON producer
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
])

# Baca stream mentah dari topic Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse bytes JSON jadi kolom-kolom sesuai skema
parsed_df = raw_df.select(
    from_json(col("value").cast(StringType()), transaction_schema).alias("data")
).select("data.*")

# Konversi timestamp string ke TimestampType; gagal parse -> null (tidak crash job)
stream_df = parsed_df.withColumn(
    "event_time",
    try_to_timestamp(col("timestamp"), lit("yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

# Watermark untuk operasi stateful (dipakai tumbling window di bawah)
watermarked_df = stream_df.withWatermark("event_time", "3 minutes")

# Akumulator total transaksi valid sepanjang umur job (reset kalau job restart)
running_total = 0

def process_batch(batch_df, batch_id):
    global running_total
    print(f"=== Memproses batch_id: {batch_id} ===")

    # Validasi 1: field wajib tidak boleh null
    batch_df = batch_df.withColumn(
        "err_missing_field",
        when(
            col("user_id").isNull() | col("amount").isNull() | col("timestamp").isNull(),
            True
        ).otherwise(False)
    )

    # Validasi 2: timestamp gagal di-parse (event_time null)
    batch_df = batch_df.withColumn(
        "err_incorrect_timestamp",
        when(col("event_time").isNull(), True).otherwise(False)
    )

    # Validasi 3: amount di luar rentang 1 - 10.000.000
    batch_df = batch_df.withColumn(
        "err_out_of_range",
        when((col("amount") < 1) | (col("amount") > 10000000), True).otherwise(False)
    )

    # Validasi 4: source di luar mobile/web/pos
    batch_df = batch_df.withColumn(
        "err_unknown_source",
        when(~col("source").isin("mobile", "web", "pos"), True).otherwise(False)
    )

    # Validasi 5: duplicate dalam batch yang sama (user_id + timestamp identik)
    window_spec = Window.partitionBy("user_id", "timestamp").orderBy("transaction_id")
    batch_df = batch_df.withColumn("row_num", row_number().over(window_spec))
    batch_df = batch_df.withColumn("err_duplicate", when(col("row_num") > 1, True).otherwise(False))

    # Validasi 6: event lebih dari 3 menit lebih lambat dari waktu sekarang
    batch_df = batch_df.withColumn(
        "err_late",
        when(
            col("event_time").isNotNull() &
            (unix_timestamp(current_timestamp()) - unix_timestamp(col("event_time")) > 180),
            True
        ).otherwise(False)
    )

    # Gabungkan semua flag error jadi is_valid + satu error_reason (prioritas berurutan)
    final_df = batch_df.withColumn(
        "is_valid",
        when(
            (col("err_missing_field") == False) &
            (col("err_incorrect_timestamp") == False) &
            (col("err_out_of_range") == False) &
            (col("err_unknown_source") == False) &
            (col("err_duplicate") == False) &
            (col("err_late") == False),
            True
        ).otherwise(False)
    ).withColumn(
        "error_reason",
        when(col("err_missing_field") == True, "Missing Field")
        .when(col("err_incorrect_timestamp") == True, "Incorrect Timestamp")
        .when(col("err_out_of_range") == True, "Invalid Amount")
        .when(col("err_unknown_source") == True, "Invalid Source")
        .when(col("err_duplicate") == True, "Duplicate Row")
        .when(col("err_late") == True, "late_event")
        .otherwise("Valid Row")
    ).select("transaction_id", "user_id", "amount", "event_time", "source", "is_valid", "error_reason")

    valid_df = final_df.filter(col("is_valid") == True)
    invalid_df = final_df.filter(col("is_valid") == False)

    # Update running total dan cetak ke console sesuai format yang diminta
    valid_count = valid_df.count()
    running_total += valid_count
    output_row = spark.createDataFrame(
        [Row(timestamp=datetime.now(timezone.utc), running_total=running_total)]
    )
    output_row.show(truncate=False)

    # Routing: valid -> transactions_valid, invalid -> transactions_dlq
    valid_df.select(to_json(struct("*")).alias("value")).write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", VALID_TOPIC) \
        .save()

    invalid_df.select(to_json(struct("*")).alias("value")).write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", INVALID_TOPIC) \
        .save()

# Monitoring: total transaksi per window 1 menit
windowed_df = watermarked_df.filter(col("event_time").isNotNull()) \
    .groupBy(window(col("event_time"), "1 minute")) \
    .count()

window_query = windowed_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/checkpoint/window") \
    .start()

query = watermarked_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/checkpoint/validation") \
    .start()

# Tunggu kedua query, bukan cuma salah satu
spark.streams.awaitAnyTermination()