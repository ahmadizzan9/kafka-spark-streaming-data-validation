from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, from_json, try_to_timestamp, lit, row_number, when, to_json, struct


spark = SparkSession.builder \
    .appName("TransactionStreamValidation") \
    .getOrCreate()

# TODO: set log level ke "WARN" biar console tidak penuh log info Spark
spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP = "broker:19092"
TOPIC = "transactions"
VALID_TOPIC = "transactions_valid"
INVALID_TOPIC = "transactions_dlq"
 
# TODO: lengkapi StructType sesuai field JSON producer-mu
# Field: transaction_id (string), user_id (string), amount (integer),
#        timestamp (string, BUKAN TimestampType di sini — masih mentah dari JSON),
#        source (string)
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
])

# Baca raw stream dari Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_df.select(
    from_json(col("value").cast(StringType()), transaction_schema).alias("data")
).select("data.*")

# TODO: tambahkan kolom event_time bertipe TimestampType, hasil konversi dari kolom "timestamp"
# Hint: to_timestamp(kolom, format_string) — format_string harus PERSIS cocok dengan
#       format producer-mu: "%Y-%m-%dT%H:%M:%SZ" (strftime) → di Spark formatnya beda syntax,
#       cari tahu sendiri format string yang setara di Spark's to_timestamp (bukan Python strftime)
final_df = parsed_df.withColumn("event_time", try_to_timestamp(col("timestamp"), lit("yyyy-MM-dd'T'HH:mm:ss'Z'")))

def process_batch(batch_df, batch_id):
    print(f"=== Memproses batch_id: {batch_id} ===")
    # Validasi 1: mandatory field check
    batch_df = batch_df.withColumn(
        "err_missing_field",
        when(col("user_id").isNull() | col("amount").isNull() | col("timestamp").isNull(), True).otherwise(False)
    )

    # Validasi 2: type validation (event_time null berarti timestamp gagal parse)
    batch_df = batch_df.withColumn(
        "err_bad_type",
        when(col("event_time").isNull(), True).otherwise(False)
    )

    # Validasi 3: range validation
    batch_df = batch_df.withColumn(
        "err_out_of_range",
        when((col("amount") < 1) | (col("amount") > 10000000), True).otherwise(False)
    )

    # Validasi 4: source validation
    batch_df = batch_df.withColumn(
        "err_unknown_source",
        when(~col("source").isin("mobile", "web", "pos"), True).otherwise(False)
    )

    # Validasi 5: duplicate (window function seperti sebelumnya)
    window_spec = Window.partitionBy("user_id", "timestamp").orderBy("transaction_id")
    batch_df = batch_df.withColumn("row_num", row_number().over(window_spec))
    batch_df = batch_df.withColumn("err_duplicate", when(col("row_num") > 1, True).otherwise(False))

    # TODO: gabungkan kelimanya jadi is_valid + error_reason
    # Hint is_valid: is_valid = NOT (err_missing_field OR err_bad_type OR err_out_of_range OR err_unknown_source OR err_duplicate)
    # Hint error_reason: karena kamu putuskan cuma butuh SATU alasan (bukan multiple),
    #                     pakai when/otherwise berurutan — urutan prioritas kamu yang tentukan,
    #                     mana yang dicek duluan kalau kebetulan lebih dari satu true
    final_df = batch_df.withColumn(
        "is_valid", 
        when(
            (col("err_missing_field")==False) &
            (col("err_bad_type")==False) &
            (col("err_out_of_range")==False) & 
            (col("err_unknown_source")==False) &
            (col("err_duplicate")==False), 
            True
        ).otherwise(False)
    ).withColumn(
        "error_reason",
        when(col("err_missing_field")==True, "Missing Field")
        .when(col("err_bad_type")==True, "Incorrect Type")
        .when(col("err_out_of_range")==True, "Invalid Amount")
        .when(col("err_unknown_source")==True, "Invalid Source")
        .when(col("err_duplicate")==True, "Duplicate Row")
        .otherwise("Valid Row")
    ).select("transaction_id", "user_id", "amount", "event_time", "source","is_valid","error_reason")

    final_df = final_df.withColumnRenamed("event_time", "timestamp")

    # TODO: pisahkan jadi dua DataFrame berdasarkan is_valid
    valid_df = final_df.filter(col("is_valid")==True)
    invalid_df = final_df.filter(col("is_valid")==False)

    # TODO: ubah masing-masing jadi kolom "value" berisi JSON string dari seluruh kolom
    # Hint: to_json(struct(*kolom_yang_mau_disertakan)) menghasilkan satu kolom JSON,
    #       lalu .select() itu saja sebagai "value" sebelum ditulis ke Kafka
    valid_kafka_ready = valid_df.select(to_json(struct("*")).alias("value"))
    invalid_kafka_ready = invalid_df.select(to_json(struct("*")).alias("value"))

    # TODO: tulis masing-masing ke topic yang sesuai
    # Hint: pola penulisan batch statis ke Kafka:
    #   df.write.format("kafka").option("kafka.bootstrap.servers", ???).option("topic", ???).save()
    valid_kafka_ready.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", VALID_TOPIC) \
        .save()

    invalid_kafka_ready.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", INVALID_TOPIC) \
        .save()

query = final_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()