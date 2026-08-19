import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from confluent_kafka import KafkaException, Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "transactions"

USER_IDS = [
    "u1327",
    "u3726",
    "u5473",
]

SOURCES = [
    "mobile",
    "web",
    "pos",
]

UNKNOWNS = [
    "tablet",
    "tape",
    "book",
]

LOAD_TYPE = [
    "amount_negative",
    "amount_too_large",
    "bad_timestamp",
    "unknown_source",
    "late",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("producer")

def build_valid_transaction():
    return{
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "amount": random.randint(1, 10000000),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": random.choice(SOURCES),
    }

def broken_transaction(transaction, kind):
    if kind == "amount_negative":
        transaction["amount"] = -random.randint(1, 10000000)

    if kind == "amount_too_large":
        transaction["amount"] = random.randint(11000000,100000000)

    if kind == "bad_timestamp":
        transaction["timestamp"] = ""

    if kind == "unknown_source":
        transaction["source"] = random.choice(UNKNOWNS)

    if kind == "late":
        transaction["timestamp"] = (datetime.now(timezone.utc) - timedelta(minutes=random.randint(4,10))).strftime("%Y-%m-%dT%H:%M:%SZ")

    return transaction

def delivery_report(err, msg):
    if err is not None:
        logger.error("Gagal mengirim pesan ke kafka: %s", err)
        return
    logger.info(
        "Pesan berhasil dikirim ke kafka: key: %s, Partition: %s, Offset: %s",
        msg.key().decode("utf-8"),
        msg.partition(),
        msg.offset()
    )

def wait_while_serving(producer, seconds):
    deadline = time.monotonic()+seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        producer.poll(min(remaining, 0.5))

def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    logger.info(
        "Mengirim ke topic '%s' tiap 1-2 detik (ctrl+C untuk berhenti) ",
        TOPIC,
    )

    plan = (LOAD_TYPE * 3)
    random.shuffle(plan)  

    sent = 0
    try:
        while True:
            INTERVAL_SECONDS = random.uniform(1, 2)
            transaction = build_valid_transaction()

            if plan and random.random() < 0.3:
                kind = plan.pop()
                transaction = broken_transaction(transaction, kind)
                logger.info("Mengirim BROKEN event (%s) | sisa plan: %d", kind, len(plan))

            payload = json.dumps(transaction,ensure_ascii=False)
            
            try:
                producer.produce(
                    TOPIC,
                    key=transaction["user_id"].encode("utf-8"),
                    value=payload.encode("utf-8"),
                    callback=delivery_report,
                )
                sent += 1
            except BufferError:
                logger.warning("Queue penuh, menunggu flush")
                producer.flush(INTERVAL_SECONDS)
            except KafkaException:
                logger.exception("kafka menolak order %s", transaction["transaction_id"])

            wait_while_serving(producer, INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Kafka dihentikan oleh user")
    finally:
        remaining = producer.flush(10)
        if remaining:
            logger.error("%s pesan belum terkirim saat keluar", remaining)
        logger.info("Total %s pesan dikirim ke '%s'", sent, TOPIC)

if __name__ == "__main__":
    main()