#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('boolean')
def is_enrolment(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'USER_data':
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "starfighter") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    enrolment_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_enrolment('raw'))

    extracted_enrolment_events = enrolment_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_enrolment_events.printSchema()
    extracted_enrolment_events.show()

    extracted_enrolment_events.registerTempTable("extracted_enrolment_events")

    spark.sql("""
        create external table enrolment
        stored as parquet
        location '/tmp/enrolments'
        as
        select * from extracted_enrolment_events
    """)


if __name__ == "__main__":
    main()
