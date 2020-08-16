#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType


def pilot_enrolment_event_schema():
    """
    root
    |-- content-Length: string (nullable = true)
    |-- host: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- accept-encoding: string (nullable = true)
    |-- ship_cost: string (nullable = true)
    |-- allegiance: string (nullable = true)
    |-- accept: string (nullable = true)
    |-- user-agent: string (nullable = true)
    |-- connection: string (nullable = true)
    |-- pilotid: string (nullable = true)
    |-- datestamp: string (nullable = true)
    |-- ship: string (nullable = true)
    |-- content-type: string (nullable = true)
    |-- pilot: string (nullable = true)
    """
    return StructType([
        StructField("Content-Length", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("Accept-Encoding", StringType(), True),
        StructField("ship_cost", StringType(), True),
        StructField("allegiance", StringType(), True),
        StructField("Accept", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("Connection", StringType(), True),
        StructField("pilotID", StringType(), True),
        StructField("datestamp", StringType(), True),
        StructField("ship", StringType(), True),
        StructField("Content-Type", StringType(), True),
        StructField("pilot", StringType(), True)
    ])


@udf('boolean')
def is_pilot_enrol(event_as_json):
    """udf for filtering events
    """
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
        .getOrCreate()

    raw_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "starfighter") \
        .load()

    pilot_enrolment = raw_events \
        .filter(is_pilot_enrol(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          pilot_enrolment_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    sink = pilot_enrolment \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_pilot_enrolments") \
        .option("path", "/tmp/pilot_enrolments") \
        .trigger(processingTime="60 seconds") \
        .start()

    sink.awaitTermination()


if __name__ == "__main__":
    main()
