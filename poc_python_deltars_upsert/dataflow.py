import json

from deltalake import DeltaTable, Schema
from datetime import datetime
import polars as pl
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio

from poc_python_deltars_upsert.sales_data import sales_data


def dummy_dataflow() -> None:
    df = pl.DataFrame(
        {
            "sales_order_id": ["1000", "1001", "1002", "1003"],
            "product": ["bike", "scooter", "car", "motorcycle"],
            "order_date": [
                datetime(2023, 1, 1),
                datetime(2023, 1, 5),
                datetime(2023, 1, 10),
                datetime(2023, 2, 1),
            ],
            "sales_price": [120.25, 2400, 32000, 9000],
            "paid_by_customer": [True, False, False, True],
        }
    )
    print(df)

    df.write_delta("data/sales_orders", mode="append")

    new_data = pl.DataFrame(
        {
            "sales_order_id": ["1002", "1004"],
            "product": ["car", "car"],
            "order_date": [datetime(2023, 1, 10), datetime(2023, 2, 5)],
            "sales_price": [30000.0, 40000.0],
            "paid_by_customer": [True, True],
        }
    )

    dt = DeltaTable("data/sales_orders")
    source = new_data.to_arrow()
    delta_schema = Schema.from_pyarrow(source.schema).to_pyarrow()
    source = source.cast(delta_schema)

    (
        dt.merge(
            source=source,
            predicate="s.sales_order_id = t.sales_order_id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    print(pl.read_delta("data/sales_orders"))

def ingest_json_kafka_dataflow(kafka_server: str, topic: str) -> None:
    async def send():
        producer = AIOKafkaProducer(bootstrap_servers=kafka_server)
        await producer.start()
        try:
            
            for sale_data in sales_data:
                await producer.send_and_wait(topic, json.dumps(sale_data, default=serialize_datetime).encode("utf-8"))
            
        finally:
            await producer.stop()

    asyncio.run(send())

def read_kafka_sink_delta(kafka_server: str, topic: str, consumer_group: str):
    dt = DeltaTable("data/sales_orders")
   
    async def consume():
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
            group_id=consumer_group,
            auto_offset_reset='earliest',
        )
        
        await consumer.start()
        try:
            async for msg in consumer:
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                    msg.key, msg.value, msg.timestamp)
                data = [json.loads(msg.value.decode("utf-8"), object_hook=datetime_parser)]
                new_data = pl.from_dicts(data)
                print(new_data)
                source = new_data.to_arrow()
                delta_schema = Schema.from_pyarrow(source.schema).to_pyarrow()
                source = source.cast(delta_schema)
                
                (
                    dt.merge(
                        source=source,
                        predicate="s.sales_order_id = t.sales_order_id",
                        source_alias="s",
                        target_alias="t",
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute()
                )

        finally:
            await consumer.stop()

    asyncio.run(consume())

def serialize_datetime(obj): 
    if isinstance(obj, datetime): 
        return obj.isoformat() 
    raise TypeError("Type not serializable") 

def datetime_parser(json_dict):
    for (key, value) in json_dict.items():
        try:
            if isinstance(value, str) : 
                json_dict[key] = datetime.fromisoformat(value)
        except:
            pass
    return json_dict

datetime.fromisoformat