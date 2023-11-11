from deltalake import DeltaTable, write_deltalake, Schema
from datetime import datetime
import polars as pl


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