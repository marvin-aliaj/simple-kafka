"""Order schema shared by the producer and tracker."""
import json
from uuid import UUID


def validate_order(order):
    if not isinstance(order, dict):
        raise ValueError("Order must be a JSON object")
    for field in ("order_id", "user", "item"):
        if not isinstance(order.get(field), str) or not order[field].strip():
            raise ValueError(f"{field} must be a non-empty string")
    try:
        UUID(order["order_id"])
    except ValueError as exc:
        raise ValueError("order_id must be a UUID") from exc
    if type(order.get("quantity")) is not int or order["quantity"] <= 0:
        raise ValueError("quantity must be a positive integer")
    return order


def decode_order(value):
    if value is None:
        raise ValueError("Tombstones are not valid orders")
    return validate_order(json.loads(value.decode("utf-8")))
