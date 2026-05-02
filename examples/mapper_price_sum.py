def map_row(row):
    try:
        price = float(row.get("price", 0) or 0)
    except (TypeError, ValueError):
        price = 0.0
    return [("__all__", price)]
