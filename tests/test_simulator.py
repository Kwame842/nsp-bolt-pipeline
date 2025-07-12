from simulator.simulate import normalize_record

def test_normalize_record():
    row = {
        "trip_id": "xyz001",
        "pickup_datetime": "11/07/2024 14:00",
        "dropoff_datetime": "11/07/2024 14:30",
        "fare_amount": "29.99",
        "vendor_id": "1"
    }
    record = normalize_record(row, 'end')
    assert record['pickup_datetime'] == '2024-07-11 14:00:00'
    assert isinstance(record['fare_amount'], float)
    assert isinstance(record['vendor_id'], int)