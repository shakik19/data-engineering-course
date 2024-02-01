import re
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data.columns = (data.columns.str.replace(' ', '_').str.lower().str.replace(r'id$', '_id'))

    # print(data['vendor_id'].unique())

    return data


@test
def test_output(output, *args) -> None:
    
    assert output['passenger_count'].isin([0]).sum() == 0, 'Assertion 1 failed: The output contains passenger_count row/rows with value 0'
    assert output['trip_distance'].isin([0]).sum() == 0, 'Assertion 2 failed: The output contains trip_distance row/rows with value 0'
    assert all(output['vendor_id'].isin(output['vendor_id'].unique())), "Assertion 3 failed: vendor_id should be one of the existing values in the column."


