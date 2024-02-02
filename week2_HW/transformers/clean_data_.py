import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Rows with zero passengers: " ,data['passenger_count'].isin([0]).sum())
    #data = data['passenger_count']>0
    data.columns = (data.columns
                        .str.replace(' ','_')
                        .str.lower()

    )

# Create a new column with only the date part
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    return data.loc[(data['passenger_count'] > 0)  & (data['trip_distance'] > 0) ]


@test
def test_trip_distance(output, *args) -> None:

    # Check if there are trips with zero mileage
    trip_distance_zero_count = output['trip_distance'].isin([0]).sum()
    assert trip_distance_zero_count == 0, f"There are {trip_distance_zero_count} trips with zero mileage"


@test
def vendor_assertion(output, *args) -> None:
    unique_vendor_ids = output['vendorid'].unique()
    assert output['vendorid'].isin(unique_vendor_ids).all(), "Invalid vendor_id found in the DataFrame"

@test
def test_passenger_count(output, *args) -> None:
# Check if there are rides with zero passengers
    passenger_count_zero_count = output['passenger_count'].isin([0]).sum()
    assert passenger_count_zero_count == 0, f"There are {passenger_count_zero_count} rides with zero passengers"
