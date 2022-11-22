from prefect import Flow, Parameter

from tasks import (
    get_api_data,
    create_partitions_country_state,
)

with Flow("Users info Download [Exercicio ED]") as flow:

    # Tasks
    data = get_api_data( rows=10, gender="female", nat="US,BR" )
    create_partitions_country_state( data )
