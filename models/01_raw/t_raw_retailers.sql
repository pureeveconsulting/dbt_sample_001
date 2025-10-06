SELECT *
FROM read_csv('/home/u001/dbt_sample/dbt_sample_001/csv/go_retailers.csv',
    delim = ',',
    header = true
    )