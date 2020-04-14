# parser.py

Simpler parser that takes each CSV timeseries file and makes a JSON record per row in the CSV, and stores in S3, with synthetic partitions for `type=confirmed`, `type=deaths` and `type=recovered`

Use AWS Glue to create the Athena table

# parser2.py

A little more complex parser combines the data from the three CSV timeseries files to create a single JSON record per row, and stores in S3.

Use AWS Glue to create the Athena table