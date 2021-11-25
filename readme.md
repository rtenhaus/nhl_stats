# NHL Stats Data Pipeline Demo

This project is meant to simulate data pipeline in a modern analytics stack.  This project uses python, Google BigQuery, _dbt, and Looker.  I will acquire data from the NHL's website `undocumented` API.


## Extract
This is a simple python demo to create a pseudo-api wrapper around an open HTTP request that returns NHL data.  I won't grab all data, but enough to demo functionality in later ELT steps.

## Load
Loading the data to Google BigQuery.  Many databases could be used, but due to the small amount of data used in this project, Google BigQuery is a cost-efficient choice and requires minimal overhead maintenance.

## Transform
*dbt* is a modern transformation engine in the ELT stack.

## Reporting
Demo Looker code (if time)