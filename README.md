# Freedom Convoy Data Product

Unveiling the narratives of the Freedom Convoy protest movement through data, this repository tracks the unfolding events, involved actors, and evolving narratives. Sourcing data from GDELT, we delve into a deeper understanding of societal dynamics.

## Installation
1. Clone the repository.
2. Ensure you have Poetry installed for dependency management.
3. Run poetry install to install all necessary dependencies.

## Usage
1. Have a `.env` file and define the following environment variables:
```
export SNOWFLAKE_ACCOUNT=dz02451.us-east-1
export SNOWFLAKE_USER=[olivier]
export SNOWFLAKE_PASSWORD='[PASSWORD]'
export SNOWFLAKE_ROLE=DEV_ROLE
export SNOWFLAKE_WAREHOUSE=FREEDOM_CONVOY_DEV_WAREHOUSE
export SNOWFLAKE_DATABASE=FREEDOM_CONVOY
export SNOWFLAKE_SCHEMA=[OLIVIER]_DEV
```
2. Run the Dagster pipelines to load, transform, and enhance the data.
3. Explore the data in Snowflake

## Technologies Used
- Dagster for data orchestration.
- dbt for data transformation.
- Snowflake for data management.
