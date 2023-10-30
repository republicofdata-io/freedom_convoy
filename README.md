# Freedom Convoy Data Product

Unveiling the narratives of the Freedom Convoy protest movement through data, this repository tracks the unfolding events, involved actors, and evolving narratives. Sourcing data from GDELT, we delve into a deeper understanding of societal dynamics.

## Installation
1. Clone the repository.
2. Ensure you have Poetry installed for dependency management.
3. Run poetry install to install all necessary dependencies.

## Usage
1. Configure your AWS S3, dbt, and DuckDB settings as outlined in the __init__.py file.
2. Run the Dagster pipelines to load, transform, and enhance the data.
3. Explore the data in DuckDB to unravel the events and narratives of the Freedom Convoy protest movement.

## Technologies Used
- Dagster for data orchestration.
- dbt for data transformation.
- DuckDB for data management.
- AWS S3 for storage and IO management.
