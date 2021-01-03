The goal of this project is to design a database for Sparkify to store and analyze their data. Currently the streaming data is stored in json files which makes the analysis of the data hard. To help Sparkify to have a capacity for different queries.

The following steps are taken to tackle the project:

#### Reading Data from json files
1. read the log files into relational database: To make the data queriable, we choosed to use relational database.
2. Using the Star Scehma to struct our database because it makes it easy to query, simpler and faster aggregations which aligns with project goals

##### Database Schems Design and ER Diagram
As it is stated, star schema is used for the purpose of this project.

**Fact and dimension tables**

![ER diagram](https://github.com/parvinDaD/Data-Engineering/blob/ab96f78282a693851e79df7284db31c7c6374f6f/Relational%20DataBase%20Design/ER-diagram.jpeg)


#### Project outline
The project repo can be used with the following steps:
1. execute sql_queries.py: This files consists of all the queries will be used to drop, create, insert and more complex analytical queries needed for the purpose
2. execute create_tables.py: This files is used to drop and recreate tables
3. execute etl.pyL This file has all the script needed for reading json files data into tables.

Supporting files:
test.ipynb: used for testing the queries and format of tables at different point of project development
etl.ipynb: used for developing the etl pipeline
