# XEStoDB

This repository the follwing folders:
- `Schemas/` -- SQL scripts to reproduce Monolithic, DBXES, RXES and RXES+ database schemas;
- `Logs/` -- The real-life and synthetic XES event logs used during the experiments;
- `src/sql_server/` -- Java scripts to translate a XES-formatted event log file into one of the mentioned DBs;
- `src/sql_server/test` -- Java scripts to run performance experiments ;
- `Dumps/` -- SQL dumps containing each of the populated logs;
- `Queries/` -- SQL queries reproducing the implemented Declare templates and all of their different types (IS, LRC, MT, VAL, RNG);
- `Performance/` -- Performance experiments raw results.

## Software requirements

- Java 11
- Microsoft SQL Server 2019

## User manual

To populate a new DB:
1. Create a new DB with SQL Server 2019;
2. Apply to it the schema that you want to use;
3. Launch the Java script related to the chosen schema (e.g. Monolithic schema corresponds to XesToMonolithic.java script);
4. Insert the name of your newly created DB in the console;
5. Choose the XES log to translate;
6. Wait until the execution ends.

Otherwise, we provide you dumps of the logs we used during the research work; you can simply run it by using SQL Server 2019 and the related log will be populated.

## Experiment reproducibility

To reproduce the performance experiments for BS and Join query sets, run `src/sql_server/test/TestQuerySets.java` by specifying inside the java file:
- the name of the DB on which to try the queries;
- the DB schema (monolithic, dbxes, rxes, rxes+) to which your DB is compliant;
- the query set (BS or Join) you want to try out.

To reproduce the performance experiments for new query types (IS, LRC, MT, VAL), run `src/sql_server/test/TestNewQueryTypes.java` by specifying inside the java file only the name of the DB on which to try the queries.

To reproduce the performance experiments for RNG query type, run with SQL Server 2019 one of the queries inside `Queries/RNG queries` folder, depending on the schema to which your DB is compliant.
