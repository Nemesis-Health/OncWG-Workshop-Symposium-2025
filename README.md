## Summary

This is a repo with containing SQL queries, the results of which will be used during the 2025 OHDSI Symposium Oncology WG session. 

1. Queries have already been translated into several DBMS. Any other DMBS can be rendered and written to a new folder using `translate.R`. 

2. In all queries, replace @vocabulary_database_schema and @cdm_database_schema with the appropriate schema name before running (either using SqlRender or a simple find and replace wll). 

3. In each case, `1_INITIATE_CONCEPT_SETS.sql` must be run first. This will initiate a temporary `Codesets` table that is used across other queries. 

4. Other queries should run after that in the same session. If the session is interrupted, you may need to run `1_INITIATE_CONCEPT_SETS.sql` again. 