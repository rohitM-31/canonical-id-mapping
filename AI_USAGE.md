AI Usage Documentation

Project: Unified Entity ID Mapping Pipeline

I have used AI for development assistance to understand the requirements, plan the architecture, generate suggestions, troubleshoot and document.

1. Business Requirement Understanding

Prompt:

Describe the data engineering requirements to map IDs to canonical IDs with an exact match or rule match matching mechanism. Explain the data quality, conflict resolution, mapping history, expected deliverables, and the relationship of entities and the workflow.

Purpose: Translate the requirements of the project to actionable technical tasks.

2. System Architecture and Tool Selection

Prompt:

Describe an easy to build and maintain data pipeline that performs ID mapping between two systems, using PostgreSQL as the data store, Python/Pandas/NumPy as the data processing layer and Apache Airflow as the data pipeline workflow orchestration engine. Describe the steps of the pipeline, starting with data ingestion, followed by data validation and data transformation, then matching and conflict resolution, ending with storage, reconciliation and finally data reporting.

Objective: Describe the system architecture and the reasoning behind each choice.

3. Generate a realistic synthetic dataset for a Unified Entity ID Mapping Pipeline using Python and Pandas.

Create multiple source-system CSV files containing customer records with the following fields:

source_system

source_id

customer_name

email

phone

created_at

updated_at

Generate datasets with different volumes:

Small sample dataset (100 records) for initial development.

Medium dataset (10,000 records) for functional testing.

Large dataset (100,000+ records) for scalability and performance testing

4. Conflict Detection

Prompt:

Develop a conflict detection module that identifies one-to-many mapping conflicts, where a source identifier or matching key is associated with multiple canonical IDs. Generate a conflict report and preserve affected records for investigation.

Purpose: Detect inconsistent mappings and prevent incorrect entity assignments.

5. Apache Airflow DAG

Prompt:

Create an Apache Airflow DAG for a canonical ID mapping pipeline with the following tasks: validate input files, execute the Python mapping pipeline, perform SQL reconciliation checks, and export unresolved records. Include retries, execution logging, configurable input paths, and safe rerun behavior.

Purpose: Automate and orchestrate pipeline execution.

6. Performance
Prompt:
Add a simple performance measurement and one optimisation.

Outcome: `scripts/benchmark.py`; in-memory master index about 21x faster than a SQL lookup per record.
The assistant first reported 1473x, then noticed the SQL baseline was missing indexes (an unfair comparison),
added them and re-measured. The corrected figure (about 21x) is the one used in the README.

7.  Automated Testing

I  write comprehensive test cases for all the project files and cover as many possible scenarios, including positive, negative, boundary, and edge cases. Therefore, I used AI assistance to generate and expand the Pytest test cases to improve overall test coverage and validate the reliability of the pipeline.

Prompt:

Generate Pytest test cases for a Python entity-matching pipeline. Cover exact matches, rule-based matches, empty input, malformed records, missing values, duplicate identifiers, conflicting mappings, unresolved records, unexpected schema changes, and idempotent reruns.

Purpose: Verify pipeline correctness and edge-case handling.

8. Documentation and Code Review

Prompt:

Review the complete Unified Entity ID Mapping Pipeline for code quality, modularity, maintainability, data integrity, error handling, security, and idempotency. Identify missing requirements and suggest improvements. Generate README documentation covering architecture, setup, execution, assumptions, testing, and limitations.

Prompts used while building and running the project (Claude Code)

Pasted the full problem statement and asked: "write the code in the files with the two datasets I downloaded; WSL without Docker; use Airflow."
"How to execute steps line by line" (Windows and WSL/Airflow commands).
Pasted the WSL error (Ubuntu had Python 3.14, Airflow needs 3.12 or older) and asked for a fix.
Pasted `airflow dags test` output for a first run and a rerun, and asked how to see the DAG in the Airflow web UI.
Questions about which files are necessary (benchmark, generator, run script, data/large, sql folder) and which can be deleted.

Where AI accelerated my work

Project structure, module split, SQL schema, Airflow DAG and WSL setup script.
Writing the 82 pytest cases covering the required edge cases.
The seeded synthetic data generator with a separate answer key.
Debugging environment problems (WSL Python version, empty DAG list caused by the "Running" filter in the UI).

Where I relied on AI heavily

Most of the first implementation was generated from the problem statement.
The wording of the README, data dictionary and design note.
The Airflow DAG and the WSL setup script.

What I personally reviewed, changed or validated

(Fill in with what you actually did. Examples:)
- Ran the test suite myself (82 passed) and the pipeline on dataset_1.csv and dataset_2.csv.
- Ran the DAG in Airflow and reran it to confirm no duplicate rows are inserted.
- Read the matching, history and idempotency code and can explain it.
- Changes I made myself (for example deleting data/large, restructuring the README).

How I know the solution works

82 automated tests pass, including an end-to-end test on the two delivered files.
10 SQL reconciliation checks pass on every report, and a test corrupts data on purpose to prove they can fail.
Counts for rejected rows (8) and duplicates (6) in dataset_1.csv were profiled separately and then asserted in a test.
A generated dataset was scored against a separate answer key (3,000 rows in tests, 100,000 rows in a manual run).
The DAG ran in Airflow 2.10.5, and a second run inserted 0 rows.

Limits of the AI-generated work

The canonical master is synthetic; no real master was provided.
100% accuracy on generated data only shows the code matches its own rules, not real-world accuracy.
Matching is rule-based only (no fuzzy matching). See README section 9 for the full list.

AI Usage Guidelines

AI tools were used for technical guidance, implementation suggestions, debugging, and documentation.

Generated code was reviewed and adapted to the project's actual requirements.

Database operations, matching rules, and pipeline outputs were tested before acceptance.

AI-generated suggestions were not treated as automatically correct.

Final implementation decisions and validation were performed by the developer.

