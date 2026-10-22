# Ontario Electricity Market Intelligence Platform

## Project Overview

This project aims to develop a cloud-based data engineering pipeline that collects, processes, and analyzes regularly updated electricity market data published by the Independent Electricity System Operator (IESO) in Ontario, Canada. The platform is designed to integrate Azure Data Factory, Azure Functions, Kafka-compatible Azure Event Hubs, Azure Databricks, PySpark, Spark SQL, Delta Lake, and Power BI into an automated data workflow.

The project focuses on data engineering rather than price forecasting or machine learning. Its main goal is to make public electricity market information reliable, traceable, and accessible for business analysis. The architecture and milestones below describe the planned system, not a completed deployment.

### Project Status

- Current stage: Project planning and source-access validation.
- Completed: Verified that the IESO Real-Time Energy LMP report can be downloaded through Python using an unauthenticated HTTPS request.
- Verified test result: HTTP 200; a CSV file of 488,964 bytes was downloaded in approximately 1.1 seconds during a local test.
- Next milestone: Configure GitHub and Azure, then demonstrate an ADF-triggered Azure Function archiving an IESO report in Azure Data Lake Storage Gen2.
- Not yet implemented: Cloud orchestration, Event Hubs publishing, Databricks transformations, SQL marts, and Power BI dashboard.

## 1. Business Problem and Objectives

Ontario's wholesale electricity market produces pricing, demand, and generation reports at different frequencies and in different formats. This project will consolidate these reports into an analytics-ready platform for market analysts, energy purchasers, generators, and other electricity-sector stakeholders.

### Business Questions

- What are the latest real-time electricity prices at different Ontario pricing locations?
- How do wholesale electricity prices change throughout the day?
- Which locations experience higher price volatility or congestion-related price differences?
- How do day-ahead hourly prices compare with real-time prices for the same location and delivery hour?
- How does Ontario electricity demand vary by time of day and across days?
- How do demand and generation conditions relate to observed market-price changes?
- How complete and up to date are the underlying reports and analytical tables?

### Engineering Objectives

- Automate retrieval of genuine IESO public CSV and XML reports.
- Support scheduled incremental ingestion rather than repeatedly reprocessing unchanged files.
- Handle source corrections, missing reports, duplicate records, and pipeline interruptions.
- Implement Kafka-compatible event messaging through Azure Event Hubs.
- Build an Azure Databricks lakehouse using Bronze, Silver, and Gold Delta tables.
- Apply PySpark and SQL to create validated, relational analytical datasets.
- Use Azure Data Factory for cross-service orchestration and Databricks Jobs for Spark execution.
- Provide a Power BI dashboard and a separate data-pipeline health view.
- Keep Python, SQL, orchestration definitions, tests, and cloud configurations under GitHub version control.

## 2. Project Architecture

```text
IESO Public Reports (CSV / XML)
               |
               v
Azure Data Factory (schedule and orchestration)
               |
               v
Azure Functions (Python HTTP collector)
               |
        +------+------+
        |             |
        v             v
ADLS Gen2 Archive    Azure Event Hubs (Kafka endpoint)
Original reports              |
                               v
                  Azure Databricks Job
                  Spark Structured Streaming
                               |
                               v
                         Bronze Delta
                               |
                               v
                         Silver Delta
                               |
                               v
                         Gold Delta / SQL
                               |
                               v
                     Databricks SQL Warehouse
                               |
                               v
                        Power BI Dashboard

ADF also invokes the Databricks Job on its processing schedule.
GitHub stores code, infrastructure definitions, and deployment configuration.
```

### Cloud Services and Their Responsibilities

- Azure Data Factory (ADF)
  - Own scheduled triggers and cross-service pipeline dependencies.
  - Invoke the Python Azure Function and Databricks Jobs.
  - Record pipeline success, failure, and retries.
  - Do not act as a Kafka broker or transport individual streaming messages.
- Azure Functions
  - Execute Python code to retrieve IESO reports through HTTPS.
  - Check whether source files are new or revised.
  - Archive the original source reports and publish accepted records to Event Hubs.
- Azure Data Lake Storage Gen2
  - Preserve raw, versioned IESO reports for recovery and audit purposes.
  - Hold Delta Lake analytical data where configured through Databricks and Unity Catalog.
- Azure Event Hubs
  - Provide a managed Kafka-compatible endpoint for event producers and consumers.
  - Buffer new and revised market observations until Databricks processes them.
  - Replace the need for a separate cloud-hosted Apache Kafka cluster in the initial deployment.
- Azure Databricks
  - Ingest Kafka messages using Spark Structured Streaming.
  - Process data incrementally using PySpark and Spark SQL.
  - Organize analytical data into Bronze, Silver, and Gold Delta tables.
  - Execute Spark tasks through Databricks Jobs invoked by ADF.
- Unity Catalog
  - Manage metadata, permissions, and data lineage for governed tables.
- Databricks SQL and Power BI
  - Query curated Gold tables and visualize market conditions and data freshness.
- GitHub and GitHub Actions
  - Version-control Python, SQL, infrastructure, ADF definitions, and Databricks Bundles.
  - Automate linting, tests, validation, and approved deployments.

## 3. Data Sources: IESO Public Reports

- Data provider: Independent Electricity System Operator (IESO), Ontario, Canada.
- Official directory: https://www.ieso.ca/en/power-data/data-directory
- Public report server: https://reports-public.ieso.ca/public/
- Access method: Public HTTPS CSV/XML downloads. The selected public files do not require an API key.
- Collection model: Poll stable latest-report URLs, archive new versions, and reconcile dated historical reports.
- Important distinction: The market reporting interval is not necessarily the same as the time at which a report file is published.
- Source schemas, actual header names, and exact deduplication keys will be validated from downloaded files during development.

### 3.1 Real-Time Energy LMP (CSV) - MVP

- Official report: Real-time 5-min Energy LMP Report.
- Latest file: `PUB_RealtimeEnergyLMP.csv`
- URL: https://reports-public.ieso.ca/public/RealtimeEnergyLMP/PUB_RealtimeEnergyLMP.csv
- Archive directory: https://reports-public.ieso.ca/public/RealtimeEnergyLMP/
- Format: CSV.
- Reporting interval: Five minutes, after successful real-time market calculations.
- Report coverage: Generator and load pricing locations in the IESO-administered market.
- What the data represents:
  - Locational Marginal Price (LMP), expressed in CAD per MWh.
  - Delivery date, hour, and five-minute reporting interval.
  - Generator or load pricing-location identifier.
  - Price components associated with energy, transmission losses, and congestion.
- Planned business use:
  - Monitor recent electricity market prices by location.
  - Analyze price volatility, congestion components, and price differences across locations.
  - Calculate hourly real-time price summaries for comparison with day-ahead prices.
- Planned ingestion:
  - ADF initiates frequent source checks, initially every five minutes.
  - Python archives only new or revised report contents and publishes identifiable observations to Event Hubs.
- Important consideration:
  - Repeated downloads can contain observations already retrieved; report versions and business keys must be tracked separately.

### 3.2 Real-Time Totals / Ontario Demand (CSV or XML) - MVP

- Official report: Real-time Totals Report (Ontario and Market Demand).
- Latest CSV file: `PUB_RealtimeTotals.csv`
- URL: https://reports-public.ieso.ca/public/RealtimeTotals/PUB_RealtimeTotals.csv
- Archive directory: https://reports-public.ieso.ca/public/RealtimeTotals/
- Format: CSV or XML; initial implementation will use CSV.
- Reporting interval: Five minutes.
- What the data represents:
  - Ontario electricity demand.
  - Total energy scheduled in the IESO-administered market.
  - System losses, load, and scheduled operating-reserve quantities where provided by the report.
  - Delivery date, hour, interval, and associated system-level measurements.
- Planned business use:
  - Monitor provincial electricity demand throughout the day.
  - Compare electricity demand with contemporaneous real-time prices.
  - Analyze peak-demand periods and short-term demand variability.
- Planned ingestion:
  - Poll on the same initial schedule as Real-Time LMP.
  - Preserve the source-defined distinction between Ontario Demand and Market Demand.
- Important consideration:
  - Demand is a system-level measurement, not a separate demand value for each LMP pricing location.
  - Joining this report with LMP data requires matching reporting intervals rather than assuming a common location key.

### 3.3 Day-Ahead Hourly Energy LMP (CSV) - Full Version

- Official report: Day-ahead Hourly Energy LMP Report.
- Latest file: `PUB_DAHourlyEnergyLMP.csv`
- URL: https://reports-public.ieso.ca/public/DAHourlyEnergyLMP/PUB_DAHourlyEnergyLMP.csv
- Archive directory: https://reports-public.ieso.ca/public/DAHourlyEnergyLMP/
- Format: CSV.
- Reporting interval: One hour for the delivery day.
- Publication: Day-ahead market results; generally a daily report rather than a new report every five minutes.
- What the data represents:
  - Day-ahead energy LMP for generator and load pricing locations.
  - Hourly delivery periods and the relevant pricing locations.
  - Energy price components, including losses and congestion.
- Planned business use:
  - Compare day-ahead and real-time electricity prices.
  - Analyze hourly price spreads and periods of disagreement between markets.
  - Assess price volatility for selected locations and hours.
- Planned ingestion:
  - Schedule collection around the expected daily publication period.
  - Reconcile reports that have been republished or corrected.
- Important consideration:
  - The day-ahead price is a market outcome, not a conventional statistical forecast of the real-time price.
  - Hourly day-ahead values must be compared with appropriately aggregated five-minute real-time observations.

### 3.4 Generator Output and Capability (XML) - Full Version

- Official report: Generator Output and Capability Report.
- Latest file: `PUB_GenOutputCapability.xml`
- URL: https://reports-public.ieso.ca/public/GenOutputCapability/PUB_GenOutputCapability.xml
- Archive directory: https://reports-public.ieso.ca/public/GenOutputCapability/
- Format: XML for the current operational report, not CSV.
- Reporting interval: Hourly output and capability measurements.
- Coverage: Generating facilities participating in the IESO-administered market with maximum output capability of at least 20 MW.
- What the data represents:
  - Generating facility identifier or name.
  - Fuel category, such as nuclear, hydro, gas, wind, and solar.
  - Hourly output in MW.
  - Available capability where applicable.
  - Forecast generation for variable sources such as wind and solar instead of capability where specified.
- Planned business use:
  - Describe generation output and generation mix.
  - Compare hourly generation conditions with market demand and price conditions.
  - Examine variation in facility output over time.
- Planned ingestion:
  - Poll periodically for changes to hourly or cumulative daily reports.
  - Preserve the original XML and parse it into structured Kafka events or Delta records.
- Important consideration:
  - Output, capability, and forecast are not interchangeable measurements.
  - Generator identifiers may not directly match pricing-location identifiers. Any facility-to-price mapping must be explicitly validated.

### 3.5 Historical Generator Output and Capability (CSV) - Backfill Option

- Official report: Monthly Generator Output and Capability Data.
- Latest CSV file: `PUB_GenOutputCapabilityMonth.csv`
- URL: https://reports-public.ieso.ca/public/GenOutputCapabilityMonth/PUB_GenOutputCapabilityMonth.csv
- Archive directory: https://reports-public.ieso.ca/public/GenOutputCapabilityMonth/
- Format: CSV.
- Historical coverage: Monthly report files from May 2019 onward, as listed by IESO.
- What the data represents:
  - Historical hourly generator measurements organized within monthly reporting files.
  - Generation output and related capability/forecast measurements, as defined by the report.
- Planned business use:
  - Load historical generating-facility activity before the continuous collector begins operating.
  - Validate longer-term generation patterns.
- Important consideration:
  - This historical CSV is different from the current XML report and requires its own parsing and reconciliation logic.

### 3.6 Historical Ontario and Market Demand (CSV) - Backfill Option

- Official report: Historical hourly Ontario and Market Demand.
- Report directory: https://reports-public.ieso.ca/public/Demand/
- Format: CSV.
- Historical coverage: IESO lists annual hourly demand files from 2002 onward.
- What the data represents:
  - Hourly Ontario Demand and Market Demand observations over historical periods.
- Planned business use:
  - Provide longer-term reference data for demand patterns and seasonality.
- Important consideration:
  - Historical hourly demand and current five-minute Real-Time Totals are different report families and should not be merged without harmonizing definitions and time grains.

### 3.7 Source and Time Conventions

- IESO market reports may use delivery-hour conventions such as hour ending 1 through 24.
- The pipeline will preserve source fields and convert report-defined times to UTC using verified market-time rules.
- Report dates, delivery dates, created timestamps, and retrieval timestamps may represent different moments.
- File content may be cumulative or revised; files are not automatically equivalent to individual immutable events.
- Ontario's renewed wholesale market began on May 1, 2025. Historical legacy HOEP prices are not directly equivalent to the renewed-market LMP series.
- Source attribution and permitted reuse will follow the IESO Terms of Use.

## 4. Data Processing and Modelling Plan

### Bronze Layer

- Ingest Kafka-compatible Event Hubs messages through Databricks Spark Structured Streaming.
- Preserve raw event payloads and message metadata, including topic, partition, offset, and ingestion timestamp.
- Write append-only Delta tables for raw prices, demand, day-ahead prices, and generation events.
- Use persistent checkpoints to resume processing after interruptions.
- Retain original CSV/XML files separately in the ADLS Gen2 raw archive.

### Silver Layer

- Parse raw JSON events and apply explicit schemas.
- Standardize date, time, identifiers, numeric types, and units.
- Validate required fields and quarantine malformed observations.
- Use source-version-aware keys to handle duplicates and corrected reports.
- Maintain validated current-state measurements without discarding source revision history.
- Check interval completeness and source-to-target record counts.

### Gold Layer

- Build analytical tables using PySpark and Spark SQL.
- Proposed dimensions:
  - `dim_time`: Common time and reporting-period attributes.
  - `dim_pricing_location`: Validated electricity pricing locations.
  - `dim_generator`: Generator reference information where available.
  - `dim_fuel_type`: Generation technology categories.
- Proposed facts:
  - `fact_rt_lmp_5m`: Five-minute electricity prices by location.
  - `fact_ontario_demand_5m`: System-level demand observations.
  - `fact_da_lmp_hourly`: Day-ahead hourly electricity prices by location.
  - `fact_generator_hourly`: Hourly generating-facility observations.
- Proposed business marts:
  - `mart_market_overview`: Current market and demand summary.
  - `mart_price_volatility`: Price variation by time and pricing location.
  - `mart_hourly_price_spread`: Day-ahead versus hourly real-time price differences.
  - `mart_pipeline_freshness`: Source and processing freshness indicators.
- Validate fact-table grain and referential integrity through SQL tests.
- Avoid many-to-many joins or invalid comparisons between five-minute and hourly data.

## 5. Orchestration and Scheduling

- Azure Data Factory is the primary scheduler and cross-service orchestrator.
- ADF triggers Azure Functions to collect source reports.
- ADF invokes Databricks Jobs to run the Bronze, Silver, and Gold processing workflow.
- Databricks Jobs manage Spark task dependencies but do not maintain a second, competing production schedule.
- Planned ADF pipelines:
  - `pl_ieso_collect`: Scheduled ingestion of real-time reports.
  - `pl_databricks_processing`: Incremental Event Hubs-to-Gold processing.
  - `pl_ieso_reconciliation`: Scheduled checks for missed or revised source reports.
- Proposed initial timing:
  - LMP and demand collection: Every five minutes.
  - Databricks incremental processing: Every 15 minutes.
  - Generator report collection: Every five to ten minutes once added.
  - Day-ahead report collection: Near expected publication, with daily reconciliation.
- Refine schedules after measuring real publication delay, actual data volume, and Azure operating cost.
- Use cost-conscious incremental Spark processing where practical instead of running Databricks compute continuously.

## 6. Data Quality, Monitoring, and Recovery

- Record HTTP request status, retrieval time, source modification metadata, and report content hashes.
- Preserve each unique or revised raw source report in ADLS Gen2.
- Maintain a persistent manifest for collection, archival, and event-publishing status.
- Apply retries and backoff to transient errors without creating duplicate analytical results.
- Reconcile dated IESO reports to identify missed updates or source corrections.
- Store stable event IDs and business keys for replay-safe processing.
- Monitor source freshness, Kafka delivery acknowledgements, consumer lag, invalid rows, and Databricks job failures.
- Test system restart, duplicate delivery, corrected reports, and interrupted Spark processing.
- Measure collection and end-to-end processing latency using actual logs rather than assumed five-minute availability.

## 7. Business Intelligence Dashboard

- Platform: Power BI connected to curated Databricks SQL tables.
- Planned report pages:
  - Market Overview: Recent prices, Ontario demand, latest reporting period, and key changes.
  - Real-Time vs Day-Ahead: Hourly price differences and market comparisons.
  - Demand and Generation: Provincial load patterns and generating-facility output.
  - Pipeline Health: Report freshness, ingestion lag, validation failures, and workflow status.
- Validate dashboard values against SQL queries.
- Evaluate Import and DirectQuery based on cost, refresh requirements, and available Power BI licensing.
- Include explicit last-updated timestamps and data-source attribution.

## 8. Development and Version Control

- Primary development environment: Azure and Azure Databricks.
- Source of truth: GitHub repository.
- Azure Data Factory pipelines: GitHub integration for version-controlled definitions.
- Databricks development: Git folders connected to the repository.
- Databricks deployments: Declarative Automation Bundles with separate development and production targets.
- Azure Functions: Python source code deployed from GitHub.
- GitHub Actions: Unit tests, linting, validation, and approved deployments.
- Cloud resource provisioning: Document initially; add Bicep or Terraform for reproducibility.
- Security: Do not commit keys, connection strings, source credentials, or private configuration.

### Planned Repository Structure

```text
ontario-electricity-lakehouse/
  README.md
  configs/
  src/
    ingestion/
    parsers/
    streaming/
  functions/
    ieso_ingest/
  adf/
  databricks/
    bronze/
    silver/
    gold/
    monitoring/
  sql/
    dimensions/
    facts/
    marts/
  resources/
  infra/
  tests/
  powerbi/
  docs/
  .github/
    workflows/
  databricks.yml
```

## 9. Project Milestones

### M1. Cloud Foundation and First Data Ingestion

- Create and configure the GitHub repository.
- Confirm Azure subscription capabilities, permissions, region, and spending limits.
- Create a dedicated resource group and ADLS Gen2 storage account.
- Deploy the initial IESO Python collector as an Azure Function.
- Create ADF with GitHub integration and invoke the collector from a manual pipeline run.
- Verify that an actual IESO report is archived in Azure Storage.
- Confirm that rerunning the collector does not create an unnecessary duplicate archive.

### M2. Reliable Source Ingestion

- Define source-specific schemas and ingestion contracts.
- Implement source version detection, hashes, and a durable manifest.
- Add retry handling, validation, source reconciliation, and historical backfill.
- Test missing, duplicate, and corrected IESO reports.

### M3. Kafka-Compatible Streaming

- Provision Azure Event Hubs Standard and Kafka endpoints.
- Configure the Python producer to publish normalized source observations.
- Create topic/event-hub naming, message keys, and schemas.
- Test Kafka-compatible delivery acknowledgements, duplicates, and replay.

### M4. Databricks Bronze and Silver

- Create Azure Databricks with Unity Catalog and GitHub integration.
- Connect Structured Streaming to Event Hubs using the Kafka source.
- Implement Bronze ingestion, Delta checkpoints, and Silver validation.
- Test incremental processing, duplicate handling, and corrected values.

### M5. Gold SQL Modelling

- Finalize dimension and fact table grains and join keys.
- Create business-ready Delta tables using SQL and PySpark.
- Develop market-overview and volatility analytical marts.
- Validate analytical outputs against source reports.

### M6. Additional IESO Sources

- Add day-ahead pricing and generator output/capability feeds.
- Integrate historical backfill data where needed.
- Create hourly price-spread and generation analytical views.

### M7. Full Orchestration and Operations

- Configure ADF schedules, dependencies, retries, and Databricks Job activity.
- Implement monitoring, reconciliation, and recovery workflows.
- Test multiple unattended cycles and failure scenarios.
- Measure performance, data freshness, and cost.

### M8. Power BI Dashboard

- Connect Power BI to Gold analytical tables.
- Build market, pricing, generation, and platform-health report pages.
- Validate calculated KPIs and reporting freshness.
- Prepare dashboard screenshots and demonstration material.

### M9. CI/CD and Portfolio Release

- Implement GitHub Actions checks and deployment workflows.
- Version-control Databricks Bundles, ADF definitions, and cloud resource configurations.
- Document architecture, lineage, data quality, setup, costs, and recovery.
- Publish actual benchmark results and project limitations.
- Prepare a reproducible demonstration of the complete pipeline.

## 10. Expected Deliverables

- Cloud-hosted ingestion of real, regularly updated Ontario electricity market reports.
- Kafka-compatible data streaming from Python into Azure Event Hubs.
- Managed Azure Data Factory orchestration and Databricks Job execution.
- Delta Lake Bronze, Silver, and Gold datasets with validated relational SQL models.
- Reliable handling of duplicates, revisions, and interrupted processing.
- Interactive Power BI market intelligence dashboard.
- Version-controlled infrastructure and pipeline code with automated tests.
- Documentation with architecture diagrams, sample schemas, execution evidence, benchmarks, and cost controls.

## 11. References and Data Attribution

- [IESO Data Directory](https://www.ieso.ca/en/power-data/data-directory)
- [IESO Public Reports Repository](https://reports-public.ieso.ca/public/)
- [IESO Terms of Use](https://www.ieso.ca/terms-of-use)
- [IESO Market Renewal launch, May 1, 2025](https://www.ieso.ca/en/Sector-Participants/IESO-News/2025/05/The-Launch-of-the-Renewed-Market-is-Currently-Underway)
- [Azure Event Hubs for Kafka examples](https://github.com/Azure/azure-event-hubs-for-kafka)
- [Azure Data Factory Azure Function activity](https://learn.microsoft.com/en-us/azure/data-factory/control-flow-azure-function-activity)
- [Azure Data Factory Databricks Job activity](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-databricks-job)
- [Azure Databricks Kafka streaming source](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/streaming-event-hubs)
- [Databricks Declarative Automation Bundles examples](https://github.com/databricks/bundle-examples)
- [gridstatus IESO Python implementation](https://github.com/gridstatus/gridstatus)
- [GridGPT IESO streaming reference](https://github.com/kaustav927/GridGPT)

### Data Usage Notes

- All electricity market data originates from IESO, not from this repository.
- This project is independent of IESO and is not endorsed by IESO.
- Any stored examples or redistributed source content must comply with the IESO Terms of Use and applicable report-specific conditions.
- The project will not publish private cloud credentials, bulk source archives, or confidential market-participant data.

## 12. Project Limitations

- The architecture is planned and the full Azure pipeline has not yet been deployed.
- The current source validation consists of a successful manual CSV download; recurring cloud collection remains to be implemented and tested.
- Exact report schemas, real file-update latency, monthly cloud cost, and performance metrics are not yet measured.
- Azure Event Hubs supports Kafka-compatible producer and consumer APIs but is not a self-managed Apache Kafka cluster.
- Power BI report freshness will depend on the completed Databricks pipeline and the chosen refresh/query mode.
