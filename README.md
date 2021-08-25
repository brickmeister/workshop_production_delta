# Introduction : Databricks 1:M Production Delta Workshop

Welcome to the repository for the Databricks 1:M Production Delta Workshop!

This repository contains the notebooks that are used in the workshop to demonstrate the use of production structured streaming, delta lake, as well as the databricks medallion architecture.

[Link to Databricks Workshop](https://pages.databricks.com/202106-AMER-VE-MMComm-Delta-in-ProductionHands-on-Workshop-Delta-in-Production-2021-06-03.html)

![delta_lake](img/Multi-Hop%20Delta%20Lake.png)

- [Introduction : Databricks 1:M Production Delta Workshop](#introduction--databricks-1m-production-delta-workshop)
- [Reading Resources](#reading-resources)
- [Workshop Flow](#workshop-flow)
  - [Generator](#generator)
- [Setup / Requirements](#setup--requirements)
  - [DBR Version](#dbr-version)
  - [Repos](#repos)
  - [DBC Archive](#dbc-archive)

# Reading Resources

* [Lakehouse Whitepaper](https://databricks.com/wp-content/uploads/2020/12/cidr_lakehouse.pdf)
* [Delta Lake Primer](https://databricks.com/wp-content/uploads/2019/01/Databricks-Delta-Technical-Guide.pdf)
* [Delta Live Tables](https://databricks.com/blog/2021/05/27/announcing-the-launch-of-delta-live-tables-reliable-data-engineering-made-easy.html)
* [Photon Execution Engine](https://databricks.com/blog/2021/06/17/announcing-photon-public-preview-the-next-generation-query-engine-on-the-databricks-lakehouse-platform.html)

# Workshop Flow

The workshop consists of 4 interactive sections that are separated by 4 notebooks located in the notebooks folder in this repository. Each is run sequentially as we explore the abilities of the lakehouse from data ingestion, data curation, and performance optimizations
|Notebook|Summary|
|--------|-------|
|`01-Intro to Unified Delta Processing.py`|Processing and ingesting data at scale utilizing databricks tunables and the medallion architecture|
|`02-DLT for Data Engineering Pipelines.py`|Curating data and pipelining it via Delta Live Tables (DLT)|
|`03-Delta Jobs Optimization.py`|Debugging and understanding delta lake performance|
|`04-Production Compliance.py`|Handy delta lake features for production compliant workloads|

## Generator
In order to visualize the effect of increasing throughput on Delta Lake performance, there is a generator script located in lib. Run `00-Generator.py` after `01-Intro to Unified Delta Processing.py` in Databricks to increase throughput requirements on the delta stream.


# Setup / Requirements

This workshop requires a running Databricks workspace. If you are an existing Databricks customer, you can use your existing Databricks workspace. Otherwise, the notebooks in this workshop have been tested to run on [Databricks Community Edition](https://databricks.com/product/faq/community-edition) as well.

## DBR Version

The features used in this workshop require `DBR 8.3`.

## Repos

If you have repos enabled on your Databricks workspace. You can directly import this repo and run the notebooks as is and avoid the DBC archive step.

## DBC Archive

Download the DBC archive from releases and import the archive into your Databricks workspace.
