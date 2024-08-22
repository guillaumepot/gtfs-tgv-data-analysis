# HighSpeed Train data analysis & predictions (France | TGV)


This project covers Data engineering, Datascience & Mlops.

- Get the datas form GTFS & GTFS RT sources and store them in a Database
- use ETL pipelines to serve different purposes
- Analyse datas with a dashboard
- Train a ML model to predict delays & delay time.



<img src="./media/project_img.jpeg" width="350" height="350">



## Current Features


- Get & update (every 5 mins) GTFS Real Time datas, add these datas to a postgres Database.




## Project Information

- **Version**: 0.0.1
- **Development Stage**: Dev
- **Author**: Guillaume Pot

[![LinkedIn Badge](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/062guillaumepot/)




## Table of Contents
- [Introduction](#introduction)
- [What is GTFS](#what-is-gtfs)
- [Installation](#installation)
    - [Requirements](#requirements)
- [Repository](#repository)
    - [Architecture](#architecture)
    - [Branch Policy](#branch-policy)
    - [Changelogs](#changelogs)
    - [Roadmap](#roadmap)
    - [Contributions](#contirbutions)
- [Modules](#modules)
    - [Airflow](#airflow)
    - [Postgres](#postgres)
- [Miscellaneous](#miscellaneous)
    - [Data Sources](#data-sources)




## Introduction

[WIP]



## What is GTFS

``` text
The General Transit Feed Specification (GTFS) is an Open Standard used to distribute relevant information about transit systems to riders. It allows public transit agencies to publish their transit data in a format that can be consumed by a wide variety of software applications. Today, the GTFS data format is used by thousands of public transport providers.

GTFS consists of two main parts: GTFS Schedule and GTFS Realtime. GTFS Schedule contains information about routes, schedules, fares, and geographic transit details, and it is presented in simple text files. This straightforward format allows for easy creation and maintenance without relying on complex or proprietary software.

GTFS Realtime contains trip updates, vehicle positions, and service alerts. It is based on Protocol Buffers, which are a language (and platform) neutral mechanism for serializing structured data.

GTFS is supported around the world and its use, importance, and scope has been increasing. It’s likely that an agency you know already uses GTFS to represent routes, schedule, stop locations, and other information, and that riders are already consuming it via various applications.

```
<b>Source: https://gtfs.org/</b>



## Installation

-
-




### Requirements

- Docker
-




## Repository
### Architecture


``` yaml
|
├── changelogs         # Changelogs files which contains changes for each new version 
|
├── media              # General directory, contains images & schemas for the repository
|
├── notebooks          # Notebooks used to test functions, data exploration. Draft code only.
|
├── src                # Contains 'modules' used to run the app
|    |
|    ├── airflow       # Airflow files
|    |
|    ├── postgres      # Postgres db files
|    |
|    └── unit_tests    # Unit tests files
|
├── storage
|      |
|      ├── cleaned    # Cleaned data
|      |
|      ├── gtfs       # GTFS data
|      |
|      └── raw        # Raw data 
|
├── .gitignore
|
├── LICENSE
|
└── README.md

```


### Branch Policy

``` yaml

├── main    # Main branch, contains releases
|   
├── build   # Used to build releases
|
├── debug   # Debug branch
|
└── develop # New features development branch

```



### Changelogs

[v0.0.1](./changelogs/0.0.1.md)



### Roadmap

```

- Dag to ingest GTFS (not RT) datas in the PG database
- ETL pipeline to upgrade GTFS data structure
- BI board for data analysis
- Train delay prediction (classification)
- Train time delay prediction (regression)

```


### Contributions

```

-

```


## Modules

### Airflow

### PostGres







## Miscellaneous

### Data sources

Train stations:
https://ressources.data.sncf.com/explore/dataset/gares-de-voyageurs/information/?disjunctive.segment_drg

Station occupation:
https://ressources.data.sncf.com/explore/dataset/frequentation-gares/information/?disjunctive.nom_gare&disjunctive.code_postal&sort=nom_gare

TGV global ponctuality:
https://ressources.data.sncf.com/explore/dataset/reglarite-mensuelle-tgv-nationale/information/

TGV by route ponctuality:
https://ressources.data.sncf.com/explore/dataset/regularite-mensuelle-tgv-aqst/information/

GTFS:
https://ressources.data.sncf.com/explore/dataset/horaires-des-train-voyages-tgvinouiouigo/information/

Traffic informations:
https://www.sncf-voyageurs.com/fr/voyagez-avec-nous/horaires-et-itineraires/