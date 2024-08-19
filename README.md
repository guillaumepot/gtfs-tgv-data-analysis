# HighSpeed Train delay prediction (France | TGV)

Can We predict delay for TGV trains in France ?


<!-- 
IMG TO ADD 
<img src="./media/img.jpeg" width="350" height="350">
-->


---


## Project Information

- **Version**: 0.0.1dev
- **Development Stage**: Dev
- **Author**: Guillaume Pot
- **Contact Information**: guillaumepot.pro@outlook.com


---


## Table of Contents [WIP]
- [Introduction](#introduction)
- [What is GTFS](#what-is-gtfs)
-

---


## Introduction

[WIP]

---


## What is GTFS [WIP]


SNCF GTFS TR is updated every 2mins, display from now to now+60mins
-> Update datas with gtfr_rf_updater every 6hours ? 

---


## Current Features [WIP]

- 

---


## Repo Architecture [WIP]

```
|
├── airflow <- Contains all airflow files to orchestrate pipelines
|
├── notebooks <- Draft code to explore data files
|
└── postgres <- Postgres database to store GTFS & GTFS RT datas

```


---


## Branch logic [WIP]

```

├── main    # Main branch, contains releases
|   
├── build   # Used to build releases
|
├── debug   # Debug branch
|
└── develop # New features development branch

```


---


## Installation [WIP]
- Update postgres/docker compose & set postgres credentials
- Update airflow/.env file with postgres infos & credentials



---


## Changelogs [WIP]

-



---


## Roadmap [WIP]

```

-

```



---


## Data sources

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