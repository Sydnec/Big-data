# Big Data Weather ETL

Projet ETL de données météo TP Big Data.

## Architecture

```
Open Meteo API -> MongoDB -> Spark -> PostgreSQL -> Grafana
                     |                     |
                  Airflow             Prometheus
```

## Stack technique

| Composant   | Role                              |
|-------------|-----------------------------------|
| Airflow     | Orchestration des pipelines       |
| MongoDB     | Data lake (donnees brutes)        |
| Spark       | Transformation des donnees        |
| PostgreSQL  | Data warehouse (donnees traitees) |
| Prometheus  | Collecte des metriques            |
| Grafana     | Visualisation et monitoring       |

## Donnees collectees

Donnees meteo horaires pour les villes d'Occitanie via Open Meteo API :

- Temperature (2m)
- Precipitations
- Vitesse et direction du vent (10m)
- Humidite relative (2m)
- Pression atmospherique
- Temperature et humidite du sol
- Couverture nuageuse

## Metriques Grafana

Dashboard de monitoring du pipeline :

- Nombre de documents bruts (MongoDB)
- Nombre de lignes traitees (PostgreSQL)
- Ratio de traitement
- Volume de donnees par ville
- Evolution du volume dans le temps
- Erreurs de collecte

## Lancement

```bash
docker compose up -d
```

## URLs des services

| Service       | URL                       |
|---------------|---------------------------|
| Airflow       | http://localhost:8080     |
| Mongo Express | http://localhost:8081     |
| Spark Master  | http://localhost:8082     |
| Prometheus    | http://localhost:9090     |
| Grafana       | http://localhost:3000     |

## Structure du projet

```
├── dags/              # DAGs Airflow
├── spark/jobs/        # Jobs Spark
├── config/            # Config Grafana/Prometheus
├── metrics-exporter/  # Exporteur custom
└── docker-compose.yml
```
