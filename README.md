# Big Data Weather ETL

Projet ETL de données météo TP Big Data.

## Architecture

```
   Open Meteo API
         |
      Airflow
      |     \
   MongoDB -> Spark -> PostgreSQL
                           |
                        Prometheus -> Grafana
```

## Stack technique

| Composant   | Role                              |
|-------------|-----------------------------------|
| Airflow     | Orchestration des pipelines       |
| MongoDB     | Data lake (données brutes)        |
| Spark       | Transformation des données        |
| PostgreSQL  | Data warehouse (données traitées) |
| Prometheus  | Collecte des métriques            |
| Grafana     | Visualisation et monitoring       |

## Données collectées

Les données colléctées pour les villes d'Occitanie sont les suivantes :

- Température (2m)
- Précipitations
- Vitesse et direction du vent (10m)
- Humidité relative (2m)
- Pression atmosphérique
- Température et humidité du sol
- Couverture nuageuse

## Métriques Grafana

Dashboard de monitoring :

- Nombre de documents bruts (MongoDB)
- Nombre de lignes traitées (PostgreSQL)
- Ratio de traitement
- Volume de données par ville
- Évolution du volume dans le temps
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