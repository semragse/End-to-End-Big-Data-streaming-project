# Mini-projet 09 - Visualiser PostgreSQL avec Grafana

Objectif : connecter Grafana a PostgreSQL et creer un dashboard pour explorer les donnees transformees.

## Questions de comprehension

1. Pourquoi Grafana lit-il PostgreSQL au lieu de lire Kafka directement ?
2. C'est quoi une datasource Grafana ?
3. Pourquoi le serveur PostgreSQL s'appelle `postgres` dans Grafana ?
4. Pourquoi utilise-t-on le port `13000` dans le navigateur ?
5. Quelle est la difference entre Adminer et Grafana ?
6. Pourquoi un dashboard est utile dans un projet Data Engineering ?
7. Quelles metriques e-commerce aimerais-tu ajouter ensuite ?

## Schema de cette etape

```text
PostgreSQL table: order_items
      |
      v
Grafana datasource: Ecommerce PostgreSQL
      |
      v
Dashboard: Ecommerce Streaming Overview
```

## Acces Grafana

```text
URL      : http://localhost:13000
Username : admin
Password : admin
```

## Mini-exercice

Explique avec tes mots pourquoi `localhost:13000` ouvre Grafana, mais la datasource Grafana utilise `postgres:5432`.
