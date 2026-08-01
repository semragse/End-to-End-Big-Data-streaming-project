# Mini-projet 08 - Ecrire les donnees transformees dans PostgreSQL

Objectif : prendre les lignes propres produites par Spark et les inserer dans la table PostgreSQL `order_items`.

## Questions de comprehension

1. Pourquoi Spark ecrit-il dans PostgreSQL apres la transformation ?
2. Pourquoi utilise-t-on `foreachBatch` au lieu d'un simple affichage console ?
3. C'est quoi un micro-batch Spark ?
4. Pourquoi utilise-t-on `ON CONFLICT DO NOTHING` ?
5. Pourquoi la table contient-elle 4 lignes pour une seule commande ?
6. Pourquoi `kafka_partition`, `kafka_offset` et `product_id` permettent-ils d'eviter les doublons ?
7. Pourquoi PostgreSQL est-il plus adapte que Kafka pour faire des requetes SQL et un dashboard ?

## Schema de cette etape

```text
Kafka topic: ecommerce_orders
      |
      v
Spark Structured Streaming
      |
      v
Transformation JSON -> colonnes
      |
      v
foreachBatch
      |
      v
PostgreSQL table: order_items
```

## Resultat attendu

```text
order_items
├── Blue Frock
├── Generic Motorcycle
├── iPhone 6
└── Baseball Ball
```

## Mini-exercice

Explique avec tes mots pourquoi Kafka garde les messages, mais PostgreSQL garde les donnees propres pour l'analyse.
