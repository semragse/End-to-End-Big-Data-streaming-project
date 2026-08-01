# Mini-projet 07 - Creer les tables PostgreSQL

Objectif : creer une base PostgreSQL et une table `order_items` pour recevoir les donnees nettoyees par Spark.

## Questions de comprehension

1. Pourquoi stocke-t-on les donnees transformees dans PostgreSQL ?
2. Pourquoi ne stocke-t-on pas directement le JSON brut dans une seule colonne ?
3. Pourquoi une commande avec 4 produits devient-elle 4 lignes dans `order_items` ?
4. A quoi sert une cle primaire ?
5. Pourquoi utilise-t-on `(kafka_partition, kafka_offset, product_id)` comme cle primaire ?
6. A quoi sert le volume `postgres_data` ?
7. Pourquoi PostgreSQL est expose sur le port local `5433` au lieu de `5432` ?

## Schema de cette etape

```text
Spark transformed rows
      |
      v
PostgreSQL database: ecommerce
      |
      v
Table: order_items
```

## Table creee

```text
order_items
├── kafka_topic
├── kafka_partition
├── kafka_offset
├── kafka_timestamp
├── order_id
├── user_id
├── order_total
├── product_id
├── product_title
├── product_price
├── product_quantity
└── inserted_at
```

## Mini-exercice

Explique avec tes mots pourquoi on garde `kafka_partition` et `kafka_offset` dans PostgreSQL.
