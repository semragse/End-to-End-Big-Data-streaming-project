# Mini-projet 05 - Connecter Spark Structured Streaming a Kafka

Objectif : faire lire le topic Kafka `ecommerce_orders` par Spark Structured Streaming et afficher les messages dans la console.

## Questions de comprehension

1. Pourquoi Spark lit-il Kafka au lieu de lire directement l'API ?
2. C'est quoi un DataFrame streaming ?
3. Pourquoi Kafka envoie `value` sous forme de bytes ?
4. Pourquoi fait-on `col("value").cast("string")` ?
5. C'est quoi un micro-batch dans Spark Structured Streaming ?
6. Pourquoi utilise-t-on `startingOffsets = earliest` dans ce test ?
7. Pourquoi Spark a besoin d'un connecteur Kafka externe ?

## Schema de cette etape

```text
Kafka topic: ecommerce_orders
      |
      v
Spark Structured Streaming
      |
      v
Console sink
```

## Mini-exercice

Explique avec tes mots pourquoi Spark affiche :

```text
Batch: 0
```

Indice : Spark Structured Streaming traite le streaming comme une suite de petits traitements continus.
