# Mini-projet 04 - Lire Kafka avec un consumer Python

Objectif : remplacer le consumer du terminal par un vrai script Python.

## Questions de comprehension

1. C'est quoi un consumer Kafka ?
2. Pourquoi le consumer lit depuis un topic et non directement depuis le producer ?
3. C'est quoi un offset ?
4. Pourquoi utilise-t-on `auto.offset.reset = earliest` dans ce test ?
5. Pourquoi le message Kafka est-il stocke en bytes avant d'etre transforme en JSON Python ?
6. Quel est le role du `group.id` ?
7. Pourquoi `confluent-kafka` est-il plus professionnel que `kafka-python` dans notre environnement ?

## Schema de cette etape

```text
Kafka topic: ecommerce_orders
      |
      v
Python consumer
      |
      v
Affichage terminal
```

## Mini-exercice

Explique avec tes mots ce que signifie cette ligne affichee par le consumer :

```text
Offset   : 0
```
