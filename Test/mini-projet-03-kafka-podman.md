# Mini-projet 03 - Kafka avec Podman

Objectif : lancer Kafka localement avec Podman, creer un topic, envoyer un premier message, puis le lire.

## Questions de comprehension

1. C'est quoi Kafka ?
2. C'est quoi un broker Kafka ?
3. C'est quoi un topic ?
4. Pourquoi un producer envoie un message dans un topic au lieu de l'envoyer directement au consumer ?
5. Quel probleme Kafka resout-il dans notre pipeline e-commerce ?
6. Pourquoi utilise-t-on Podman ici ?
7. Quelle est la difference entre une image et un conteneur ?

## Schema de cette etape

```text
Terminal producer
      |
      v
Kafka topic: ecommerce_orders
      |
      v
Terminal consumer
```

## Mini-exercice

Explique avec tes mots le trajet du premier message Kafka.
