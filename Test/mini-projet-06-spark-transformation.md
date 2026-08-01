# Mini-projet 06 - Nettoyage et transformation avec Spark

Objectif : transformer le message JSON brut lu depuis Kafka en colonnes propres exploitables par PostgreSQL.

## Questions de comprehension

1. Pourquoi le message Kafka arrive-t-il d'abord comme une chaine JSON ?
2. A quoi sert un schema Spark ?
3. Pourquoi utilise-t-on `from_json` ?
4. Pourquoi utilise-t-on `explode` sur la liste `products` ?
5. Pourquoi obtient-on 4 lignes pour une seule commande ?
6. Pourquoi l'ancien message manuel avec `user_id: 101` n'apparait plus dans le resultat transforme ?
7. Pourquoi les noms de colonnes comme `user_id` ou `product_title` sont-ils plus pratiques pour PostgreSQL ?

## Schema de cette etape

```text
Kafka message JSON brut
      |
      v
Spark from_json + schema
      |
      v
Commande structuree
      |
      v
explode(products)
      |
      v
Une ligne par produit commande
```

## Resultat attendu

Pour une commande contenant 4 produits, Spark affiche 4 lignes :

```text
order_id | user_id | product_id | product_title       | product_quantity
1        | 1       | 162        | Blue Frock          | 4
1        | 1       | 113        | Generic Motorcycle  | 3
1        | 1       | 122        | iPhone 6            | 3
1        | 1       | 138        | Baseball Ball       | 2
```

## Mini-exercice

Explique avec tes mots pourquoi une seule commande peut devenir plusieurs lignes apres transformation.
