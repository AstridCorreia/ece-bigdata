# Lab 10 - Trino : couche gold en schéma en étoile

Lancé dans la même session Python que le lab 9 (`labs/lab-9/trino_conn.py`), avec la même connexion Trino.

## Gold layer with star schema

Tables créées avec `CREATE TABLE AS SELECT` dans `lakehouse.gold` :
- `dim_user` : 50 lignes, clé `user_key` générée avec `row_number()`
- `dim_product` : 6 lignes, clé `product_key` générée avec `row_number()`
- `dim_date` : 118 lignes, une par jour entre la première et la dernière commande, clé `date_key` au format `YYYYMMDD`
- `fact_orders` : 2829 lignes, comme `silver.orders`

La requête sur l'étoile (année, État, produit) marche, et la reconstruction de `orders_by_state_month` à partir de `fact_orders`, `dim_user` et `dim_date` redonne les mêmes chiffres que la table gold du lab 9.

## Validation

Les 3 assertions passent : 2829 commandes et 8615 unités dans `fact_orders` comme dans `silver.orders`, et 0 commande sans ligne de dimension.

### Ces vérifications suffisent-elles à garantir que chaque commande est bien représentée dans `fact_orders` ? Exemple de défaut non détecté, et vérifications à ajouter
Non. Elles vérifient les totaux et que chaque clé existe dans sa dimension, mais pas que la clé pointe vers la bonne ligne. Par exemple, si deux commandes avaient leurs `user_key` inversées, ou si `date_key` était calculée dans un autre fuseau horaire (une commande à 23h30 rangée le jour suivant), le nombre de lignes, la somme des quantités et les jointures seraient toujours bons. Il faudrait ajouter :
- une comparaison ligne à ligne : joindre `fact_orders` aux dimensions puis à `silver.orders` sur `order_id`, et vérifier que `user_id`, `product` et `CAST(ordered_at AS date)` sont les mêmes que dans silver
- l'unicité de `order_id` dans `fact_orders`
- l'unicité de `user_id` dans `dim_user` et de `product` dans `dim_product` (une seule ligne par valeur métier)

### Quel est le grain de `fact_orders` ? De `dim_date` ?
`fact_orders` : une ligne par commande, le même grain que `silver.orders`. `dim_date` : une ligne par jour calendaire.

### Quelle assertion échoue si `silver.users` contient deux fois le même `user_id` ? Laquelle échoue si une commande n'a pas de produit (`NULL`) ?
Avec un `user_id` en double, `dim_user` a deux lignes pour ce client avec deux `user_key` différentes. Le `LEFT JOIN` duplique alors toutes ses commandes, et c'est `silver_orders == fact_orders` qui échoue (et aussi celle des unités). Avec un produit à `NULL`, le `LEFT JOIN` ne trouve pas de `product_key`, la commande est gardée avec une clé `NULL` : les totaux sont bons, mais l'assertion `unmatched == 0` échoue.

### Les clés sont générées avec `row_number()`. Que devient `user_key` si on reconstruit `dim_user` après l'ajout d'un utilisateur ? Conséquence pour `fact_orders` ?
Les clés sont attribuées dans l'ordre des `user_id`. Si le nouvel utilisateur se place au milieu de cet ordre, tous les utilisateurs après lui changent de `user_key`. Si on ne reconstruit pas `fact_orders` en même temps, les anciennes commandes pointent vers les mauvais utilisateurs. Il faut donc toujours reconstruire la table de faits avec les dimensions, ou utiliser des clés stables (par exemple garder les clés existantes et donner `max(user_key) + 1` aux nouveaux, ou une clé calculée à partir de `user_id`).

### `dim_user` écrase les attributs d'un utilisateur. S'il déménage dans un autre État, quel État montrent ses anciennes commandes ? Que faudrait-il pour garder l'historique ?
Elles montrent le nouvel État, car il n'y a qu'une ligne par utilisateur, avec ses valeurs actuelles : l'historique des ventes par État change après coup. Pour garder l'historique, il faut une dimension à évolution lente de type 2 : une nouvelle ligne (avec une nouvelle `user_key`) à chaque changement, avec des colonnes `valid_from`, `valid_to` et `is_current`. La table de faits prend la clé de la version valide à la date de la commande.

### Comparer `orders_by_state_month` et la requête sur l'étoile qui la reconstruit. Quelles questions chacune peut-elle traiter sans nouvelle table, et que coûte l'étoile à la requête ?
`orders_by_state_month` répond seulement aux questions par État et par mois. Elle est déjà agrégée (50 lignes), donc très rapide à lire, mais pour une autre question (par produit, par jour de la semaine, par ville...) il faut créer une nouvelle table. L'étoile répond à n'importe quelle combinaison des dimensions (année, trimestre, week-end, produit, ville, date de naissance...) sans nouvelle table. En échange, chaque requête doit relire toute la table de faits, faire les jointures avec les dimensions puis l'agrégation, ce qui coûte plus cher au moment de la requête quand la table de faits devient grosse.

## Commit

Les métadonnées des tables gold sont copiées depuis le bucket dans `lab_trino/gold/` (sans les dossiers `data/`) et committées.
