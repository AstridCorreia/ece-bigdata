# TP 3 — Analyse de données avec DuckDB et Parquet

## 1. Analyse SQL des commandes

### 1.1 Moyennes

La quantité moyenne par commande est d'environ **3,045 unités**.

Le nombre moyen de commandes par utilisateur est de **56,58**.

Ces moyennes donnent une vue générale de l'activité des utilisateurs et du volume de produits commandés.

### 1.2 Première et dernière commande par utilisateur

Pour chaque utilisateur, on recherche la date de sa première commande, la date de sa dernière commande et le nombre de jours séparant ces deux dates.

Cette analyse permet d'observer la durée pendant laquelle chaque utilisateur a passé des commandes. Dans les résultats obtenus, l'écart entre la première et la dernière commande ne dépassait pas 5 jours.

La requête SQL utilisée doit calculer les dates minimale et maximale par utilisateur, puis la différence en jours entre ces deux dates.

### 1.3 Quantités commandées selon l'heure

L'heure où la quantité totale commandée était la plus élevée est **6 h**, avec **385 unités réparties sur 118 commandes**.

La deuxième heure la plus élevée était **15 h**, avec 382 unités.

Cela indique que, dans ce jeu de données, l'activité est particulièrement importante à 6 h. Ce résultat décrit uniquement les données analysées et ne doit pas être généralisé sans autre étude.

### 1.4 Évolution mensuelle par produit avec LAG

La fonction SQL `LAG` permet de récupérer la valeur du mois précédent afin de calculer l'évolution mensuelle.

Les pourcentages ci-dessous correspondent à la variation des quantités entre deux mois consécutifs.

| Produit | Janvier | Février | Mars | Avril |
|---|---:|---:|---:|---:|
| bread | 315 | 362 (+14,92 %) | 411 (+13,54 %) | 332 (-19,22 %) |
| brioche | 431 | 350 (-18,79 %) | 333 (-4,86 %) | 310 (-6,91 %) |
| cookie | 376 | 371 (-1,33 %) | 340 (-8,36 %) | 278 (-18,24 %) |
| croissant | 360 | 392 (+8,89 %) | 385 (-1,79 %) | 386 (+0,26 %) |
| donut | 438 | 314 (-28,31 %) | 321 (+2,23 %) | 317 (-1,25 %) |
| drink | 377 | 365 (-3,18 %) | 402 (+10,14 %) | 349 (-13,18 %) |

Le pain (`bread`) augmente entre janvier et mars, puis diminue en avril. Les quantités de brioches (`brioche`) et de cookies (`cookie`) diminuent sur l'ensemble de la période après janvier. Les croissants restent relativement stables entre février et avril.

### 1.5 Utilisateurs ayant commandé tous les produits

**45 utilisateurs sur 50** ont commandé chacun des six produits.

Cela représente 90 % des utilisateurs analysés. Les cinq autres utilisateurs n'ont pas commandé au moins un des produits.

## 2. Export et partitionnement Parquet

Les données ont été exportées au format Parquet et partitionnées par produit dans le dossier S3 `analytics/orders_by_product/`.

Le partitionnement crée une organisation par valeur de produit. Lorsqu'une requête ne concerne qu'un produit, DuckDB peut éviter de lire les fichiers des autres produits.

Le test avec `EXPLAIN ANALYZE` a confirmé que DuckDB ne scannait qu'un fichier sur les six disponibles pour une requête filtrée sur un produit. Cela illustre le *partition pruning*, qui réduit les données à lire et peut accélérer les requêtes.

## 3. Comparaison entre CSV et Parquet à grande échelle

### 3.1 Génération et nombre de lignes

Le jeu de données généré avec 5 000 utilisateurs contenait **252 416 commandes** selon le comptage effectué sur le fichier Parquet.

Ce nombre est inférieur aux quelque 999 490 commandes mentionnées dans l'exemple du TP. Les mesures ci-dessous correspondent donc au jeu de données effectivement généré dans cet environnement.

### 3.2 Comparaison des tailles

| Format | Taille sur S3 |
|---|---:|
| CSV | 29 574 123 octets (environ 29,6 Mo) |
| Parquet | 11 532 863 octets (environ 11,5 Mo) |

Le fichier Parquet est environ **61 % plus petit** que le CSV, soit une économie d'environ 18 Mo.

Parquet est un format en colonnes qui prend en charge la compression et permet de ne lire que les colonnes nécessaires à une requête. CSV est un format texte qui doit être analysé pour interpréter les valeurs.

### 3.3 Comparaison des temps d'exécution

Les tests ont été réalisés avec le cache des fichiers externes désactivé.

| Requête | Données lues | Temps total |
|---|---:|---:|
| Agrégation depuis le CSV | 28,2 MiB | 1,18 s |
| Agrégation depuis le Parquet | 203,5 KiB | 0,345 s |
| Comptage avec filtre de date sur Parquet | 16,0 KiB | 0,299 s |

Pour l'agrégation testée, Parquet a été environ **3,4 fois plus rapide** que CSV.

La différence s'explique notamment par la compression, le stockage en colonnes et la possibilité de ne lire que les colonnes utiles. Les temps observés dépendent cependant de l'environnement, du réseau et de la taille des données.

### 3.4 Métadonnées et groupes de lignes

L'inspection des métadonnées de `orders.parquet` a montré trois groupes de lignes, avec les plages de dates suivantes :

- Groupe 0 : de janvier 2020 à février 2034.
- Groupe 1 : de février 2034 à avril 2048.
- Groupe 2 : d'avril 2048 à octobre 2048.

Les métadonnées Parquet comprennent notamment les valeurs minimales et maximales de certaines colonnes pour chaque groupe de lignes. DuckDB peut utiliser ces statistiques pour ignorer les groupes qui ne peuvent pas satisfaire un filtre.

### 3.5 Filtrage sur les dates

La requête filtrant les commandes dont la date est supérieure ou égale au 1er janvier 2100 a renvoyé **0 ligne**.

Toutes les dates maximales observées dans les groupes étaient antérieures à 2100. DuckDB peut donc éviter de lire les données de ces groupes pour ce filtre. Cette optimisation est appelée *row-group pruning*.

Lorsque les données sont ordonnées par date, les plages min/max des groupes peuvent être plus sélectives. Si les dates sont mélangées, davantage de groupes peuvent contenir des dates qui se chevauchent et l'optimisation peut être moins efficace.

## 4. Conclusion

Ce TP a permis d'utiliser DuckDB pour analyser des commandes avec SQL, d'exporter les données vers Parquet, de les partitionner sur S3 et de mesurer les performances de requêtes.

Les tests réalisés montrent que Parquet occupe environ 61 % d'espace en moins que le CSV et que l'agrégation testée est environ 3,4 fois plus rapide. Le partitionnement et les statistiques des groupes de lignes permettent également de réduire les données lues.

Ces résultats illustrent l'intérêt de Parquet pour les traitements analytiques. Les performances mesurées restent propres au jeu de données et à l'environnement utilisés.
