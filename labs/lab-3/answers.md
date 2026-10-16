# TP 3 — DuckDB et Parquet

## 1. Requêtes SQL

### 1.1 Moyennes

Le nombre moyen de commandes par utilisateur est de **56,58**. La quantité moyenne par commande est d'environ **3,045 produits**.

### 1.2 Première et dernière commande

Pour chaque utilisateur, on récupère la date de sa première commande et celle de sa dernière commande. On calcule ensuite le nombre de jours entre les deux dates.

D'après les résultats obtenus, la différence ne dépasse pas 5 jours.

### 1.3 Quantités selon l'heure

L'heure avec la plus grande quantité commandée est **6 h**, avec 385 unités pour 118 commandes. La deuxième heure est 15 h, avec 382 unités.

### 1.4 Évolution des ventes avec LAG

La fonction `LAG` permet de comparer les quantités vendues avec celles du mois précédent.

| Produit | Janvier | Février | Mars | Avril |
|---|---:|---:|---:|---:|
| bread | 315 | 362 (+14,92 %) | 411 (+13,54 %) | 332 (-19,22 %) |
| brioche | 431 | 350 (-18,79 %) | 333 (-4,86 %) | 310 (-6,91 %) |
| cookie | 376 | 371 (-1,33 %) | 340 (-8,36 %) | 278 (-18,24 %) |
| croissant | 360 | 392 (+8,89 %) | 385 (-1,79 %) | 386 (+0,26 %) |
| donut | 438 | 314 (-28,31 %) | 321 (+2,23 %) | 317 (-1,25 %) |
| drink | 377 | 365 (-3,18 %) | 402 (+10,14 %) | 349 (-13,18 %) |

On remarque que les ventes de pain augmentent jusqu'en mars, puis baissent en avril. Les ventes de cookies diminuent chaque mois après janvier. Les croissants restent plutôt stables.

### 1.5 Utilisateurs ayant commandé tous les produits

**45 utilisateurs sur 50** ont commandé les six produits, soit 90 % des utilisateurs.

## 2. Export Parquet et partitionnement

J'ai exporté les commandes au format Parquet sur S3, en créant un dossier par produit.

Quand on cherche un produit précis, DuckDB peut lire uniquement le fichier correspondant au lieu de lire les six fichiers. Le test avec `EXPLAIN ANALYZE` a confirmé que seul 1 fichier sur 6 était lu.

Partitionner par UUID serait une mauvaise idée, car presque chaque commande possède un UUID différent. Cela créerait beaucoup trop de petits fichiers.

Pour des commandes qui augmentent chaque jour, je choisirais plutôt la date, par exemple une partition par mois. Cela permettrait de retrouver plus facilement les commandes d'une période donnée.

## 3. CSV et Parquet à grande échelle

### 3.1 Nombre de commandes

Le fichier généré avec 5 000 utilisateurs contient **252 416 commandes**. Le TP donne un exemple avec 999 490 commandes, mais les résultats ci-dessous correspondent au fichier que j'ai obtenu.

### 3.2 Taille des fichiers

| Format | Taille |
|---|---:|
| CSV | 29 574 123 octets (environ 29,6 Mo) |
| Parquet | 11 532 863 octets (environ 11,5 Mo) |

Le fichier Parquet est environ **61 % plus petit** que le CSV. Il prend donc moins de place sur S3.

### 3.3 Temps de lecture

| Requête | Données lues | Temps |
|---|---:|---:|
| Agrégation CSV | 28,2 MiB | 1,18 s |
| Agrégation Parquet | 203,5 KiB | 0,345 s |
| Filtre sur la date en Parquet | 16,0 KiB | 0,299 s |

L'agrégation sur Parquet est environ **3,4 fois plus rapide** que celle sur CSV dans mes tests.

Avec le CSV, DuckDB doit télécharger et analyser le texte du fichier. Avec Parquet, il peut lire seulement les colonnes utiles et les données sont compressées. Cela réduit les transferts réseau et le temps de traitement. La mesure du temps total ne permet toutefois pas de séparer précisément ces deux effets.

### 3.4 Métadonnées et filtre sur les dates

Le fichier Parquet contient trois groupes de lignes. Les dates observées vont de 2020 à 2048 environ.

Le filtre `date >= '2100-01-01'` renvoie **0 ligne**, car les dates du fichier sont toutes antérieures à 2100. DuckDB peut utiliser les dates minimales et maximales stockées dans les métadonnées pour éviter de lire les groupes qui ne correspondent pas au filtre.

Si les commandes étaient mélangées au hasard, les dates seraient réparties dans davantage de groupes. DuckDB aurait alors plus de mal à ignorer certains groupes et le filtre pourrait être moins efficace.

## 4. Questions sur les données et la sécurité

### 4.1 Secret S3

Le fichier du secret contient des informations qui permettent d'accéder à S3. Si quelqu'un peut lire ce fichier, il peut essayer d'utiliser ces informations pour accéder aux données.

Sur Onyxia, le risque est limité par l'isolation des environnements et par l'utilisation d'identifiants temporaires. Mais un autre processus exécuté avec le même utilisateur peut aussi accéder au fichier.

Pour un Job Kubernetes, je donnerais uniquement les droits S3 nécessaires au travail demandé. J'utiliserais un mécanisme d'identité ou un secret Kubernetes avec des accès limités, sans mettre les identifiants dans le code ou dans Git.

### 4.2 Approximation et types des colonnes

`approx_unique` donne une estimation du nombre de valeurs différentes. Le résultat n'est donc pas forcément exact : il indique par exemple 40 utilisateurs au lieu de 50, et 3167 UUID au lieu de 2829.

DuckDB détecte les types des colonnes en lisant un échantillon du CSV. Si les premières lignes ne représentent pas bien le reste du fichier, certaines valeurs peuvent être mal interprétées ou provoquer des erreurs. On peut alors définir les types des colonnes manuellement.

### 4.3 Compression Parquet

Les UUID sont presque tous différents et difficiles à compresser. À l'inverse, une colonne avec peu de valeurs différentes, comme le produit, se compresse mieux.

Certaines colonnes peuvent être légèrement plus grosses après compression, car le format doit aussi stocker des informations supplémentaires. Sur de petites colonnes, ce coût peut être supérieur au gain de compression.

## 5. Python

J'ai créé un script Python qui produit un rapport mensuel et permet de filtrer les résultats par produit.

Il fonctionne avec les données Parquet partitionnées et renvoie un DataFrame. Le TP demande plutôt de lire `bronze/orders.csv` et d'afficher le mois, le nombre de commandes et la quantité totale. Mon script est donc une version un peu différente de celle demandée.

## Conclusion

Ce TP m'a permis de faire des requêtes SQL avec DuckDB, de stocker les données en Parquet sur S3 et de comparer les performances avec le CSV.

Dans mes tests, Parquet prend environ 61 % de place en moins et l'agrégation est environ 3,4 fois plus rapide. Le partitionnement permet aussi d'éviter de lire les fichiers qui ne sont pas utiles.
