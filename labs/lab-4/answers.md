# Lab 4 - Bronze to Silver avec dbt et DuckDB

## Bronze layer

### Quel est l'intérêt de laisser les CSV bronze intacts si on trouve un bug dans une transformation six mois plus tard ?
On a toujours les données brutes d'origine. Il suffit de corriger le modèle et de relancer dbt pour reconstruire la couche silver, sans avoir à récupérer les données une deuxième fois à la source.

### La source relit tout le fichier à chaque requête. Quel format et quelle organisation choisir si les commandes arrivaient toutes les heures pendant des années ?
Du Parquet partitionné par date (par exemple un dossier par jour ou par mois). On ne lit que les colonnes utiles, les fichiers sont compressés, et avec un filtre sur la date on ne lit que les partitions concernées. On peut aussi utiliser un modèle incrémental dbt pour ne traiter que les nouvelles données.

## Build

### Les modèles silver sont des vues. Que se passe-t-il quand on interroge une vue, et quand est-elle préférable à une table ?
Une vue ne stocke pas les données, seulement la requête. À chaque fois qu'on l'interroge, DuckDB relit les CSV sur S3 et refait toute la transformation. C'est bien quand les données sont petites, changent souvent ou sont peu interrogées, car on a toujours la dernière version et rien n'est stocké en double. Une table est mieux quand on l'interroge souvent ou que la transformation est lourde.

### Un UUID est stocké sur 16 octets. Combien d'octets utilise sa représentation en VARCHAR ?
36 octets : 32 caractères hexadécimaux et 4 tirets, 1 octet par caractère. C'est plus de 2 fois plus que le type UUID.

## Data tests

### Résultats
Les 14 tests génériques et le test `assert_orders_have_users` passent : toutes les commandes pointent vers un utilisateur existant, il n'y avait donc pas d'échec à analyser avec `--store-failures`.
Le test `assert_orders_after_user_birth` échoue avec 288 lignes : les commandes datent de 2020, mais Faker génère les dates de naissance par rapport à la date d'aujourd'hui, donc certains utilisateurs sont "nés" après leur première commande. Avec `severity='warn'`, le test passe en avertissement : PASS=14, WARN=1, ERROR=0.

### Quelle stratégie choisir pour un dashboard sur l'âge des clients ? Pour un dashboard des ventes par région ?
Pour l'âge, je remplacerais les dates de naissance invalides par NULL (ou j'exclurais ces clients), sinon on affiche des âges faux, voire négatifs. Pour les ventes par région, la date de naissance n'est pas utilisée, donc on garde toutes les commandes et on laisse le test en warning pour surveiller le problème.

### Les adresses associent des États à des codes postaux aléatoires (KY 01352 par exemple). Quel test le détecterait, et avec quelles données de référence ?
Un test singulier qui vérifie que le code postal correspond bien à l'État. Il faut une table de référence qui donne, pour chaque État, les plages ou les préfixes de codes postaux valides (par exemple les 3 premiers chiffres, publiés par l'USPS). Le test renvoie les utilisateurs dont le préfixe du code postal n'est pas dans la plage de leur État.

### Note
Le `silver.yml` de l'énoncé fait référence à `ref('states')`, qui n'existait pas dans le projet. J'ai d'abord essayé de le créer comme seed, mais le chargement du seed plantait avec dbt-duckdb. Je l'ai donc créé comme modèle SQL dans `models/reference/states.sql`, avec la liste des codes des États, de DC, des territoires et des régions militaires (AA, AE, AP).
MD

## Export de la couche silver en Parquet

`stg_users` et `stg_orders` ont été exportés sur S3 dans `silver/dbt/users.parquet` (7 Ko) et `silver/dbt/orders.parquet` (72 Ko). On peut les interroger directement avec `read_parquet`, par exemple 30 femmes et 20 hommes dans `users.parquet`.

### Exercice : comparer `orders.parquet` avec `main_silver.stg_orders`
Les deux contiennent 2829 lignes, et un `EXCEPT` dans les deux sens renvoie 0 ligne : le fichier Parquet contient exactement les mêmes données que la vue.
La différence, c'est que la vue refait la transformation à partir du CSV bronze à chaque requête (matérialisation logique), alors que le Parquet stocke le résultat une fois pour toutes (matérialisation physique). Si le CSV bronze change, la vue le voit tout de suite, mais le Parquet reste à l'ancienne version tant qu'on ne refait pas l'export.
MD