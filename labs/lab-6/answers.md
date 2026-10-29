# Lab 6 - Bronze to silver avec les DataFrames Spark

Le code exécuté est dans `labs/lab-6/spark_silver.py` (lancé dans le shell PySpark avec `exec(open(...).read())`, parce que coller les cellules une par une dans le terminal du navigateur coupait les lignes trop longues).

## SparkSession with S3 access

### Les identifiants sont temporaires. Que se passe-t-il pour un job Spark en cours si le service est redémarré au milieu du lab, et pourquoi ?
Le job est perdu : Spark tourne en mode `local[*]` dans le service, donc le driver et les executors sont tués avec lui. Au redémarrage, Onyxia donne de nouveaux identifiants, il faut relancer la session et tout recalculer (les DataFrames ne sont que des plans en mémoire). Et même sans redémarrage, les identifiants sont lus une seule fois au démarrage de la session : s'ils expirent pendant un long job, les accès S3 commencent à échouer avec des erreurs 403.

### Pourquoi `SimpleAWSCredentialsProvider` ne marche pas ici, alors qu'il marchait sans session token ?
Il n'utilise que l'access key et la secret key. Les identifiants d'Onyxia sont des identifiants STS temporaires, qui ne sont valides qu'avec le session token. Sans le token, S3 refuse la requête (403). `TemporaryAWSCredentialsProvider` envoie les trois.

## Silver layer

Résultats : 2829 commandes, 2832 après avoir ajouté 3 doublons, et 2829 après la déduplication avec `row_number()`. La version Spark SQL donne le même résultat que la version DataFrame.

### Expliquer le résultat de la requête par État
La requête compte les utilisateurs par État et combien n'ont pas de ville. Les utilisateurs sont répartis au hasard sur beaucoup d'États (3 au maximum par État). Les seuls qui ont des villes manquantes sont AE (3 sur 3) et AP (2 sur 2) : ce sont des adresses militaires du type "DPO AP 09617", qui n'ont pas de ville, donc la regex ne trouve rien et `city` est à NULL.

### DataFrame ou SQL : lequel choisir pour une exploration rapide, et lequel pour un modèle réutilisé par plusieurs collègues ?
Pour une exploration rapide, je prendrais Spark SQL : c'est plus rapide à écrire et tout le monde sait lire du SQL. Pour un modèle réutilisé, je prendrais l'API DataFrame : on peut découper le code en fonctions Python, le tester, le réutiliser et les erreurs sont visibles plus tôt dans l'éditeur. Les deux donnent de toute façon le même plan Catalyst, donc ce n'est pas une question de performance.

### `regexp_extract()` renvoie une chaîne vide au lieu de NULL. Où ailleurs ce comportement pourrait donner un résultat faux sans qu'on le voie ?
Sur `state` et `zip_code`, qui sont aussi extraits avec `regexp_extract()` et qu'on n'a pas convertis en NULL. Si une adresse ne correspondait pas au format, on aurait un État "" au lieu de NULL : il apparaîtrait comme un groupe à part dans un `groupBy("state")`, et un test "not null" passerait alors qu'il manque la valeur.

## Write the silver layer as Parquet

### Combien de part-files sous `users.parquet/`, et pourquoi ?
Un seul `part-00000-...snappy.parquet` par table, plus un fichier `_SUCCESS`. Spark écrit un fichier par partition, et les données sont petites : le CSV est lu en une seule partition (avec `multiLine=True` le fichier ne peut pas être découpé), et après le shuffle de la déduplication, Spark (AQE) regroupe les petites partitions en une seule.

### `coalesce(1)` avant l'écriture donnerait un seul fichier. Qu'est-ce qu'on perd sur un plus gros dataset ?
Le parallélisme : une seule tâche doit écrire toutes les données, donc c'est lent et ça peut manquer de mémoire, pendant que les autres cœurs ne font rien. Les lecteurs ont aussi moins de facilité à lire le fichier en parallèle.

## Write the silver layer as Iceberg tables

La table `lab.silver.orders` a 1 snapshot et 1 fichier de données (2829 lignes, 84 Ko). La relecture des tables Iceberg donne bien 50 utilisateurs et 2829 commandes.

### Comparer `silver/spark/orders.parquet/` et `warehouse/silver/orders/`. Quels fichiers contiennent les données et lesquels décrivent la table ?
`silver/spark/orders.parquet/` ne contient que le fichier de données et `_SUCCESS`. Dans `warehouse/silver/orders/`, les données sont dans `data/` (fichiers Parquet), et `metadata/` décrit la table : `v1.metadata.json` (schéma, partitionnement, liste des snapshots), `snap-....avro` (la manifest list du snapshot), `...-m0.avro` (le manifest qui liste les fichiers de données avec leurs stats) et `version-hint.text` (le numéro de la version courante).

### Un 2e job écrase le dossier Parquet pendant qu'une requête le lit. Que voit le lecteur ? Qu'est-ce qui change avec Iceberg ?
Avec Parquet, l'écriture en mode overwrite supprime les anciens fichiers puis écrit les nouveaux : le lecteur peut voir un mélange des deux, un dossier à moitié vide ou planter avec un fichier introuvable. Avec Iceberg, les nouveaux fichiers sont écrits à côté, puis le commit change le pointeur vers la nouvelle version des métadonnées en une seule fois. Le lecteur continue de lire le snapshot qu'il avait au début, qui reste complet et cohérent.

### Ici, le pointeur vers la version courante est un fichier dans le bucket. Que peut-il se passer si deux jobs commitent en même temps, et qu'apporterait un service de catalogue ?
Les deux jobs peuvent écrire une nouvelle version des métadonnées en même temps et mettre à jour `version-hint.text` : S3 n'a pas de renommage atomique, donc un des deux commits peut écraser l'autre et ses données sont perdues. Un service de catalogue (REST, Nessie, Hive Metastore, Glue...) fait le changement de pointeur de façon atomique (compare-and-swap) : le 2e commit est refusé et doit réessayer. Il permet aussi à plusieurs moteurs de partager la même table.

## Compare with the dbt lab

Les deux couches silver contiennent les mêmes données : 2829 commandes pour 8615 unités, et 50 utilisateurs, avec dbt comme avec Spark. Mes exports dbt s'appellent `silver/dbt/orders.parquet` et `silver/dbt/users.parquet` (noms du lab dbt), j'ai donc adapté les chemins des requêtes DuckDB.

### Qu'est-ce qui diffère entre les deux schémas ? Spark n'a pas de type UUID : qu'a-t-on fait, et quelle conséquence pour ceux qui utilisent la couche silver ?
Avec dbt, `order_id` et `user_id` sont en `uuid` et `ordered_at` en `timestamp with time zone`. Avec Spark, les identifiants sont en `varchar` (on les a castés en string) et `ordered_at` en `timestamp` sans fuseau. Les consommateurs doivent donc savoir que l'identifiant est une chaîne de 36 caractères : ça prend plus de place (le fichier orders fait 139 Ko avec Spark contre 72 Ko avec dbt) et il faut caster pour faire une jointure avec des données en UUID.

### Une sortie par modèle avec dbt, un dossier de part-files avec Spark : lequel est plus simple à lire, lequel passe à l'échelle ?
Un seul fichier par modèle est plus simple à lire. Le dossier de part-files passe mieux à l'échelle : chaque tâche écrit son propre fichier en parallèle, et les lecteurs peuvent aussi lire les fichiers en parallèle. La plupart des outils savent lire un dossier avec `*.parquet`.

### Le lab dbt valide la couche silver avec des tests. Qu'a-t-on utilisé ici, et que faudrait-il écrire pour avoir les mêmes garanties ?
Ici, on a seulement des `assert` sur le nombre de lignes et les colonnes, et un `groupBy` pour vérifier qu'il n'y a plus de doublons. Pour avoir les mêmes garanties, il faudrait écrire nous-mêmes un contrôle pour chaque test : `groupBy().count() > 1` pour unique, un filtre sur `isNull()` pour not_null, `isin()` pour accepted_values et une jointure `left_anti` vers les utilisateurs pour relationships, et les lancer à chaque exécution.

### `ref()` et `source()` donnent à dbt le graphe des dépendances. Qu'est-ce qui joue ce rôle dans le notebook ?
Les variables Python et l'ordre d'exécution du code : `stg_users` est construit à partir de `users_typed`, qui vient de `users_bronze`. Mais il n'y a pas de graphe explicite : c'est à nous d'exécuter les cellules dans le bon ordre, et rien ne vérifie les dépendances.

### Pourquoi les temps de build avec `dbt build` et avec les cellules du lab ne sont pas directement comparables ?
dbt ne fait que créer des vues et lancer les tests, alors que Spark calcule vraiment les données et les écrit en Parquet et en Iceberg sur S3. En plus, Spark doit démarrer une JVM et charger le package Iceberg, et avec si peu de données le temps est surtout du temps de démarrage, pas du calcul.

### dbt avec DuckDB ou Spark : batch quotidien de 10 Go, batch de 10 To, équipe d'analystes qui ne connaissent que SQL ?
10 Go : dbt avec DuckDB, une seule machine suffit et c'est plus simple. 10 To : Spark sur un cluster, car ça ne tient plus sur une machine. Des analystes qui ne connaissent que SQL : dbt, avec ses modèles, tests et documentation en SQL (avec DuckDB, ou avec l'adaptateur dbt-spark si les données sont grosses).

## Note
Avec 2 CPU et 2 Go de mémoire, le shell PySpark plantait pendant le calcul. J'ai recréé le service vscode-pyspark avec les ressources demandées par le lab (limits : 4000m CPU, 4000Mi de mémoire) et tout est passé.
