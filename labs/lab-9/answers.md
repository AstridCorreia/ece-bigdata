# Lab 9 - Trino : requêtes sur la couche silver et couche gold

Trino a été déployé avec Helm (lab de déploiement), en mode coordinateur seul, avec un catalogue `lakehouse` de type Iceberg REST qui pointe vers Polaris (principal `dev`). La connexion Python est dans `labs/lab-9/trino_conn.py`, lancée dans `uv run python` avec `exec(open(...).read())`. Les dépendances `trino` et `pandas` ont été ajoutées avec `uv add`.

## Environment

### Que change le paramètre `catalog=` de `connect()` pour les requêtes suivantes ?
Il définit le catalogue par défaut de la session. On pourrait écrire `silver.orders` au lieu de `lakehouse.silver.orders`, Trino compléterait avec `lakehouse`. Dans le lab on met quand même le nom complet partout, donc ça ne change rien au résultat, mais ça évite les erreurs si on l'oublie.

## Catalogs, schemas and tables

`SHOW CATALOGS` donne `lakehouse`, `system`, `tpcds` et `tpch`. Dans `lakehouse` on retrouve le schéma `silver` avec `orders` et `users`, les tables enregistrées dans Polaris au lab précédent. `users` a aussi la colonne `country` ajoutée pendant le lab Polaris.

### Quel type Trino contient `ordered_at` ? Est-ce qu'il garde un fuseau horaire ?
`timestamp(6) with time zone`. Oui, c'est l'équivalent du type Iceberg `timestamptz` écrit par Spark : la valeur est stockée en UTC et Trino l'affiche avec `+00:00`, avec une précision à la microseconde.

### Spark a déclaré `order_id` en string car il n'a pas de type UUID. Que renvoie Trino ?
`varchar`. Trino a un type `uuid`, mais il lit le type de la table Iceberg, qui est `string` puisque c'est Spark qui l'a écrite. Le type est fixé par la table, pas par le moteur qui la lit.

## Query the silver layer

Résultats : 2829 commandes et 50 utilisateurs, comme dans le lab Spark. La requête par État donne au maximum 3 utilisateurs par État, et seuls AE (3/3) et AP (2/2) ont des villes manquantes. La requête des commandes orphelines renvoie 0.

### Expliquer le résultat des requêtes
Les comptes sont les mêmes que dans Spark parce que Trino lit exactement la même table Iceberg (même snapshot), via Polaris. Les villes manquantes viennent des adresses militaires (« DPO AP 09617 »), qui n'ont pas de ville, donc la regex du lab Spark a mis `city` à NULL. 0 commande orpheline : chaque `user_id` de `orders` existe dans `users`, donc une jointure interne ne perdra aucune commande.

### Quel test du lab dbt remplace la requête orpheline ? Que faudrait-il écrire pour la lancer à chaque refresh ?
Le test `relationships` sur `stg_orders.user_id` (et notre test singulier `assert_orders_have_users`). Pour le lancer à chaque refresh, on peut le mettre dans l'étape de validation du pipeline avec un `assert orphan_orders == 0` avant de construire le gold, ou garder le test dans dbt et lancer `dbt test` à chaque exécution, pour arrêter le pipeline s'il échoue.

### Trino a renvoyé les résultats sans job à soumettre ni session à créer. Qu'est-ce que ça dit sur quand choisir Trino plutôt que Spark ?
Trino est un service qui tourne en permanence : on envoie une requête SQL et on a la réponse en quelques secondes. C'est adapté aux requêtes interactives, à l'exploration et aux dashboards. Spark demande de démarrer une session (JVM, packages, config), il est plus adapté aux gros traitements batch et aux transformations en Python.

## Iceberg metadata

`orders$snapshots` et `orders$history` montrent un seul snapshot (`overwrite`), et `orders$files` un seul fichier Parquet de 2829 lignes (84 055 octets).

### Combien de snapshots a `silver.orders` ? Quel moteur les a écrits ?
Un seul snapshot `overwrite`, écrit par Spark avec `writeTo(...).createOrReplace()` quand j'ai recréé la table dans le catalogue `lab`, puis enregistré dans Polaris. Trino n'a rien écrit dans cette table, il la lit seulement.

### Lister les fichiers avec `aws s3 ls` et comparer avec `$files`. Lequel n'a pas besoin d'accès au bucket, et pourquoi ?
`aws s3 ls` liste tous les objets du dossier : le fichier Parquet dans `data/` mais aussi les fichiers de `metadata/` (JSON, manifests Avro, `version-hint.text`). `$files` ne montre que le fichier de données qui fait partie du snapshot courant, avec ses statistiques. C'est `$files` qui n'a pas besoin d'accès au bucket côté utilisateur : on interroge juste Trino, qui trouve les métadonnées grâce à Polaris et lit les manifests avec ses propres identifiants. Avec `aws s3 ls`, il faut avoir soi-même les clés S3.

## Query plan and execution

Le plan distribué a 3 fragments :
- Fragment 2 (`SOURCE`) : `TableScan` de `silver.users` (50 lignes), sortie en `BROADCAST`.
- Fragment 1 (`SOURCE`) : `ScanFilter` de `silver.orders` (2829 lignes), jointure `InnerJoin` avec `distribution = REPLICATED` (la petite table `users` est copiée vers chaque tâche, pas de shuffle de `orders`), puis agrégation `PARTIAL` par `state`, sortie partitionnée en `HASH [state]`.
- Fragment 0 : agrégation `FINAL` par `state` et sortie `SINGLE` vers le client.

Les `RemoteSource` sont les échanges entre fragments (par le réseau entre tâches). Trino ajoute aussi un filtre dynamique sur `user_id` : les 50 identifiants de `users` sont utilisés pour filtrer le scan de `orders`.

Avec `EXPLAIN ANALYZE` : exécution en environ 2,9 s, 2829 lignes lues pour `orders`, 50 pour `users`, 37 lignes en sortie (37 États), pic mémoire de 227 Ko pour l'agrégation finale. La jointure prend la plus grosse part du CPU (environ 41 %). `system.runtime.queries` montre bien l'historique de nos requêtes avec leur durée.

## Gold layer

Le schéma `lakehouse.gold` a été créé, puis les 3 tables avec `CREATE TABLE AS SELECT` : `orders_by_state_month` (50 lignes), `top_products` (6 lignes, croissant en premier avec 1523 unités) et `user_activity` (50 lignes). `orders_by_state_month` n'a que 50 lignes parce que chaque utilisateur passe toutes ses commandes en quelques jours, donc dans un seul mois.

### Pourquoi `user_activity` utilise un `LEFT JOIN` alors que `orders_by_state_month` utilise une jointure interne ? Laquelle dépend du résultat de la requête orpheline ?
`user_activity` part des utilisateurs : le `LEFT JOIN` garde aussi ceux qui n'ont jamais commandé (avec 0 commande). `orders_by_state_month` part des commandes et a besoin de l'État de l'utilisateur, donc on garde seulement les commandes qui ont un utilisateur. C'est cette jointure interne qui dépend de la requête orpheline : s'il y avait des commandes sans utilisateur, elles disparaîtraient sans erreur. Ici il y en a 0, donc rien n'est perdu.

### `current_date` fait changer `age` chaque jour. Qu'est-ce que ça implique pour le refresh de la table ?
La table est figée au jour où elle a été créée : le lendemain, certains âges sont déjà faux. Il faut la rafraîchir au moins une fois par jour, ou ne pas stocker l'âge et le calculer au moment de la requête (dans une vue ou dans le dashboard) à partir de `birthdate`.

### Où sont les fichiers de données des tables gold, et qui a décidé de leur emplacement ?
Dans le bucket, sous `s3://user-a-correia-ece/warehouse/gold/<table>-<id>/`. C'est Polaris qui a choisi l'emplacement, à partir de la `default-base-location` du catalogue (`s3://<bucket>/warehouse/`) et du namespace `gold`. Trino écrit ensuite les fichiers à cet endroit.

## Validation

Tous les `assert` passent : 8615 unités dans `silver.orders`, autant dans `orders_by_state_month` et dans `top_products`, 50 utilisateurs et 2829 commandes dans `user_activity`.

## Refresh a gold table

Après le `DELETE` (6 lignes) et l'`INSERT` (6 lignes), `top_products` a 3 snapshots. Le time travel sur le premier snapshot (`FOR VERSION AS OF`) donne 6 produits, comme la version actuelle.

### Combien de snapshots après le refresh, et quelle est l'opération de chacun ?
3 snapshots : `append` (le `CREATE TABLE AS SELECT`), `delete` (le `DELETE`) et `append` (l'`INSERT`).

### Entre le `DELETE` et l'`INSERT`, un dashboard lit la table. Que voit-il ? Qu'est-ce qui changerait avec un `MERGE` ?
Il voit une table vide, parce que le `DELETE` est un commit à lui tout seul : pendant quelques secondes le snapshot courant ne contient plus aucune ligne. Avec un `MERGE`, la mise à jour se fait en une seule requête et un seul commit atomique : le dashboard voit soit l'ancienne version, soit la nouvelle, jamais une table vide.

## Compare with Spark

### De quoi chaque moteur a besoin pour lancer la requête `top_products`, en plus de la requête elle-même ?
Trino : juste le client Python et l'adresse du cluster (host, port, user). Le cluster tourne déjà, et c'est lui qui a les identifiants Polaris et S3. Spark : une SparkSession à démarrer, le package `iceberg-spark-runtime`, la config du catalogue Polaris avec le credential du principal, les clés S3 dans la config S3A, et le lancement du driver (un job local ou soumis au cluster).

### Trino ou Spark : dashboard rafraîchi toutes les minutes, transformation nocturne de 10 To, data scientist qui explore la couche silver ?
- Dashboard toutes les minutes sur le gold : Trino, réponses rapides et pas de session à démarrer à chaque fois.
- Transformation nocturne de 10 To de bronze : Spark, fait pour les gros batchs, avec reprise des tâches en cas d'échec.
- Data scientist qui explore avec une requête : Trino pour du SQL rapide. Spark s'il doit ensuite travailler en Python ou faire du ML sur les données.

### Trino fait échouer toute la requête si un worker est perdu, sauf avec l'exécution tolérante aux pannes. Pourquoi c'est acceptable ici et pas pour 10 To ?
Les requêtes du lab durent quelques secondes : si elles échouent, on les relance et on perd presque rien. Une transformation de 10 To peut durer des heures, donc perdre un worker à la fin obligerait à tout recommencer. Il faut alors un moteur qui relance seulement les tâches perdues, comme Spark, ou activer l'exécution tolérante aux pannes de Trino.
