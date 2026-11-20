# Lab 8 - Gouverner les tables Iceberg avec Polaris

Polaris a été déployé avec Helm dans le namespace (lab de déploiement). Le code de la SparkSession est dans `labs/lab-8/session.py`, lancé dans le shell PySpark avec `exec(open(...).read())`. Le principal est choisi avec la variable d'environnement `POLARIS_PRINCIPAL` (`dev` par défaut).

Note : le dossier `warehouse/` avait été supprimé à la fin du lab Iceberg (cleanup). J'ai donc recréé `lab.silver.orders` et `lab.silver.users` à partir des fichiers Parquet du lab Spark (`silver/spark/*.parquet`) avec `writeTo(...).createOrReplace()` avant de commencer.

## SparkSession with the Polaris catalog

`SHOW NAMESPACES IN lab` renvoie `silver`, `SHOW NAMESPACES IN polaris` ne renvoie rien au début.

### Quel composant a répondu à chacune des deux commandes `SHOW NAMESPACES` ? Comment le voir ?
Pour `lab`, c'est Spark lui-même : le catalogue `hadoop` liste juste les dossiers sous `s3a://<bucket>/warehouse/`, donc il trouve `silver`. Pour `polaris`, c'est le serveur Polaris qui répond via l'API REST (`http://polaris:8181/api/catalog`), à partir de sa base PostgreSQL. On le voit parce que le dossier `silver` existe bien dans le bucket mais Polaris renvoie une liste vide : il ne regarde pas le bucket, seulement ce qui a été déclaré chez lui.

### Où est utilisé le credential : dans la session Spark, dans le bucket ou dans une requête à Polaris ?
Dans une requête à Polaris. Spark envoie le `clientId:clientSecret` du principal à Polaris (endpoint OAuth `/api/catalog/v1/oauth/tokens`) pour obtenir un token, puis met ce token dans chaque appel REST. Le bucket ne connaît pas ce credential : les fichiers sont lus et écrits avec les clés S3 d'Onyxia configurées dans S3A.

## Register the Silver table in Polaris

`version-hint.text` contenait `1`, la table a été enregistrée avec `v1.metadata.json`. Résultat : 2829 commandes, 1 fichier de données, le même snapshot que dans `lab`.

### Qu'est-ce que `register_table` a écrit, dans Polaris et dans le bucket ?
Dans Polaris, une entrée `silver.orders` dans le catalogue, qui pointe vers `v1.metadata.json`. Dans le bucket, rien : il y avait 5 objets sous `warehouse/silver/orders/` avant et toujours 5 après. Les données et les métadonnées ne sont pas copiées, Polaris garde juste le pointeur.

### Pourquoi c'est dangereux de continuer à écrire dans cette table via le catalogue `lab` une fois qu'elle est enregistrée dans `polaris` ?
Les deux catalogues ont chacun leur propre pointeur vers la version courante. Un commit via `lab` crée un `v2.metadata.json` et met à jour `version-hint.text`, mais Polaris pointe toujours sur l'ancienne version : les lecteurs qui passent par Polaris (comme Trino plus tard) ne voient pas les nouvelles données. Si on écrit des deux côtés, on a deux historiques différents pour la même table. En plus, en passant par `lab` on contourne complètement les droits de Polaris.

## Create tables as `dev`

`SHOW TABLES IN polaris.silver` montre `orders`, avec un seul snapshot `overwrite` (celui du `createOrReplace()`). La table `polaris.silver.users` a été créée à partir de `lab.silver.users` (50 utilisateurs).

### Où Polaris a stocké les fichiers de `silver.users`, et pourquoi ?
Dans `s3://<bucket>/warehouse/silver/users/`, parce que le catalogue a comme `default-base-location` `s3://<bucket>/warehouse/` et Polaris ajoute `<namespace>/<table>`. C'est exactement le même dossier que `lab.silver.users`, donc les fichiers des deux tables sont mélangés : on reconnaît ceux de `lab` (`v1.metadata.json`, `version-hint.text`) et ceux de Polaris (`00000-<uuid>.metadata.json`, puis `00001-...` après le changement de schéma). Il y a aussi 2 fichiers Parquet dans `data/`, un par table.

### Quelle différence entre "registered" et "created through Polaris" pour les fichiers de données ?
Pour `orders` (registered), les fichiers ont été écrits avant, par Spark via le catalogue `hadoop`, et Polaris ne fait que les référencer. Pour `users` (created), c'est Polaris qui a choisi l'emplacement et qui gère les métadonnées dès le début. Dans les deux cas, les fichiers Parquet sont écrits par Spark avec les clés S3, pas par Polaris.

## Schema change as `dev`

`ALTER TABLE polaris.silver.users ADD COLUMN country STRING` marche, `DESCRIBE` montre la nouvelle colonne.

### Quel fichier de métadonnées enregistre le changement ? Est-ce le même que celui de `register_table` ?
Un nouveau fichier `00001-<uuid>.metadata.json` dans `warehouse/silver/users/metadata/`. Ce n'est pas le même : `register_table` pointait vers `orders/metadata/v1.metadata.json`, et de toute façon Iceberg ne modifie jamais un fichier de métadonnées existant, il en écrit un nouveau à chaque commit et Polaris déplace son pointeur dessus.

### Quel privilège Polaris vérifie pour cette requête ?
L'opération vérifiée est `ADD_TABLE_SCHEMA` (c'est le nom qu'on retrouve dans l'erreur de l'analyst). Il faut un droit d'écriture sur les métadonnées de la table (`TABLE_WRITE_PROPERTIES`, ou le privilège plus fin `TABLE_ADD_SCHEMA` dans les versions récentes). `dev` l'a grâce à `CATALOG_MANAGE_CONTENT` (via `catalog_contributor`), qui inclut `TABLE_FULL_METADATA` et `TABLE_WRITE_DATA`.

## Read as the analyst

En `analyst`, `SHOW TABLES` et le `SELECT product, count(*)` marchent. L'`INSERT` échoue avec :
`ForbiddenException: Forbidden: Principal 'analyst' with activated PrincipalRoles '[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT`
Le `ALTER TABLE ... ADD COLUMN phone` échoue de la même façon, avec `op ADD_TABLE_SCHEMA`.

### Quel privilège manque pour l'`INSERT` ? Quels privilèges donne `catalog_reader` ?
Il manque un droit d'écriture sur la table : `TABLE_WRITE_DATA` (ou le privilège fin `TABLE_ADD_SNAPSHOT`), nécessaire pour l'opération `ADD_TABLE_SNAPSHOT`. `catalog_reader` donne seulement `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` et `CATALOG_READ_PROPERTIES`, donc de la lecture.

### En repassant en `dev`, est-ce que l'`INSERT` raté a créé un snapshot ?
Non, `silver.orders` a toujours un seul snapshot et 2829 lignes. Spark écrit d'abord le fichier Parquet, puis demande le commit à Polaris, qui refuse. Spark a alors annulé l'écriture (`IcebergBatchWrite ... aborted`) et supprimé le fichier : il n'y a toujours qu'un seul fichier dans `warehouse/silver/orders/data/`.

Limite : Polaris contrôle seulement le catalogue. La session de l'analyst a quand même les clés S3, donc elle pourrait écrire directement dans le bucket ou passer par le catalogue `lab`. Sans credential vending, Polaris ne peut pas empêcher ça.

## Cleanup

Je n'ai pas supprimé `polaris.silver.users` : le lab Trino lit `lakehouse.silver.users` et `lakehouse.silver.orders` via Polaris, donc les deux tables doivent rester enregistrées.
