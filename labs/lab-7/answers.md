# Lab 7 - Explorer les tables Iceberg avec Spark

Le code exécuté est dans `labs/lab-7/iceberg.py` (lancé dans le shell PySpark avec `exec(open(...).read())`). La table `lab.silver.orders` est celle créée à la fin du lab Spark silver : son premier snapshot a l'opération `overwrite` parce qu'elle a été créée avec `createOrReplace()`.

## Inspect the files

### Quels fichiers contiennent vraiment les lignes ?
Les fichiers Parquet du dossier `warehouse/silver/orders/data/`. Au début il n'y en a qu'un (2829 lignes, 84 Ko), puis chaque écriture en ajoute un nouveau.

### Quels fichiers décrivent la table ?
Ceux du dossier `metadata/` : les `vN.metadata.json` (schéma, partitionnement, liste des snapshots et snapshot courant), les `snap-....avro` (la manifest list de chaque snapshot), les `...-m0.avro` (les manifests, qui listent les fichiers de données avec leurs statistiques) et `version-hint.text` (le numéro de la version courante).

### Pourquoi une table Iceberg a besoin de plus que les fichiers Parquet ?
Les fichiers Parquet seuls ne disent pas quels fichiers font partie de la table à un moment donné, ni quel est le schéma de la table ou son historique. Les métadonnées permettent de savoir exactement quels fichiers lire pour une version, de faire des commits atomiques, de garder des snapshots (time travel) et de faire évoluer le schéma. Elles évitent aussi de devoir lister tout le dossier sur S3 et permettent de sauter des fichiers grâce aux statistiques.

## Explore Iceberg metadata

### Combien de snapshots a la table actuellement ?
Un seul, créé par le `createOrReplace()` du lab précédent.

### Combien de fichiers de données appartiennent à la table ?
Un seul fichier Parquet, avec 2829 lignes et 84 055 octets.

### Quelles informations la table `snapshots` contient-elle que les fichiers Parquet n'ont pas ?
L'identifiant du snapshot, celui de son parent, la date du commit, le type d'opération (overwrite, append...), le chemin de la manifest list et un résumé : nombre de lignes et de fichiers ajoutés, total de lignes et de fichiers de la table, application et moteur qui ont écrit (Spark 4.1.1, Iceberg 1.11.0). Un fichier Parquet ne contient que ses propres données et son schéma, rien sur l'historique de la table.

## Create a second snapshot

Après l'ajout de 3 commandes avec `append()`, la table a 2832 lignes.

### Combien de snapshots maintenant ?
Deux : le premier (`overwrite`) et un nouveau `append`.

### Est-ce que les fichiers Parquet d'origine ont disparu ?
Non. La table `files` montre maintenant deux fichiers : l'ancien avec 2829 lignes, toujours là, et un nouveau avec les 3 nouvelles lignes. Le nouveau snapshot fait référence aux deux.

### Pourquoi Iceberg crée un nouveau snapshot au lieu de remplacer la table ?
Les fichiers ne sont jamais modifiés, on en ajoute seulement. Le nouveau snapshot est une nouvelle version de la table qui réutilise les anciens fichiers et ajoute le nouveau. Comme ça, l'écriture est atomique, une requête en cours continue de lire une version complète et cohérente, on garde l'historique pour le time travel ou un rollback, et c'est beaucoup moins cher que de tout réécrire.

## Time travel

`VERSION AS OF` sur le premier snapshot renvoie 2829 lignes, et la table actuelle 2832.

### Que lit le time travel ?
Il lit les métadonnées du snapshot demandé : sa manifest list, puis ses manifests, et donc seulement les fichiers de données qui faisaient partie de la table à ce moment-là. Ici, c'est seulement le fichier d'origine de 2829 lignes.

### Pourquoi Iceberg peut accéder à une ancienne version sans faire une copie complète de la table ?
Parce que les snapshots partagent les fichiers de données. Un nouveau snapshot n'ajoute que les nouveaux fichiers et de petits fichiers de métadonnées, les anciens fichiers restent en place et sont toujours référencés par l'ancien snapshot.

### Que se passerait-il si les métadonnées et les fichiers de l'ancien snapshot étaient supprimés physiquement ?
On ne pourrait plus faire de time travel vers ce snapshot : la requête planterait avec un fichier introuvable. Le snapshot courant pourrait aussi être cassé si on supprimait un fichier qu'il utilise encore (ici le fichier de 2829 lignes est utilisé par les deux snapshots). C'est pour ça qu'il faut passer par Iceberg (`expire_snapshots`) pour nettoyer : il ne supprime que les fichiers qui ne sont plus utilisés par aucun snapshot gardé.

## Schema evolution

Après `ALTER TABLE ... ADD COLUMN source STRING` et l'`INSERT`, la table a 2833 lignes : 2832 avec `source` à NULL et 1 avec `spark`. Il y a un 3e snapshot `append`.

### Est-ce que les fichiers Parquet existants ont dû être réécrits ?
Non. Les anciens fichiers n'ont pas la colonne `source`, et Iceberg renvoie simplement NULL pour cette colonne quand il les lit : c'est pour ça que 2832 lignes ont `source` à NULL. Seule la nouvelle ligne est écrite dans un nouveau fichier avec la colonne.

### Où est enregistré le nouveau schéma ?
Dans un nouveau fichier `vN.metadata.json` : il contient la liste des schémas avec leur identifiant, et `current-schema-id` pointe vers le nouveau. Chaque colonne a un identifiant (field id), ce qui permet de faire le lien avec les anciens fichiers.

### Pourquoi l'évolution de schéma est une opération au niveau de la table et pas un simple changement d'un fichier Parquet ?
Une table est faite de beaucoup de fichiers écrits à des moments différents, et les fichiers Parquet ne sont jamais modifiés. Le schéma doit donc être défini une seule fois pour toute la table, dans les métadonnées, pour que tous les fichiers (anciens et nouveaux) soient lus de la même façon et que tous les moteurs voient le même schéma. Comme Iceberg suit les colonnes par identifiant et pas par nom, on peut ajouter, renommer ou supprimer une colonne sans réécrire les données.

## Compare Iceberg with Parquet

### Lequel est juste une collection de fichiers Parquet ?
`silver/spark/orders.parquet/` : un fichier `part-00000-...snappy.parquet` et un fichier `_SUCCESS`, sans aucune information de version.

### Lequel a des métadonnées de table et des snapshots ?
`warehouse/silver/orders/`, avec son dossier `metadata/`.

### Comment remplacer sans risque un dossier Parquet pendant qu'une autre requête le lit ?
Il ne faut pas écrire dans le dossier en cours de lecture. On écrit les nouvelles données dans un nouveau dossier (par exemple `orders_v2/`), puis on fait pointer les lecteurs vers ce nouveau dossier (une vue, un chemin dans une config), et on supprime l'ancien plus tard, quand plus personne ne le lit. Sur S3, il n'y a pas de renommage atomique de dossier, donc on ne peut pas juste échanger les deux.

### Comment Iceberg rend l'état de la table visible de façon atomique ?
Il écrit d'abord les nouveaux fichiers de données et les nouveaux fichiers de métadonnées, sans toucher aux anciens. Ensuite, le commit change en une seule opération le pointeur vers le nouveau `metadata.json` (ici `version-hint.text`, sinon le catalogue). Avant ce changement les lecteurs voient l'ancienne version, après ils voient la nouvelle, jamais un état à moitié écrit.

### Pourquoi faut-il un catalogue dès que plusieurs moteurs partagent la même table Iceberg ?
Tous les moteurs doivent savoir où se trouve la version courante de chaque table, et deux moteurs peuvent vouloir commiter en même temps. Un catalogue (REST, Nessie, Hive Metastore, Glue...) donne un nom unique à chaque table, garde le pointeur vers ses métadonnées et fait le changement de version de façon atomique, en refusant un commit si quelqu'un d'autre a commité entre-temps. Avec le catalogue `hadoop` de ce lab, le pointeur est juste un fichier dans le bucket, ce qui n'est pas sûr avec plusieurs écrivains.
