# Lab 5 - Introduction à l'API RDD de Spark

## RDD creation and partitions

### Qu'est-ce qui décide du nombre de partitions choisi par défaut par `textFile()` pour un fichier de cette taille ?
J'obtiens 2 partitions pour `users.csv` et pour `orders.csv`. `textFile()` découpe le fichier selon la taille des blocs (128 Mo par défaut), mais il utilise aussi un minimum de partitions, `minPartitions = min(defaultParallelism, 2)`. Nos fichiers font moins de 1 Mo, donc c'est ce minimum de 2 qui s'applique.

### `repartition()` et `coalesce()` changent tous les deux le nombre de partitions. Lequel fait un shuffle ?
`repartition()` fait toujours un shuffle : les données sont redistribuées sur toutes les nouvelles partitions (ici on passe de 2 à 8). `coalesce()` ne fait pas de shuffle par défaut : il ne peut que diminuer le nombre de partitions en fusionnant des partitions voisines, donc c'est moins cher.

## Lazy evaluation and lineage

`toDebugString()` affiche seulement le lineage (`HadoopRDD` -> `MapPartitionsRDD` -> `PythonRDD`, 2 partitions) : rien n'est encore lu tant qu'on n'a pas lancé d'action. Le `count()` donne 2829 commandes.

### Si on appelle `orders_data.count()` une 2e fois, est-ce que la Spark UI affiche un nouveau stage ? Pourquoi ?
Oui. Chaque `count()` crée un nouveau job avec son propre stage, et dans la Spark UI les deux stages ont lu 417.9 KiB en entrée. Spark ne garde pas les résultats des transformations : à chaque action, il refait tout le lineage depuis le fichier (lecture + filtre).

### À quel moment `.cache()` changerait cette réponse ?
Si on met `.cache()` sur `orders_data` avant le premier `count()`. Le premier `count()` calcule les partitions et les garde en mémoire, et le deuxième lit directement le cache au lieu de relire et refiltrer le fichier CSV.

## The cost of no schema: the multiline address

`textFile()` lit 101 lignes dans `users.csv` alors qu'il n'y a que 50 utilisateurs : 1 ligne d'en-tête + 2 lignes par utilisateur, parce que chaque adresse contient un retour à la ligne. Après le filtre sur l'UUID en début de ligne, on retrouve bien 50 utilisateurs.

### Pourquoi `spark.read.csv(multiLine=True)` n'a pas ce problème alors que `sc.textFile()` l'a ?
Le lecteur CSV comprend le format : il sait qu'un champ entre guillemets peut contenir un retour à la ligne, donc il lit des enregistrements et pas des lignes. `textFile()` coupe juste à chaque `\n`, sans rien savoir du CSV ni des guillemets.

### Qu'est-ce qui doit être vrai pour qu'un format soit lisible sans risque par un lecteur ligne par ligne comme `textFile()` ?
Il faut qu'un enregistrement corresponde exactement à une ligne, sans retour à la ligne à l'intérieur des valeurs. C'est le cas par exemple du JSON Lines ou d'un CSV sans champs multilignes.

## Manual parsing of orders

### Que se passerait-il dans cette cellule si une ligne de `orders.csv` avait une virgule en trop dans `product` ?
`line.split(",")` renverrait 6 valeurs au lieu de 5, et la ligne `order_id, user_id, date, quantity, product = ...` lèverait une erreur `ValueError: too many values to unpack`. L'erreur n'apparaîtrait pas à la lecture mais à la première action qui touche cette ligne, et tout le job planterait.

### Où l'API DataFrame fait-elle la vérification équivalente, et quand ?
Dans le lecteur CSV, quand les données sont lues pendant l'exécution d'une action. Il gère les guillemets, donc une virgule dans une valeur entre guillemets ne pose pas de problème, et il convertit les colonnes selon le schéma. Selon l'option `mode`, une ligne invalide devient NULL (`PERMISSIVE`, par défaut), est ignorée (`DROPMALFORMED`) ou fait échouer le job (`FAILFAST`). En plus, les erreurs de colonnes ou de types dans la requête sont détectées avant l'exécution, quand Spark analyse le plan.

## Key-value RDDs: total quantity per user

`reduceByKey` et `groupByKey` donnent les mêmes totaux par utilisateur. Dans la Spark UI, le stage de `reduceByKey` écrit 2.5 KiB de shuffle, contre 6.1 KiB pour `groupByKey`, soit environ 2,4 fois plus.

### `reduceByKey` combine les valeurs dans chaque partition avant le shuffle. Qu'est-ce que `groupByKey` envoie à la place, et pourquoi c'est plus cher ?
`groupByKey` envoie toutes les paires (user_id, quantity) une par une dans le shuffle, donc les 2829 quantités, et ne fait la somme qu'à la fin. `reduceByKey` fait déjà une somme partielle dans chaque partition et n'envoie qu'un total par utilisateur et par partition. Moins de données passent par le réseau et sur le disque, donc c'est plus rapide et ça utilise moins de mémoire.

### Pour quelles agrégations `groupByKey` serait inévitable ?
Quand on a besoin de toutes les valeurs d'une clé en même temps et qu'on ne peut pas les combiner petit à petit : par exemple calculer une médiane, trier toutes les commandes d'un utilisateur, ou garder la liste complète de ses commandes.

## Manual deduplication

Avant et après la déduplication, on a 2829 commandes : le générateur ne crée pas de doublons.

### Dans la window function, `partitionBy()` est une transformation large qui regroupe les lignes d'un même `order_id` sur la même partition. Qu'est-ce qui joue ce rôle ici ?
C'est le `reduceByKey` sur la clé `order_id` : il fait un shuffle qui envoie toutes les commandes avec le même `order_id` dans la même partition, puis applique `most_recent` pour n'en garder qu'une.

### Quelle version exprime le plus directement l'intention ?
La window function. `row_number() over (partition by order_id order by ordered_at desc) = 1` dit clairement "garder la commande la plus récente pour chaque order_id". Avec `reduceByKey`, il faut écrire une fonction à part et comprendre qu'elle compare les éléments deux par deux.

## Joining orders with users

La jointure donne 2829 commandes enrichies avec le username de l'utilisateur.

### Qu'est-ce qui devrait être vrai sur le partitionnement de `orders_kv` et `users_kv` pour que Spark évite le shuffle ?
Il faudrait que les deux RDD soient déjà partitionnés de la même façon sur la clé `user_id`, avec le même partitioner et le même nombre de partitions (par exemple avec `partitionBy()` sur les deux avant la jointure). Les lignes avec la même clé seraient déjà dans la même partition, et Spark pourrait joindre partition par partition.

### Si on remplace `join()` par `leftOuterJoin()`, que se passe-t-il si un `user_id` de `orders` n'a pas d'utilisateur correspondant ?
Avec `join()`, la commande disparaît du résultat. Avec `leftOuterJoin()`, elle est gardée et le username vaut `None`. Ici, j'ai compté les commandes avec `None` après le `leftOuterJoin()` et j'obtiens 0 : toutes les commandes ont un utilisateur.

### Quel est le risque d'appeler `.collect()` directement sur `enriched` avec un dataset beaucoup plus gros ?
`collect()` ramène toutes les données sur le driver. Sur un gros dataset, ça ne tient pas en mémoire et le driver plante (OutOfMemoryError). Il vaut mieux d'abord filtrer ou agréger, utiliser `take()`, ou écrire le résultat sur S3.

## Note
La Spark UI passe par le proxy de VS Code sur Onyxia : il faut lancer PySpark avec `--conf spark.ui.proxyBase=/proxy/4040` et ouvrir `<url du service>/proxy/4040/`, sinon on a une page "Not found".
