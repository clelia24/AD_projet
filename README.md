# AD_projet

## Présentation du projet

Nous avons basé ce projet sur un jeu de données issues du livre Julia Cagé et Thomas Piketty (2023) : Une histoire du conflit politique. Élections et inégalités sociales en France, 1789-2022, Paris, Le Seuil. Ces données sont consultables sur le site [Une histoire du conflit politique](https://unehistoireduconflitpolitique.fr/). 

Notre objectif était d'explorer les variables socio-économiques et de comprendre la manière dont elles s'intégraient dans le paysage politique. 

## Organisation du projet 

Ce projet est construit autour d'un Jupyter Notebook qui regroupe nos résultats ainsi que nos analyses. Les codes sont consultables sur des fichiers pythons annexes, présents dans le dossier *Scripts_python*. 
Les fonctions sont classées en fonction de leur apparition dans le Notebook, et triées par partie. Les fichiers python sont les suivants :
* `script_fichier_csv.py` qui comprend la création du fichier CSV. Il n'a été executé  qu'une seule fois pour créer notre dataset 
* `transfo_data.py` qui regroupe les fonctions utilisées dans la premiere partie du notebook **"2-Traitement des données"**
* `analyse_explo` qui comprend les fonctions de la partie **"3-Analyse exploratoire"**
* `reduc_dim` qui comprend les fonctions de la partie **"4-Réduction de dimension linéaire"**
* `clustering.py` qui coomprend les fonctions de la partie **5-Clustering**

Ces indications peuvent être retrouvées directement dans le notebook. 
