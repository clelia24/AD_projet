import polars as pl

'''
1re partie
selection des colonnes qui nous intéressent dans le fichier csv téléchargé sur le site
'''


# On prépare la requête sans charger les données
overrides = {
    "codecommune": pl.Categorical,
    "dep": pl.Categorical    
}

query = (
    pl.scan_csv("diplomescommunes.csv",
        schema_overrides=overrides, 
        separator=",")
    .select([
        "codecommune",
        "dep",
        "pbac2022",
        "psup2022"
        ])
)

df = query.collect()

print(df.head())
# Sauvegarde en Parquet
df.write_parquet("data_diplomes.parquet")

'''
Ce code nous a servi à transformer des fichiers csv avec beaucoup de colonnes et de variables que nous ne souhaitons pas conserver dans notre analyse (données d'une année passée, etc...) en fichier parquet avec uniquement les variables qui nous intéressent


2e partie : normalisation de certaines colonnes pour que les données soient cohérentes
'''

from pathlib import Path
# 0. On crée une fonction pour normaliser les code communes (ajout de zéro devant les codes à 4 chiffres)
# Cette fonction sert à uniformiser les colonnes utilisées pour les jointures (codecommune et dep).
# Si elles ne sont pas strictement identiques entre les fichiers, la jointure échoue et Polars met des null.

def normaliser(df):
    return df.with_columns([
        pl.col("codecommune")
        .cast(pl.Utf8) #transforme la colonne en string pour pouvoir la modifier facilement (fcts modifs de string)
        .str.strip_chars() #supprime les espaces en début et fin de chaîne
        .str.zfill(5) #ajoute des zéros à gauche pour que tous les codes aient 5 caractères (ex: 1001 devient 01001)
        .cast(pl.Categorical), #retransforme la colonne en catégorielle pour optimiser les jointures et réduire la mémoire utilisée (sinon ce qu'on fait après ne marche pas)

        pl.col("dep")
        .cast(pl.Utf8) 
        .str.strip_chars()
        .str.zfill(2)
        .cast(pl.Categorical)
    ])
    
df=normaliser(pl.scan_parquet("données_ind/data_proprietairescommunes.parquet"))
df.collect().write_parquet("données_ind/data_proprietairescommunes.parquet") #on réécrit le fichier avec les codes normalisés pour éviter de devoir faire la normalisation à chaque fois qu'on l'utilise dans une jointure
df.head(5)


'''
3e partie

'''
# A partir de tous nos "sous-fichiers" en parquet, on crée maintenant un unique fichier parquet qui contient toutes les données. 
# Pour cela, on va faire des jointures successives entre les différents fichiers en utilisant les colonnes "codecommune" et "dep" comme clés de jointure.
# Cela nous permet d'assembler nos colonnes en gardant nos colonnes alignées par individu (dans le cas où les données n'existent pas pour certaines communes,
# on aura des valeurs null mais les autres colonnes seront bien alignées pour les communes qui ont des données dans tous les fichiers).


# 1. On définit le chemin du dossier
dossier_data = Path("données_ind")

# Liste des noms de fichiers (sans le dossier, on l'ajoutera dynamiquement)
fichiers_a_joindre = [
    "data_agesex.parquet", "data_basesfiscalescommunes.parquet",
    "data_capitalimmobiliercommunes.parquet", "data_crimesdelits.parquet",
    "data_csp.parquet", "data_diplomes.parquet", "data_electeurs.parquet",
    "data_isfcommunes.parquet", "data_menages.parquet", "data_naticommune.parquet",
    "data_population.parquet", "data_proprietairescommunes.parquet",
    "data_publicprive.parquet", "data_revenu.parquet", "data_rsa.parquet"
]

with pl.StringCache():
    # 2. On utilise / pour joindre le dossier et le nom du fichier
    path_base = dossier_data / "data_vote2022.parquet"
    
    

    q_globale =pl.scan_parquet(path_base).with_columns([
        pl.col("codecommune").cast(pl.Categorical),
        pl.col("dep").cast(pl.Categorical)
    ])

    for nom_fichier in fichiers_a_joindre:
        # On construit le chemin complet : données_ind/nom_du_fichier.parquet
        chemin_complet = dossier_data / nom_fichier
        
        # On vérifie si le fichier existe pour éviter un crash
        if not chemin_complet.exists():
            print(f"⚠️ Fichier introuvable : {chemin_complet}")
            continue

        q_autre =pl.scan_parquet(chemin_complet).with_columns([
            pl.col("codecommune").cast(pl.Categorical),
            pl.col("dep").cast(pl.Categorical)
        ])
        
        # Nettoyage des colonnes pour éviter les doublons
        cols_a_garder = [c for c in q_autre.columns if c not in q_globale.columns or c in ["codecommune", "dep"]]
        q_autre = q_autre.select(cols_a_garder)

        q_globale = q_globale.join(q_autre, on=["codecommune", "dep"], how="left")

    df_complet = q_globale.collect()

print(f"Fusion terminée ! Taille du dataframe : {df_complet.shape}")

# Finalement, on sauvegarde le dataframe complet en parquet pour pouvoir l'utiliser facilement dans notre analyse. 
# On a choisi le format parquet plutôt que csv car il est plus rapide à lire et à écrire, ce qui est important pour un dataframe aussi volumineux que celui-ci. 

df_complet.write_parquet("data_complet.parquet")
shape = df_complet.shape
print(f"Le DataFrame complet a {shape[0]} lignes et {shape[1]} colonnes.")