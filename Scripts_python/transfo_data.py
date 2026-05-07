#from great_tables import data
import pandas as pd
from pandas import col
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

"""
Cette partie du fichier contient des fonctions de transformation des données. 
Toutes les fonctions ont été développées à l'occasion du projet. 
"""
def transfo_ratios(data,col,col_scale,new_name, drop_col):
    # on prend en entrée le dataframe, la colonne à transformer, la colonne de référence pour le calcul du ratio et le nom de la nouvelle colonne
    data[new_name] = data[col] / data[col_scale] # on crée la nouvelle colonne de ratio
    if drop_col:
        data=data.drop(col, axis=1, errors='ignore') # on supprime la variable de référence si on ne veut pas la garder
    return data

def cols_drop (data): 
    cols_to_drop =[
        #colonnes en rapport à l'ISF : trop peu de communes avec des données et de plus les données sont très corrélées avec les données de revenu et de capital immobilier, on perd peu d'information en les supprimant
        "nfoyisf2017",
        "mmoyfortune2017",
        "perpisf2017",
        "misf2017",
        'permisf2017',
        "pisf2017",
        #trop de données manquantes : 
        "pop2017",
        #indique simplement si la ville est Paris, Lyon ou Marseille, elle n'importe pas d'information supplémentaire par rapport au code commune
        "plm",
        #colonnes de vote : on préfère les pourcentages de votes qui sont plus parlants et comparables entre les communes
        'pervoteG',
        'pervoteCG',
        'pervoteC', 
        'pervoteCD', 
        'pervoteD', 
        'pervoteTG', 
        'pervoteTD', 
        'pervoteGCG', 
        'pervoteDCD',
        # pour les données concernant l'abstention, on garde uniquement le  ration du pourcentage de participation par rapport à la population totale. Les autres informations sont redondantes 
        'ppar',
        'perpar',
        'pblancnul',
        'pblancsnuls',
        'pins',
        'pabs',
        'pblancsnuls',
        # on garde seulement la variable d'age moyen, les autres variables d'age sont redondantes et très corrélées entre elles
        'prop0142022',
        'prop15392022',
        'prop40592022',
        'prop60p2022',
        # les données de recettes et de revenus sont très corrélées entre elles, on garde uniquement les ratios qui sont plus parlants et comparables entre les communes
        'recetteratio2022',
        'recetteimpotslocauxratio2022',
        'capitalimmo2022',
        'prsa2021',
        'revmoyfoy2022',
        'revratiofoy2022',
        'perrev2022',
        'revmoy2022',
        'ppropri2022',
        'prive2021_total',
        'prixm22022',
        'propappartement2022',
        'prixbien2022',
        'prixm',
        # les percentiles de taille de commune n'apportent pas d'informations complémentaires
        'peragglo2022',
        'percommu2022',
        # si les percentiles sont présents, on enlève les variabes de pourcentage et de bruts 
        'petranger2022',
        'pcrimesdelits2020',
        "electeurs2022",
        "prive2021_total"
        "nmencomp73",
        "nmen"
    ]

    #pour enlever toutes les colonnes qui commencent par voix ou par vote -> ce sont des données brutes de votes, on préfère les pourcentages de votes qui sont plus parlants et comparables entre les communes
    raw_votes_cols = [col for col in data.columns if col.startswith('voix') or col.startswith('vote')]


    total_drop = cols_to_drop + raw_votes_cols


    data = data.drop(total_drop, axis=1, errors='ignore')
    return data

def modif_quali (data, col, bins, labels,nom, right):
    data[nom]=pd.cut(data[col], bins=bins, labels=labels, right=right) # right=False pour que 20 soit dans la tranche 20-30, etc.
    data= data.drop(col, axis=1, errors='ignore') # on supprime la variable quantitative 
    data[nom] = data[nom].astype('category') # on convertit la variable qualitative en type category pour économiser de la mémoire et faciliter les analyses ultérieures
    return data

def crea_variable_gagnants (data):
    # on crée les deux variables dont on a besoin pour l'analyse
    # Définition des blocs à comparer
    blocs = [
        'pvoteG', 'pvoteCG', 'pvoteC', 
        'pvoteCD', 'pvoteD',
    ]
    #par commune : le bloc "vainqueur" et son score
    data['Bloc_Dominant'] = data[blocs].idxmax(axis=1)
    data['Bloc_Score'] = data[blocs].max(axis=1)

    partis =['pvoixAUG', 'pvoixNUP', 'pvoixDVG', 'pvoixECO', 'pvoixREG', 'pvoixENS', 'pvoixUDI', 'pvoixDVD', 'pvoixREC', 'pvoixRN', 'pvoixLR' ]
    data['Parti_Dominant']= data[partis].idxmax(axis=1)
    data['Parti_score']=data[partis].max(axis=1)

    data['Bloc_Dominant']=data['Bloc_Dominant'].astype('category')
    data['Parti_Dominant']=data['Parti_Dominant'].astype('category')
    return data

def ajouter_variable_division(data, colonnes_partis, seuil=0.25):
    # On cherche le score maximum pour chaque commune parmi les colonnes de partis
    score_max = data[colonnes_partis].max(axis=1)
    
    # Si le score max est inférieur au seuil, la commune est considérée comme "divisée"
    data['Est_Divise'] = (score_max < seuil).astype(int)
 
    nb_divises = data['Est_Divise'].sum()
    print(f"Variable 'Est_Divise' créée : {nb_divises} communes sont considérées comme divisées.")
    
    return data


""" 
Cette partie du fichier contient des fonctions d'affichage des données.
Toutes les fonctions ont été développées à l'occasion du projet.
"""

def affichage_qualite(data): 
    # on affiche les doublons 
    n_duplicated = data.duplicated().sum()
    print(f"Number of duplicated rows: {n_duplicated}\n")

    # On crée le tableau de résumé de la qualité

    data_quality = pd.DataFrame({
        "colonne": data.columns,
        "n_unique": data.nunique().values,
        "n_missing": data.isna().sum().values,
        "missing_ratio": (data.isna().sum() / len(data)).values,
        "dtype": data.dtypes.astype(str).values
    })
    # création visuel coloré
    # data_quality est DÉJÀ un DataFrame Pandas, on applique le style directement.
    affichage_colore = (
        data_quality.style
        .background_gradient(subset=['n_missing', 'missing_ratio'], cmap='Reds')
        .bar(subset=['n_unique'], color='#5fba7d')
        .format({'missing_ratio': '{:.2%}'})
        .map(lambda x: 'font-weight: bold', subset=['colonne'])
    )
    return affichage_colore

def affichage_hist(data, col,log_scale): 
    # Configuration du style visuel
    sns.set_theme(style="whitegrid")
    if log_scale==True:
        # On utilise log_scale=True
        sns.histplot(data=data, x=col, bins=50, kde=True, log_scale=True, color='skyblue')
        plt.title(f'Distribution (Échelle Logarithmique) de {col}', fontsize=14)
        plt.xlabel(f'{col} (Log)', fontsize=12)
        plt.ylabel('Fréquence', fontsize=12)
        plt.show()
        return
    else : 
        # Création de l'histogramme
        plt.figure(figsize=(10, 6))
        sns.histplot(data=data, x=col, bins=100, kde=True, color='skyblue')

        # Ajout des titres et labels
        plt.title(f'Distribution de {col}', fontsize=14)
        plt.xlabel(f'{col}', fontsize=12)
        plt.ylabel('Fréquence', fontsize=12)
        plt.xlim(0, data[col].quantile(0.99))  # Limiter l'axe x pour mieux visualiser la distribution
        # Affichage
        plt.show()
