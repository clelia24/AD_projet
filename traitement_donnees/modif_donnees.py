import pandas as pd
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

def transfo_ratios(data):
    # on prend en entrée le dataframe et la liste des colonnes à transformer en ratios
    data['part_etranger'] = data['etranger2022'] / data['pop2022']
    data['part_francais'] = data['francais2022'] / data['pop2022']
    data['densite_menages'] = data['pop2022']/data['nmen'] 

    cols_to_drop = [
        "etranger2022", 
        "francais2022", 
        "nmen",             # Nombre de ménages
    ]   

    data = data.drop(cols_to_drop, axis=1, errors='ignore')
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
    ]

    #pour enlever toutes les colonnes qui commencent par voix ou par vote -> ce sont des données brutes de votes, on préfère les pourcentages de votes qui sont plus parlants et comparables entre les communes
    raw_votes_cols = [col for col in data.columns if col.startswith('voix') or col.startswith('vote')]


    total_drop = cols_to_drop + raw_votes_cols


    data = data.drop(total_drop, axis=1, errors='ignore')
    return data

def modif_quali (data, col, bins, labels,right,nom):
    data[nom]=pd.cut(data[col], bins=bins, labels=labels, right=right) # right=False pour que 20 soit dans la tranche 20-30, etc.
    data= data.drop(col, axis=1, errors='ignore') # on supprime la variable quantitative 
    return data