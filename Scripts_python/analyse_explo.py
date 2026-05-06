import pandas as pd
from pandas import col
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import geopandas as gpd
import matplotlib.colors as mcolors

"""
Ce fichier contient les fonctions utiles à l'analyse exploratoire des données.
"""

def mat_corre(data):
    # Sélection des variables numériques
    data_pd = data.select_dtypes(include='number')

    # Calcul de la corrélation (Pandas gère les NA par paire automatiquement)
    corr_pd = data_pd.corr()

    #Sécurité : on remplace par 0 les NaN qui apparaîtraient si une variable a une variance nulle
    corr_pd = corr_pd.fillna(0)

    #Génération du graphique
    sns.clustermap(
        corr_pd, 
        annot=False, 
        cmap='coolwarm',
        vmax=1, vmin=-1,
        figsize=(20, 20),
        linewidths=.5,
        xticklabels=True,  
        yticklabels=True
    )
    plt.show()


def France_Bloc (data):
    # Chargement et Nettoyage des géométries
    url_geojson = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes.geojson"
    france_communes = gpd.read_file(url_geojson)

    # Préparation rigoureuse des données (nettoyage des codes et des noms)
    data['codecommune'] = data['codecommune'].astype(str).str.strip().str.zfill(5)
    data['Bloc_Dominant'] = data['Bloc_Dominant'].astype(str).str.strip() # Enlève les espaces invisibles
    france_communes['code'] = france_communes['code'].astype(str).str.strip().str.zfill(5)

    # Fusion (Merge)    
    carte_data = france_communes.merge(data, left_on='code', right_on='codecommune', how='inner')

    # Configuration du dictionnaire de couleurs
    couleurs_dict = {
        'pvoteG': '#FF0000',   # Rouge
        'pvoteCG': "#C46B7A",  # Rose
        'pvoteC': '#FFA500',   # Orange
        'pvoteCD': '#ADD8E6',  # Bleu clair
        'pvoteD': '#0000FF',   # Bleu
    }

    # Création  de la palette (Colormap)
    categories_reelles = sorted(list(carte_data['Bloc_Dominant'].unique()))

    # On crée la liste des couleurs : si une catégorie est inconnue, elle sera VERT FLUO
    couleurs_liste = [couleurs_dict.get(cat, '#00FF00') for cat in categories_reelles]
    cmap_custom = mcolors.ListedColormap(couleurs_liste)

    # Affichage de la carte
    fig, ax = plt.subplots(1, 1, figsize=(15, 15), dpi=100)

    carte_data.plot(
        column='Bloc_Dominant', 
        ax=ax, 
        categorical=True,
        categories=categories_reelles, # On force l'ordre détecté
        cmap=cmap_custom,              
        legend=True, 
        linewidth=0,          # Supprime les lignes qui font le voile gris
        edgecolor='none',     # Supprime les contours
        antialiased=False,    # Évite le lissage qui crée du gris entre les communes
        legend_kwds={
            'title': "Blocs Politiques",
            'loc': 'upper left',
            'bbox_to_anchor': (1, 1),
            'frameon': False
        }
    )

    # Finalisation
    ax.set_axis_off()
    plt.title("Carte de France par Bloc Dominant (2022)", fontsize=18, fontweight='bold', pad=20)

    # Petit check de sécurité dans la console
    print(f"Nombre de communes affichées : {len(carte_data)}")
    if '#00FF00' in couleurs_liste:
        print("ATTENTION : Certaines catégories n'ont pas été trouvées dans ton dictionnaire (affichées en vert fluo).")

    plt.tight_layout()
    plt.show()