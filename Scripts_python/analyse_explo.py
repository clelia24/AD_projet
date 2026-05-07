from great_tables import data
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

def France_Parti(data): 
    # Chargement & Préparation 
    url_geojson = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes.geojson"
    france_communes = gpd.read_file(url_geojson)
    data['codecommune'] = data['codecommune'].astype(str).str.zfill(5)
    carte_data = france_communes.merge(data, left_on='code', right_on='codecommune')

    # Configuration des couleurs (Vérifie bien que ces noms sont dans 'Parti_Dominant')
    couleurs = {
        'pvoixAUG': '#8B0000',    # Rouge sombre / Bordeaux (Extrême Gauche)
        'pvoixNUP': '#FF3333',    # Rouge vif (NUPES)
        'pvoixDVG': '#FF9999',    # Rose (Divers Gauche)
        'pvoixENS': '#FFCC00',    # Jaune/Orange (Ensemble / Majorité Présidentielle)
        'pvoixUDI': '#99CCFF',    # Bleu ciel très clair ou Orange (souvent entre centre et droite)
        'pvoixREG': '#006633',    # Vert forêt (Régionalistes)
        'pvoixLR': '#3366CC',     # Bleu (Les Républicains)
        'pvoixDVD': '#ADC2EB',    # Bleu clair (Divers Droite)
        'pvoixRN': '#000080',     # Bleu marine (Rassemblement National)
        'pvoixREC': '#000033',    # Bleu nuit / Noir (Reconquête)
    }

    # Gestion des catégories
    valeurs_reelles = carte_data['Parti_Dominant'].unique().tolist()

    # Correction : Ajout de la virgule entre 'pvoixENS' et 'pvoixLR'
    ordre_logique = ['pvoixAUG','pvoixNUP','pvoixDVG','pvoixENS', 'pvoixLR','pvoixDVD','pvoixRN','pvoixREC','pvoixUDI','pvoixREG']

    categories_finales = [b for b in ordre_logique if b in valeurs_reelles]
    for v in valeurs_reelles:
        if v not in categories_finales:
            categories_finales.append(v)

    couleurs_liste = [couleurs.get(cat, '#CCCCCC') for cat in categories_finales]
    cmap_custom = mcolors.ListedColormap(couleurs_liste)

    #  Affichage
    fig, ax = plt.subplots(1, 1, figsize=(15, 15), dpi=150)

    carte_data.plot(
        column=carte_data['Parti_Dominant'].astype(str), 
        ax=ax, 
        categorical=True,
        categories=categories_finales,
        cmap=cmap_custom,              
        legend=True, 
        linewidth=0,          
        edgecolor='none',     
        legend_kwds={
            'title': "Partis Politiques",
            'loc': 'upper left',
            'bbox_to_anchor': (1, 1),
            'frameon': False
        }
    )

    ax.set_axis_off()
    plt.title("Carte de France par Parti Dominant (2022)", fontsize=18, fontweight='bold', pad=20)
    plt.show()

def tracer_scatter (data, col_x, col_y, log_scale, moyenne, lim): 
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=data, 
        x=col_x,  
        y=col_y,  
        alpha=0.4, 
        s=15,      
        color='#2b8cbe' 
    )
    if moyenne:
        col_moyenne = col_y
        plt.axhline(data[col_moyenne].mean(), color='red', linestyle='--', linewidth=2, label=f'Moyenne de {col_moyenne}')
        plt.legend()
    
    plt.title(f'{col_y} en fonction de {col_x}', fontsize=14)
    plt.xlabel(col_x, fontsize=12)
    plt.ylabel(col_y, fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.5)

    if log_scale:
        plt.xscale('log')
        plt.yscale('log')
        plt.title(f'{col_y} en fonction de {col_x} (Échelle Logarithmique)', fontsize=14)

    plt.xlim(0,lim)

    plt.show()


def tracer_scatter_bloc(data, bloc, couleurs, col_x, col_y, titre):
    plt.figure(figsize=(13, 8))

    # On crée le graphique
    ax = sns.scatterplot(
        data=data, 
        x=col_x, 
        y=col_y, 
        hue=bloc, 
        palette=couleurs, 
        alpha=0.6, 
        s=20, 
        edgecolor='w', 
        linewidth=0.5
    )

    # Configuration des titres
    plt.title(titre, fontsize=15, pad=20)
    plt.xlabel(col_x, fontsize=13)
    plt.ylabel(col_y, fontsize=13)
    
    # Limites de l'axe X
    plt.xlim(0, 2.5) 

    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1), title='Bloc Majoritaire', frameon=False)

    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()

    plt.show()

import pandas as pd
import matplotlib.pyplot as plt

def top_communes_barres(data, col_top, col_blocs, n, minimum):
    if minimum: 
        top_comm = data.sort_values(by=col_top, ascending=True).head(n)
    else : 
        top_comm = data.sort_values(by=col_top, ascending=False).head(n)

    # Calcul des moyennes pour la liste des colonnes (col_blocs doit être une liste)
    moyenne_absten = top_comm[col_blocs].mean()
    moyenne_totale = data[col_blocs].mean()

    df_plot = pd.DataFrame({
        f'Top {n} {col_top}': moyenne_absten,
        'Moyenne France': moyenne_totale
    })

    # Génération du graphique
    ax = df_plot.plot(kind='bar', figsize=(14, 7), color=['#006633', '#8B0000'], width=0.8)


    plt.title(f"Comparaison du vote par bloc : {col_top} vs France Entière", fontsize=15, pad=20)
    plt.ylabel("Score moyen (%)", fontsize=12)
    plt.xlabel("Blocs Politiques", fontsize=12)


    new_labels = [str(col).replace('ratio', '').replace('pvote', '') for col in df_plot.index]
    plt.xticks(range(len(df_plot)), new_labels, rotation=0)

    plt.legend(title="Zones comparées", fontsize=11)
    plt.grid(axis='y', linestyle='--', alpha=0.5)

    for p in ax.patches:
        height = p.get_height()
        ax.annotate(f'{height:.1f}%', 
                    (p.get_x() + p.get_width() / 2., height), 
                    ha='center', va='center', 
                    xytext=(0, 9), textcoords='offset points', fontsize=9)

    plt.tight_layout()
    plt.show()


def comparer_structure_vote(data, col_critere, blocs_vote, dict_couleurs, n=10, ascending=False, titre=""):
    # 1. Sélection des données
    selection = data.sort_values(by=col_critere, ascending=ascending).head(n)
    
    # 2. Calcul de la moyenne nationale
    moyenne_nationale = data[blocs_vote].mean().to_frame().T
    moyenne_nationale.index = ['MOYENNE FRANCE']
    
    # 3. Préparation du DataFrame
    df_communes = selection.set_index('codecommune')[blocs_vote]
    df_plot = pd.concat([moyenne_nationale, df_communes])
    
    # --- LA LIGNE MAGIQUE POUR RÉGLER LA TAILLE DES BARRES ---
    # Cette ligne transforme tes données en parts relatives (0 à 100%)
    # Même si tes données sont des ratios (0.2) ou des % (20), elles rempliront la barre
    df_plot_norm = df_plot.div(df_plot.sum(axis=1), axis=0) * 100

    # 4. Attribution des couleurs
    # On s'assure que l'ordre des couleurs suit l'ordre des colonnes du DataFrame
    liste_couleurs = [dict_couleurs.get(col, '#808080') for col in df_plot_norm.columns]

    # 5. Création du graphique
    ax = df_plot_norm.plot(
        kind='barh', 
        stacked=True, 
        color=liste_couleurs, 
        figsize=(14, 8),
        width=0.8,
        edgecolor='white',
        linewidth=0.5
    )

    # Réglages des axes
    plt.gca().invert_yaxis()
    plt.xlim(0, 100) # L'axe X va maintenant de 0 à 100% proprement
    
    # Légende
    handles, labels = ax.get_legend_handles_labels()
    clean_labels = [l.replace('pvote', '') for l in labels]
    plt.legend(handles, clean_labels, title='Blocs', bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.title(titre if titre else f"Structure du vote par {col_critere}", fontsize=15)
    plt.xlabel("Part des suffrages exprimés (%)")
    plt.tight_layout()
    plt.show()

def boxplot(data, blocs):
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=data[blocs], palette="Set3")

    plt.title("Distribution des scores par bloc politique", fontsize=14)
    plt.ylabel("voix en %")
    plt.xticks(rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.show()

def histo_CG_D(data):
    plt.figure(figsize=(10, 6))
    sns.histplot(data['pvoteCGratio'], kde=True, color='pink', label='Centre-Gauche')
    sns.histplot(data['pvoteDratio'], kde=True, color='blue', label='Droite', alpha=0.3)

    plt.title("Densité des votes : Y a-t-il un consensus ?")
    plt.xlabel("Score obtenu dans la commune")
    plt.ylabel("Nombre de communes")
    plt.legend()
    plt.show()