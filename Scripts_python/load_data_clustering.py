import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

"""
Ce fichier contient les fonctions utiles au loading des données pour les différentes méthodes de clustering.
"""

def load_data(raw_data):
    """
    Prépare les données : sélection des variables numériques, gestion des infinis/NA, normalisation.
    """
    df_travail = raw_data.copy()

    colonnes_id = ['codecommune', 'dep']
    for col in colonnes_id:
        if col in df_travail.columns:
            # On utilise set_index pour les cacher, append=True pour ne pas écraser les précédents
            df_travail = df_travail.set_index(col, append=True)

    # On force la sélection des nombres et on gère les infinis/NA
    df_numerique = (
        df_travail.select_dtypes(include=[np.number]) # Exclusion stricte du texte
        .replace([np.inf, -np.inf], np.nan)           # Sécurité divisions par zéro
        .fillna(0)                                    # Remplacement des vides
    )

    # On normalise
    scaler = StandardScaler()
    matrice_scaled = scaler.fit_transform(df_numerique)
    donnees_clustering = pd.DataFrame(
        matrice_scaled, 
        columns=df_numerique.columns, 
        index=df_numerique.index
    )

    print(f'Données prêtes : {donnees_clustering.shape[0]:,} communes x {donnees_clustering.shape[1]} variables')
    return donnees_clustering

def load_raw_data(raw_data):
    df_travail = raw_data.copy()

    # On place explicitement les identifiants en index (s'ils existent encore en colonnes)
    colonnes_id = ['codecommune', 'dep']
    for col in colonnes_id:
        if col in df_travail.columns:
            # On utilise set_index pour les cacher, append=True pour ne pas écraser les précédents
            df_travail = df_travail.set_index(col, append=True)

    # On force la sélection des nombres et on gère les infinis/NA
    df_numerique = (
        df_travail.select_dtypes(include=[np.number]) # Exclusion stricte du texte
        .replace([np.inf, -np.inf], np.nan)           # Sécurité divisions par zéro
        .fillna(0)                                    # Remplacement des vides
    )
    return df_travail

def sample_data(data, sample_stride):
    """
    Echantillonne les données pour réduire la taille du dataset.
    
    Parameters:
    data (pd.DataFrame): Le dataset d'origine.
    sample_stride (int): Le pas de l'échantillon souhaitée.
    
    Returns:
    pd.DataFrame: L'échantillon du dataset.
    """
    sample_data = data[::sample_stride].copy()

    print(f'Taille du sample : {sample_data.shape[0]:,} communes (1/{sample_stride})')
    print(f'Taille des données originales : {data.shape[0]:,} communes')

    return sample_data