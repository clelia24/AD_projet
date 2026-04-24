import pandas as pd
from pandas import col
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

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

def affichage_hist(data, col): 
    # Configuration du style visuel
    sns.set_theme(style="whitegrid")
    if col=='popagglo2022':
        # On utilise log_scale=True
        sns.histplot(data=data, x=col, bins=50, kde=True, log_scale=True, color='skyblue')

        plt.title(f'Distribution (Échelle Logarithmique) de {col}', fontsize=14)
        plt.xlabel(f'{col} (Log)', fontsize=12)
        plt.ylabel('Fréquence', fontsize=12)
        plt.show()
        return
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
