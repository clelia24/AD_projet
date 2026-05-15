import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
import scipy.cluster.hierarchy as sch
from yellowbrick.cluster import KElbowVisualizer
import geopandas as gpd
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches

"""
Ce fichier contient les fonctions utiles au clustering CAH.
"""

def load_data(raw_data):
    """
    Prépare les données pour le clustering CAH : sélection des variables numériques, gestion des infinis/NA, normalisation.
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

def plot_elbow_method_graph(data, nb_clusters_max):
    """
    Affiche le graphique de la méthode du coude pour déterminer le nombre optimal de clusters.
    """
    ac = AgglomerativeClustering(linkage="ward", compute_distances=True)
    ac.fit(data)

    n_sizes = nb_clusters_max
    x = np.arange(n_sizes, 0, -1)
    y = ac.distances_[-n_sizes:]

    fig, ax = plt.subplots(figsize=(8, 4))

    ax.plot(x, y, marker="o", color="steelblue", linewidth=2, markersize=6)

    ax.set_xlabel("Number of clusters")
    ax.set_ylabel("Merge distance")
    ax.set_title("Elbow method — Agglomerative Clustering")
    ax.set_xticks(x)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(True, linestyle="--", alpha=0.3)

    plt.tight_layout()
    plt.show()

def plot_elbow_method_yellowbrick(data, metric):
    """
    Affiche le graphique de la méthode du coude avec le package yellowbrick pour déterminer le nombre optimal de clusters.
    Parameters:
    data (pd.DataFrame): Le dataset à analyser.
    metric (str): La métrique à utiliser pour évaluer les clusters (ex: 'silhouette', 'calinski_harabasz', 'davies_bouldin').
    """
    ac = AgglomerativeClustering(linkage='ward', compute_distances=True)
    visualizer = KElbowVisualizer(ac, k=(2,12), metric=metric, force_model=True)

    visualizer.fit(data) 
    visualizer.show()   
    plt.show()

def plot_dendrogram(data, nb_clusters, method='ward'):
    """
    Affiche le dendrogramme de la classification hiérarchique.
    Parameters:
    data (pd.DataFrame): Le dataset à analyser.
    nb_clusters (int): Le nombre de clusters souhaité.
    method (str): La méthode de regroupement à utiliser (ex: 'ward', 'complete', 'average').
    """
    K = nb_clusters

    Z = sch.linkage(data, method='ward')

    seuil_coupure = Z[-(K-1), 2]

    plt.figure(figsize=(15, 8))
    plt.title(f"Dendrogramme de la Classification Hiérarchique (Échantillon) — K={K} clusters")
    plt.xlabel("Index des communes (ou codecommune)")
    plt.ylabel("Distance de Ward")

    sch.dendrogram(
        Z,
        labels=data.index.get_level_values('codecommune').values,
        leaf_rotation=90.,
        leaf_font_size=8.,
        color_threshold=seuil_coupure 
    )

    # ligne de coupure des K clusters
    plt.axhline(y=seuil_coupure, color='r', linestyle='--', linewidth=1.5,
                label=f'Coupure → {K} clusters (seuil ≈ {seuil_coupure:.1f})')
    plt.legend(fontsize=10)

    plt.tight_layout()
    plt.show()

def plot_carte_dendogramme(data, raw_data, nb_clusters, method='ward'):
    """
    Affiche la carte de France avec les clusters CAH attribués à chaque commune.
    """

    K = nb_clusters
    
    # Calcul du linkage sur les données complètes (pas seulement le sample)
    Z_full = sch.linkage(data, method=method)

    # Attribution des labels CAH (fcluster retourne des labels 1..K, on ramène à 0..K-1)
    labels_cah = sch.fcluster(Z_full, K, criterion='maxclust') - 1

    n_total = len(data)

    
    df_carte_cluster = raw_data.reset_index().copy()
    df_carte_cluster['codecommune'] = df_carte_cluster['codecommune'].astype(str).str.zfill(5)

    # Rattachement des labels CAH
    # labels_cah est aligné sur donnees_clustering (même ordre que df_travail)
    df_carte_cluster['label_cah'] = labels_cah

    # Colonne texte pour la légende
    df_carte_cluster['Nom_Cluster'] = df_carte_cluster['label_cah'].apply(lambda l: f'Cluster {l}')

    # plot carte :

    url_geojson = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes.geojson"
    france_communes = gpd.read_file(url_geojson)

    carte_data = france_communes.merge(df_carte_cluster, left_on='code', right_on='codecommune')

    cmap_clusters = plt.get_cmap('Set1', K)

    couleurs_dict = {}
    for i in range(K):
        couleurs_dict[f'Cluster {i}'] = mcolors.to_hex(cmap_clusters(i))

    categories_finales = [f'Cluster {i}' for i in range(K)]
    couleurs_liste = [couleurs_dict[cat] for cat in categories_finales]
    cmap_custom = mcolors.ListedColormap(couleurs_liste)

    fig, ax = plt.subplots(1, 1, figsize=(15, 15), dpi=150)

    carte_data.plot(
        column='Nom_Cluster',
        ax=ax,
        categorical=True,
        categories=categories_finales,
        cmap=cmap_custom,
        legend=False,
        linewidth=0,
        edgecolor='none',
        missing_kwds={
            'color': '#eeeeee',
            'label': 'Données manquantes'
        }
    )

    handles = []
    for cat in categories_finales:
        n = (df_carte_cluster['Nom_Cluster'] == cat).sum()
        pct = n / n_total * 100
        label_legende = f'{cat}  ({n:,} communes, {pct:.1f}%)'
        patch = mpatches.Patch(color=couleurs_dict[cat], label=label_legende)
        handles.append(patch)

    ax.legend(
        handles=handles,
        title=f"CAH Ward — K={K} clusters",
        loc='upper left',
        bbox_to_anchor=(1, 1),
        frameon=False,
        fontsize=11,
        title_fontsize=12
    )

    ax.set_axis_off()
    plt.title(
        f"Carte de France — Classification Hiérarchique Ascendante (K={K} clusters)",
        fontsize=16, fontweight='bold', pad=20
    )

    plt.tight_layout()
    plt.show()