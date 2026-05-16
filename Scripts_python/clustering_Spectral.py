import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import seaborn as sns

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import SpectralClustering
from sklearn.neighbors import kneighbors_graph, KNeighborsClassifier
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.decomposition import PCA

import scipy.sparse as sp
import scipy.sparse.linalg as spla
import scipy.cluster.hierarchy as sch

import geopandas as gpd
import warnings
warnings.filterwarnings('ignore')

"""
Ce fichier contient les fonctions utiles au clustering Spectral.
"""

def compute_laplacian_sym_eigenvalues(X, n_neighbors=7, n_eigvals=15):
    """
    Construit le graphe k-NN symétrique, calcule le Laplacien normalisé
    et retourne ses premières valeurs propres triées.
    """
    A = kneighbors_graph(X, n_neighbors=n_neighbors,
                         mode='connectivity', include_self=False)
    A = A + A.T
    A.data = np.ones_like(A.data)  # poids binaires

    degrees = np.array(A.sum(axis=1)).flatten()
    D_inv_sqrt = sp.diags(1.0 / np.sqrt(np.maximum(degrees, 1e-10)))

    # Laplacien normalisé : L_sym = I - D^{-1/2} A D^{-1/2}
    L_sym = sp.eye(A.shape[0]) - D_inv_sqrt @ A @ D_inv_sqrt

    eigenvalues, _ = spla.eigsh(L_sym, k=n_eigvals, which='SM')

    return np.sort(np.real(eigenvalues)), n_neighbors, n_eigvals


def compute_laplacian_rw_eigenvalues(X, n_neighbors=7, n_eigvals=15):
    """
    Construit le graphe k-NN symétrique, calcule le Laplacien random walk
    et retourne ses premières valeurs propres triées.
    """
    A = kneighbors_graph(X, n_neighbors=n_neighbors,
                         mode='connectivity', include_self=False)
    A = A + A.T
    A.data = np.ones_like(A.data)  # poids binaires

    degrees = np.array(A.sum(axis=1)).flatten()
    D_inv = sp.diags(1.0 / np.maximum(degrees, 1e-10))

    # Laplacien random walk : L_rw = I - D^{-1} A
    L_rw = sp.eye(A.shape[0]) - D_inv @ A

    eigenvalues, _ = spla.eigsh(L_rw, k=n_eigvals, which='SM')
    return np.sort(np.real(eigenvalues)), n_neighbors, n_eigvals

def print_eigenvalues(eigenvalues, n_neighbors, n_eigvals):
    print(f'Calcul des valeurs propres (n_neighbors={n_neighbors} | n_eigvals={n_eigvals})...')
    print(np.round(eigenvalues, 4))

def plot_spectrum(eigenvalues,):
    gaps = np.diff(eigenvalues[:])
    K_eigengap = int(np.argmax(gaps)) + 1  # indice avant le plus grand saut → K suggéré

    plt.plot(range(1, len(eigenvalues)+1), eigenvalues, 'o-',
            color='steelblue', linewidth=2, markersize=6)
    plt.axvline(x=K_eigengap, color='red', linestyle='--', linewidth=1.5,
            label=f'K suggéré = {K_eigengap}')
    plt.set_xlabel('Indice de la valeur propre')
    plt.set_ylabel('Valeur propre λ')
    plt.set_title('Spectre du Laplacien normalisé')
    plt.legend(); plt.grid(alpha=0.3)


def plot_eigengap(eigenvalues):
    gaps = np.diff(eigenvalues[:])
    K_eigengap = int(np.argmax(gaps)) + 1  # indice avant le plus grand saut → K suggéré

    
    colors_bar = ['red' if i == K_eigengap - 1 else 'steelblue' for i in range(len(gaps))]
    plt.bar(range(1, len(gaps)+1), gaps, color=colors_bar, alpha=0.8)
    plt.set_xlabel('k')
    plt.set_ylabel('λ(k+1) − λ(k)')
    plt.set_title('Eigengap — saut entre valeurs propres consécutives')
    plt.legend(handles=[mpatches.Patch(color='red', label=f'Plus grand saut → K={K_eigengap}')])
    plt.grid(alpha=0.3)

    plt.suptitle('Choix de K par la méthode eigengap', fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.show()

    print(f'\n→ K suggéré par l\'eigengap : {K_eigengap}')


def choix_n_neighbors(X, n_neighbors_list=[5, 7, 10], n_eigvals=15):


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
    ax.set_title("Elbow method - Agglomerative Clustering")
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


def plot_dendrogram(data, nb_clusters, method='ward', ax=None):
    """
    Affiche le dendrogramme de la classification hiérarchique.
    """
    K = nb_clusters
    Z = sch.linkage(data, method=method)
    seuil_coupure = Z[-(K-1), 2]

    # Si un axe est fourni (subplot), on travaille dessus. Sinon, on prend l'axe courant.
    if ax is None:
        ax = plt.gca()

    ax.set_title(f"Dendrogramme CAH - {method} - K={K}")
    ax.set_xlabel("Index des communes")
    ax.set_ylabel(f"Distance de {method}")

    sch.dendrogram(
        Z,
        labels=data.index.get_level_values('codecommune').values,
        leaf_rotation=90.,
        leaf_font_size=8.,
        color_threshold=seuil_coupure,
        ax=ax # On force le dendrogramme à se dessiner sur le bon subplot
    )

    # Ligne de coupure des K clusters
    ax.axhline(y=seuil_coupure, color='r', linestyle='--', linewidth=1.5,
               label=f'Coupure à {K} clusters (seuil : {seuil_coupure:.1f})')
    ax.legend(fontsize=8)

def plot_carte_cah(
    data: pd.DataFrame,      # données normalisées complètes (34 870 × 72)
    nb_clusters: int,
    method: str = 'ward'
) -> tuple[plt.Figure, pd.DataFrame, np.ndarray]:

    K = nb_clusters

    # 1. CAH sur les données COMPLÈTES (comme donnees_clustering dans le notebook)
    Z = sch.linkage(data, method=method)
    labels_cah = sch.fcluster(Z, K, criterion='maxclust') - 1  # [0..K-1]
    n_total = len(data)

    # 2. DataFrame carte - aligné sur data (même index que raw_data)
    df_carte_cluster = data.reset_index().copy()
    df_carte_cluster['codecommune'] = (
        df_carte_cluster['codecommune'].astype(str).str.zfill(5)
    )
    df_carte_cluster['label_cah'] = labels_cah   # même longueur : OK
    df_carte_cluster['Nom_Cluster'] = df_carte_cluster['label_cah'].apply(
        lambda l: f'Cluster {l}'
    )

    # ... reste inchangé

    # 3. GeoData
    url_geojson = (
        "https://raw.githubusercontent.com/gregoiredavid/"
        "france-geojson/master/communes.geojson"
    )
    france_communes = gpd.read_file(url_geojson)
    carte_data = france_communes.merge(
        df_carte_cluster, left_on='code', right_on='codecommune'
    )

    # 4. Couleurs dynamiques
    cmap_clusters = plt.get_cmap('Set1', K)
    categories    = [f'Cluster {i}' for i in range(K)]
    couleurs_dict = {cat: mcolors.to_hex(cmap_clusters(i))
                     for i, cat in enumerate(categories)}
    cmap_custom   = mcolors.ListedColormap([couleurs_dict[c] for c in categories])

    # 5. Carte
    fig, ax = plt.subplots(1, 1, figsize=(15, 15), dpi=150)
    carte_data.plot(
        column='Nom_Cluster', ax=ax,
        categorical=True, categories=categories,
        cmap=cmap_custom, legend=False,
        linewidth=0, edgecolor='none',
        missing_kwds={'color': '#eeeeee', 'label': 'Données manquantes'}
    )

    # 6. Légende avec effectifs
    handles = [
        mpatches.Patch(
            color=couleurs_dict[cat],
            label=f'{cat}  ({(df_carte_cluster["Nom_Cluster"]==cat).sum():,}'
                  f' communes, '
                  f'{(df_carte_cluster["Nom_Cluster"]==cat).sum()/n_total*100:.1f}%)'
        )
        for cat in categories
    ]
    ax.legend(handles=handles, title=f"CAH {method.capitalize()}- K={K}",
              loc='upper left', bbox_to_anchor=(1, 1),
              frameon=False, fontsize=11, title_fontsize=12)
    ax.set_axis_off()
    plt.title(f"Carte de France - CAH (K={K}, méthode={method})",
              fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()

    return fig, df_carte_cluster, labels_cah