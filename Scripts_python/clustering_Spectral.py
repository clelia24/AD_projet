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

def compute_laplacian_sym_eigenvalues(data, n_neighbors=7, n_eigvals=15):
    """
    Construit le graphe k-NN symétrique, calcule le Laplacien normalisé
    et retourne ses premières valeurs propres triées.
    """
    A = kneighbors_graph(data, n_neighbors=n_neighbors,
                         mode='connectivity', include_self=False)
    A = A + A.T
    A.data = np.ones_like(A.data)  # poids binaires

    degrees = np.array(A.sum(axis=1)).flatten()
    D_inv_sqrt = sp.diags(1.0 / np.sqrt(np.maximum(degrees, 1e-10)))

    # Laplacien normalisé : L_sym = I - D^{-1/2} A D^{-1/2}
    L_sym = sp.eye(A.shape[0]) - D_inv_sqrt @ A @ D_inv_sqrt

    eigenvalues, _ = spla.eigsh(L_sym, k=n_eigvals, which='SM')

    return np.sort(np.real(eigenvalues)), n_neighbors, n_eigvals


def compute_laplacian_rw_eigenvalues(data, n_neighbors=7, n_eigvals=15):
    """
    Construit le graphe k-NN symétrique, calcule le Laplacien random walk
    et retourne ses premières valeurs propres triées.
    """
    A = kneighbors_graph(data, n_neighbors=n_neighbors,
                         mode='connectivity', include_self=False)
    A = A + A.T
    A.data = np.ones_like(A.data)  # poids binaires

    degrees = np.array(A.sum(axis=1)).flatten()
    D_inv = sp.diags(1.0 / np.maximum(degrees, 1e-10))

    # Laplacien random walk : L_rw = I - D^{-1} A
    L_rw = sp.eye(A.shape[0]) - D_inv @ A

    eigenvalues, _ = spla.eigsh(L_rw, k=n_eigvals, which='SM')
    return np.sort(np.real(eigenvalues))

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


def test_sensitivity_neighbors(data, K, n_neighbors_list=[5, 7, 9, 10, 15, 20]):
    neighbors_range  = n_neighbors_list

    resultats_neighbors = []

    for nn in neighbors_range:
        model = SpectralClustering(
            n_clusters=K,
            affinity='nearest_neighbors',
            n_neighbors=nn,
            assign_labels='kmeans',
            random_state=42,
            n_jobs=-1
        )
        labels = model.fit_predict(data.values)

        sil = silhouette_score(data.values, labels, sample_size=2000, random_state=42)
        db  = davies_bouldin_score(data.values, labels)
        ch  = calinski_harabasz_score(data.values, labels)

        resultats_neighbors.append({
            'n_neighbors': nn,
            'silhouette': round(sil, 4),
            'davies_bouldin': round(db, 4),
            'calinski_harabasz': round(ch, 1)
        })
        # print(f'n_neighbors={nn:2d} | Silhouette={sil:.4f} | DB={db:.4f} | CH={ch:.1f}')

    df_neighbors = pd.DataFrame(resultats_neighbors).set_index('n_neighbors')
    df_neighbors

def plot_sensitivity_neighbors(df_neighbors, K):
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    metrics = [
        ('silhouette',        'Silhouette (↑)',        'green'),
        ('davies_bouldin',    'Davies-Bouldin (↓)',     'red'),
        ('calinski_harabasz', 'Calinski-Harabasz (↑)',  'steelblue'),
    ]

    for ax, (col, title, color) in zip(axes, metrics):
        ax.plot(df_neighbors.index, df_neighbors[col], 'o-', color=color, linewidth=2)
        ax.set_xlabel('n_neighbors')
        ax.set_title(title)
        ax.grid(alpha=0.3)

    plt.suptitle(f'Sensibilite à n_neighbors (K={K})', fontsize=12, fontweight='bold')
    plt.tight_layout()
    plt.show()

def test_sensitivity_clusters(data, n_neighbors, K_range=range(2, 9)):
    K_RANGE           = K_range

    resultats_k = []

    for k in K_RANGE:
        model = SpectralClustering(
            n_clusters=k,
            affinity='nearest_neighbors',
            n_neighbors=n_neighbors,
            assign_labels='kmeans',
            random_state=42,
            n_jobs=-1
        )
        labels = model.fit_predict(data.values)

        sil = silhouette_score(data.values, labels, sample_size=2000, random_state=42)
        db  = davies_bouldin_score(data.values, labels)
        ch  = calinski_harabasz_score(data.values, labels)

        resultats_k.append({'K': k, 'silhouette': sil, 'davies_bouldin': db,
                            'calinski_harabasz': ch})
        # print(f'K={k} | Silhouette={sil:.4f} | DB={db:.4f} | CH={ch:.1f}')

    df_k = pd.DataFrame(resultats_k).set_index('K')
    df_k.round(4)

def plot_sensitivity_clusters(df_k, K_eigengap):
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    metrics = [
        ('silhouette',        'Silhouette (↑)',        'green'),
        ('davies_bouldin',    'Davies-Bouldin (↓)',     'red'),
        ('calinski_harabasz', 'Calinski-Harabasz (↑)',  'steelblue'),
    ]

    for ax, (col, title, color) in zip(axes, metrics):
        ax.plot(df_k.index, df_k[col], 'o-', color=color, linewidth=2)
        ax.axvline(x=K_eigengap, color='orange', linestyle='--', linewidth=1.5,
                label=f'Eigengap → K={K_eigengap}')
        ax.set_xlabel('K')
        ax.set_title(title)
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)

    plt.suptitle('Métriques de clustering selon K', fontsize=12, fontweight='bold')
    plt.tight_layout()
    plt.show()

def spectral_clustering(data, K, n_neighbors):
    N = data.shape[0]
    print(f'Nombre total de communes : {N:,}')

    cols_a_exclure = ['label_spectral']
    data = data.drop(columns=cols_a_exclure, errors='ignore')

    feature_cols = data.columns.tolist()
    print(f'Features utilisées : {len(feature_cols)} colonnes')

    model_final = SpectralClustering(
        n_clusters=K,
        affinity='nearest_neighbors',
        n_neighbors=n_neighbors,
        assign_labels='cluster_qr',
        random_state=42,
        n_jobs=-1
    )
    labels_spectral = model_final.fit_predict(data[feature_cols].values)

    data['label_spectral'] = labels_spectral

    print('\nDistribution des clusters :')
    dist = pd.Series(labels_spectral).value_counts().sort_index()
    for k, n in dist.items():
        print(f'  Cluster {k} : {n:,} communes ({n/N*100:.1f}%)')

    # évaluation des performances du clustering spectral
    idx_eval = np.random.choice(N, size=min(5000, N), replace=False)
    X_eval   = data.drop(columns='label_spectral').values[idx_eval]
    y_eval   = labels_spectral[idx_eval]

    sil_final = silhouette_score(X_eval, y_eval)
    db_final  = davies_bouldin_score(X_eval, y_eval)
    ch_final  = calinski_harabasz_score(X_eval, y_eval)

    print(f'Métriques finales  (K={K}, n_neighbors={n_neighbors})')
    print(f'  Silhouette        : {sil_final:.4f}  (↑ mieux, max=1)')
    print(f'  Davies-Bouldin    : {db_final:.4f}  (↓ mieux, min=0)')
    print(f'  Calinski-Harabasz : {ch_final:.1f} (↑ mieux)')

    return labels_spectral


def profils_par_cluster(df_numerique, labels_spectral):
    # ── Profils moyens par cluster (variables originales) ─────────────
    df_profil = df_numerique.copy()
    df_profil['label_spectral'] = labels_spectral

    profils = df_profil.groupby('label_spectral').mean()
    profils.round(3)
    

def plot_carte_spectral(data, labels_spectral, K, n_neighbors):
    # ── Préparation ───────────────────────────────────────────────────
    df_carte = data.reset_index().copy()
    df_carte['codecommune'] = df_carte['codecommune'].astype(str).str.zfill(5)
    df_carte['label_spectral'] = labels_spectral
    df_carte['Nom_Cluster'] = df_carte['label_spectral'].apply(lambda l: f'Cluster {l}')
    n_total = len(df_carte)

    # ── GeoJSON ────────────────────────────────────────────────────────
    url_geojson = 'https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes.geojson'
    france_communes = gpd.read_file(url_geojson)
    carte_data = france_communes.merge(df_carte, left_on='code', right_on='codecommune')

    # ── Couleurs ────────────────────────────────────────────────────────
    cmap_clusters    = plt.get_cmap('Set1', K)
    couleurs_dict    = {f'Cluster {i}': mcolors.to_hex(cmap_clusters(i)) for i in range(K_FINAL)}
    categories_finales = [f'Cluster {i}' for i in range(K)]
    cmap_custom      = mcolors.ListedColormap([couleurs_dict[c] for c in categories_finales])

    # ── Carte ───────────────────────────────────────────────────────────
    fig, ax = plt.subplots(1, 1, figsize=(15, 15), dpi=150)

    carte_data.plot(
        column='Nom_Cluster', ax=ax,
        categorical=True, categories=categories_finales,
        cmap=cmap_custom, legend=False,
        linewidth=0, edgecolor='none',
        missing_kwds={'color': '#eeeeee', 'label': 'Données manquantes'}
    )

    # Légende manuelle avec effectifs
    handles = []
    for cat in categories_finales:
        n = (df_carte['Nom_Cluster'] == cat).sum()
        pct = n / n_total * 100
        patch = mpatches.Patch(color=couleurs_dict[cat],
                            label=f'{cat}  ({n:,} communes, {pct:.1f}%)')
        handles.append(patch)

    ax.legend(
        handles=handles,
        title=f'Clustering Spectral — K={K}, n_neighbors={n_neighbors}',
        loc='upper left', bbox_to_anchor=(1, 1),
        frameon=False, fontsize=11, title_fontsize=12
    )

    ax.set_axis_off()
    plt.title(f'Carte de France — Clustering Spectral (K={K} clusters)',
            fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.show()

