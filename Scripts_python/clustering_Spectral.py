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
from sklearn.cluster import KMeans

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
    Construit le graphe k-NN symetrique, calcule le Laplacien symetrique normalise
    et retourne ses premières valeurs propres triees.
    """
    A = kneighbors_graph(data, n_neighbors=n_neighbors,
                         mode='connectivity', include_self=False)
    A = A + A.T
    A.data = np.ones_like(A.data)  # poids binaires

    degrees = np.array(A.sum(axis=1)).flatten()
    D_inv_sqrt = sp.diags(1.0 / np.sqrt(np.maximum(degrees, 1e-10)))

    # Laplacien normalise : L_sym = I - D^{-1/2} A D^{-1/2}
    L_sym = sp.eye(A.shape[0]) - D_inv_sqrt @ A @ D_inv_sqrt

    eigenvalues, _ = spla.eigsh(L_sym, k=n_eigvals, which='SM')

    return np.sort(np.real(eigenvalues))


def compute_laplacian_rw_eigenvalues(data, n_neighbors=7, n_eigvals=15):
    """
    Construit le graphe k-NN symetrique, calcule le Laplacien random walk
    et retourne ses premières valeurs propres triees.
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
    """
    Affiche les valeurs propres calculées pour le Laplacien normalisé ou random walk.
    """
    print(f'Calcul des valeurs propres (n_neighbors={n_neighbors} | n_eigvals={n_eigvals})...')
    print(np.round(eigenvalues, 4))

def build_rw_affinity(data, n_neighbors):
    """
    Construit la matrice d'affinité pour le clustering spectral random walk.
    """
    A = kneighbors_graph(data, n_neighbors=n_neighbors,
                         mode='connectivity', include_self=False)
    A = A + A.T
    A.data = np.ones_like(A.data)
    degrees = np.array(A.sum(axis=1)).flatten()
    D_inv = sp.diags(1.0 / np.maximum(degrees, 1e-10))
    P = D_inv @ A  # matrice de transition random walk
    return P.toarray()

def plot_spectrum(eigenvalues):
    """Affiche le spectre des valeurs propres du Laplacien normalisé et indique le K suggéré par la méthode de l'eigengap.
    """
    gaps = np.diff(eigenvalues[1:])
    K_eigengap = int(np.argmax(gaps)) + 2

    x_positions = range(2, len(eigenvalues)+1)  # commence a 2, on ignore λ₁
    plt.plot(x_positions, eigenvalues[1:], 'o-',  # eigenvalues[1:] pour aligner
            color='steelblue', linewidth=2, markersize=6)
    plt.axvline(x=K_eigengap, color='red', linestyle='--', linewidth=1.5,
            label=f'K suggere = {K_eigengap}')
    plt.xlabel('Indice de la valeur propre')
    plt.ylabel('Valeur propre λ')
    plt.title('Spectre du Laplacien normalise')
    plt.legend(); plt.grid(alpha=0.3)


def plot_eigengap(eigenvalues):
    """
    Affiche le gap entre les valeurs propres consécutives du Laplacien normalisé et indique le K suggéré par la méthode de l'eigengap.
    """
    gaps = np.diff(eigenvalues[1:])
    K_eigengap = int(np.argmax(gaps)) + 2  # +2 car eigenvalues[1:] decale de 1, et gap entre k et k+1 decale encore de 1

    x_positions = range(2, len(gaps)+2)  # les barres commencent a 2
    colors_bar = ['red' if x == K_eigengap else 'steelblue' for x in x_positions]  # comparaison directe sur x

    plt.figure(figsize=(10, 6))
    plt.bar(x_positions, gaps, color=colors_bar, alpha=0.8)
    plt.xlabel('k')
    plt.ylabel('λ(k+1) - λ(k)')
    plt.title('Eigengap - saut entre valeurs propres consecutives')
    plt.legend(handles=[mpatches.Patch(color='red', label=f'Plus grand saut : K={K_eigengap}')])
    plt.grid(alpha=0.3)

    plt.suptitle('Choix de K par la methode eigengap', fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.show()

    print(f'\n→ K suggere par l\'eigengap : {K_eigengap}')


def test_sensitivity_neighbors(data, K, n_neighbors_list=[5, 7, 9, 10, 15, 20]):
    """
    Teste la sensibilité du clustering spectral à la valeur de n_neighbors en affichant les métriques de qualité pour chaque valeur testée.
    """
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
    df_neighbors.round(4)
    return df_neighbors

def test_sensitivity_neighbors_rw(data, K, n_neighbors_list=[5, 7, 9, 10, 15, 20]):
    """
    Teste la sensibilité du clustering spectral random walk à la valeur de n_neighbors en affichant les métriques de qualité pour chaque valeur testée.
    """
    neighbors_range  = n_neighbors_list

    resultats_neighbors = []

    for nn in neighbors_range:
        affinity_matrix = build_rw_affinity(data.values, nn)
        model = SpectralClustering(
            n_clusters=K,
            affinity='precomputed',
            assign_labels='kmeans',
            random_state=42
        )
        labels = model.fit_predict(affinity_matrix)

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
    df_neighbors.round(4)
    # print(df_neighbors)
    return df_neighbors

def plot_sensitivity_neighbors(df_neighbors, K):
    """
    Affiche les métriques de qualité du clustering spectral en fonction de n_neighbors pour un K donné.
    """
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

    plt.suptitle(f'Sensibilite a n_neighbors (K={K})', fontsize=12, fontweight='bold')
    plt.tight_layout()
    plt.show()

def test_sensitivity_clusters(data, n_neighbors, K_range=range(2, 9)):
    """
    Teste la sensibilité du clustering spectral à la valeur de K en affichant les métriques de qualité pour chaque valeur testée.
    """
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

def test_sensitivity_clusters_rw(data, n_neighbors, K_range=range(2, 9)):
    """
    Teste la sensibilité du clustering spectral random walk à la valeur de K en affichant les métriques de qualité pour chaque valeur testée.
    """
    K_RANGE           = K_range

    resultats_k = []

    for k in K_RANGE:
        affinity_matrix = build_rw_affinity(data.values, n_neighbors)
        model = SpectralClustering(
            n_clusters=k,
            affinity='precomputed',
            assign_labels='kmeans',
            random_state=42
        )
        labels = model.fit_predict(affinity_matrix)

        sil = silhouette_score(data.values, labels, sample_size=2000, random_state=42)
        db  = davies_bouldin_score(data.values, labels)
        ch  = calinski_harabasz_score(data.values, labels)

        resultats_k.append({'K': k, 'silhouette': sil, 'davies_bouldin': db,
                            'calinski_harabasz': ch})
        # print(f'K={k} | Silhouette={sil:.4f} | DB={db:.4f} | CH={ch:.1f}')

    df_k = pd.DataFrame(resultats_k).set_index('K')
    df_k.round(4)
    return df_k  

def plot_sensitivity_clusters(df_k, K_eigengap):
    """
    Affiche les métriques de qualité du clustering spectral en fonction de K et indique le K suggéré par la méthode de l'eigengap.
    """
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

    plt.suptitle('Metriques de clustering selon K', fontsize=12, fontweight='bold')
    plt.tight_layout()
    plt.show()

def spectral_clustering(data, K, n_neighbors):
    """Effectue un clustering spectral sur les données fournies avec K clusters et n_neighbors pour la construction du graphe k-NN.
    Affiche la distribution des clusters et les métriques de qualité du clustering.
    """
    N = data.shape[0]
    print(f'Nombre total de communes : {N:,}')

    cols_a_exclure = ['label_spectral']
    data = data.drop(columns=cols_a_exclure, errors='ignore')

    feature_cols = data.columns.tolist()
    print(f'Features utilisees : {len(feature_cols)} colonnes')

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

    # evaluation des performances du clustering spectral
    idx_eval = np.random.choice(N, size=min(5000, N), replace=False)
    X_eval   = data.drop(columns='label_spectral').values[idx_eval]
    y_eval   = labels_spectral[idx_eval]

    sil_final = silhouette_score(X_eval, y_eval)
    db_final  = davies_bouldin_score(X_eval, y_eval)
    ch_final  = calinski_harabasz_score(X_eval, y_eval)

    print(f'Metriques finales  (K={K}, n_neighbors={n_neighbors})')
    print(f'  Silhouette        : {sil_final:.4f}  (↑ mieux, max=1)')
    print(f'  Davies-Bouldin    : {db_final:.4f}  (↓ mieux, min=0)')
    print(f'  Calinski-Harabasz : {ch_final:.1f} (↑ mieux)')

    return labels_spectral

def spectral_clustering_rw(data, K, n_neighbors):
    """
    Effectue un clustering spectral random walk sur les données fournies avec K clusters et n_neighbors pour la construction du graphe k-NN.
    Affiche la distribution des clusters et les métriques de qualité du clustering.
    """
    N = data.shape[0]
    print(f'Nombre total de communes : {N:,}')

    cols_a_exclure = ['label_spectral']
    data = data.drop(columns=cols_a_exclure, errors='ignore')

    feature_cols = data.columns.tolist()
    print(f'Features utilisees : {len(feature_cols)} colonnes')

    # Décomposition spectrale manuelle sur L_rw sparse
    # Demander K+1 vecteurs propres et ignorer le premier
    eigenvalues, eigenvectors = spla.eigsh(
        build_rw_affinity(data.values, n_neighbors), 
        k=K+1, which='LM'
    )

    # Trier par valeur propre décroissante et ignorer la première (≈ 1, triviale)
    order = np.argsort(eigenvalues)[::-1]
    eigenvectors = eigenvectors[:, order]
    eigenvectors = eigenvectors[:, 1:]  # ← drop du vecteur trivial

    # Normalisation ligne par ligne avant k-means
    norms = np.linalg.norm(eigenvectors, axis=1, keepdims=True)
    eigenvectors = eigenvectors / np.maximum(norms, 1e-10)

    labels_spectral = KMeans(n_clusters=K, random_state=42, n_init=10).fit_predict(eigenvectors)

    print('\nDistribution des clusters :')
    dist = pd.Series(labels_spectral).value_counts().sort_index()
    for k, n in dist.items():
        print(f'  Cluster {k} : {n:,} communes ({n/N*100:.1f}%)')

    # evaluation des performances du clustering spectral
    idx_eval = np.random.choice(N, size=min(5000, N), replace=False)
    X_eval = data.drop(columns='label_spectral', errors='ignore').values[idx_eval]
    y_eval   = labels_spectral[idx_eval]

    sil_final = silhouette_score(X_eval, y_eval)
    db_final  = davies_bouldin_score(X_eval, y_eval)
    ch_final  = calinski_harabasz_score(X_eval, y_eval)

    print(f'Metriques finales  (K={K}, n_neighbors={n_neighbors})')
    print(f'  Silhouette        : {sil_final:.4f}  (↑ mieux, max=1)')
    print(f'  Davies-Bouldin    : {db_final:.4f}  (↓ mieux, min=0)')
    print(f'  Calinski-Harabasz : {ch_final:.1f} (↑ mieux)')

    return labels_spectral


def profils_par_cluster(df_numerique, labels_spectral):
    """
    Calcule les profils moyens des clusters obtenus par le clustering spectral.
    """
    df_profil = df_numerique.copy()
    df_profil['label_spectral'] = labels_spectral

    profils = df_profil.groupby('label_spectral').mean()
    profils.round(3)

def plot_carte_spectral(data, labels_spectral, K, n_neighbors):
    """
    Affiche une carte de France colorée selon les clusters obtenus par le clustering spectral.
    """
    df_carte = data.reset_index().copy()
    df_carte['codecommune'] = df_carte['codecommune'].astype(str).str.zfill(5)
    df_carte['label_spectral'] = labels_spectral
    df_carte['Nom_Cluster'] = df_carte['label_spectral'].apply(lambda l: f'Cluster {l}')
    n_total = len(df_carte)

    url_geojson = 'https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes.geojson'
    france_communes = gpd.read_file(url_geojson)
    carte_data = france_communes.merge(df_carte, left_on='code', right_on='codecommune')

    cmap_clusters    = plt.get_cmap('Set1', K)
    couleurs_dict    = {f'Cluster {i}': mcolors.to_hex(cmap_clusters(i)) for i in range(K)}
    categories_finales = [f'Cluster {i}' for i in range(K)]
    cmap_custom      = mcolors.ListedColormap([couleurs_dict[c] for c in categories_finales])

    fig, ax = plt.subplots(1, 1, figsize=(15, 15), dpi=150)

    carte_data.plot(
        column='Nom_Cluster', ax=ax,
        categorical=True, categories=categories_finales,
        cmap=cmap_custom, legend=False,
        linewidth=0, edgecolor='none',
        missing_kwds={'color': '#eeeeee', 'label': 'Donnees manquantes'}
    )

    # Legende manuelle avec effectifs
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

