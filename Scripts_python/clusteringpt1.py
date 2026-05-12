import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
import matplotlib.cm as cm
from sklearn.metrics import silhouette_samples
from sklearn.metrics import calinski_harabasz_score
from sklearn.preprocessing import StandardScaler

def evaluer_clusters(data, methode='kmeans', k_range=range(2, 11), colonnes_id=['codecommune', 'dep'], sample_size=5000):
    df_travail = data.copy()
    
    for col in colonnes_id:
        if col in df_travail.columns:
            df_travail = df_travail.set_index(col, append=True)

    df_numerique = (
        df_travail.select_dtypes(include=[np.number])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    scaler = StandardScaler()
    donnees_clustering = pd.DataFrame(
        scaler.fit_transform(df_numerique), 
        columns=df_numerique.columns, 
        index=df_numerique.index
    )

    scores_metrique = []
    scores_silhouette = []

    for k in k_range:
        if methode == 'kmeans':
            modele = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = modele.fit_predict(donnees_clustering)
            scores_metrique.append(modele.inertia_)
            titre_metrique = 'WSS score (Inertie)'
        elif methode == 'gmm':
            modele = GaussianMixture(n_components=k, random_state=42, covariance_type='full')
            labels = modele.fit_predict(donnees_clustering)
            scores_metrique.append(modele.bic(donnees_clustering))
            titre_metrique = 'BIC score'
        else:
            modele = methode(n_clusters=k)
            labels = modele.fit_predict(donnees_clustering)
            scores_metrique.append(0) 
            titre_metrique = 'Métrique non définie'

        score_sil = silhouette_score(
            donnees_clustering, 
            labels, 
            sample_size=sample_size, 
            random_state=42
        )
        scores_silhouette.append(score_sil)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    ax1.plot(k_range, scores_metrique, marker='o', linestyle='-', color='#2b8cbe')
    ax1.set_title(titre_metrique, fontsize=14)
    ax1.set_xlabel('Nombre de clusters (k)')
    ax1.grid(True, linestyle='--', alpha=0.6)

    ax2.plot(k_range, scores_silhouette, marker='s', linestyle='-', color='#5fba7d')
    ax2.set_title('Silhouette score', fontsize=14)
    ax2.set_xlabel('Nombre de clusters (k)')
    ax2.grid(True, linestyle='--', alpha=0.6)

    plt.tight_layout()
    plt.show()
    


def tracer_grille_silhouettes(data, methode='kmeans', liste_k=[2, 3, 4, 5, 6, 7], sample_size=5000, random_state=42, colonnes_id=['codecommune', 'dep']):
    df_travail = data.copy()
    
    for col in colonnes_id:
        if col in df_travail.columns:
            df_travail = df_travail.set_index(col, append=True)

    df_numerique = (
        df_travail.select_dtypes(include=[np.number])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    scaler = StandardScaler()
    matrice_scaled = scaler.fit_transform(df_numerique)
    
    donnees = pd.DataFrame(
        matrice_scaled, 
        columns=df_numerique.columns, 
        index=df_numerique.index
    )

    if sample_size and sample_size < len(donnees):
        X = donnees.sample(n=sample_size, random_state=random_state)
    else:
        X = donnees

    n_colonnes = 2
    n_lignes = int(np.ceil(len(liste_k) / n_colonnes))
    fig, axes = plt.subplots(n_lignes, n_colonnes, figsize=(15, 6 * n_lignes))
    axes = axes.flatten()

    for i, k in enumerate(liste_k):
        ax = axes[i]
        
        if methode == 'kmeans':
            clusterer = KMeans(n_clusters=k, random_state=random_state, n_init=10)
            labels = clusterer.fit_predict(X)
        elif methode == 'gmm':
            clusterer = GaussianMixture(n_components=k, random_state=random_state, covariance_type='full')
            labels = clusterer.fit_predict(X)
        else:
            clusterer = methode(n_clusters=k)
            labels = clusterer.fit_predict(X)

        score_moyen = silhouette_score(X, labels)
        scores_individuels = silhouette_samples(X, labels)

        y_lower = 10
        for j in range(k):
            scores_cluster_j = scores_individuels[labels == j]
            scores_cluster_j.sort()
            
            taille_cluster_j = scores_cluster_j.shape[0]
            y_upper = y_lower + taille_cluster_j
            
            couleur = cm.nipy_spectral(float(j) / k)
            ax.fill_betweenx(np.arange(y_lower, y_upper), 0, scores_cluster_j, facecolor=couleur, edgecolor=couleur, alpha=0.7)
            ax.text(-0.05, y_lower + 0.5 * taille_cluster_j, str(j+1))
            
            y_lower = y_upper + 10

        ax.set_title(f"k = {k} (Score moyen = {score_moyen:.3f})", fontsize=14)
        ax.set_xlabel("Coefficient de silhouette")
        ax.set_ylabel("Clusters")
        ax.axvline(x=score_moyen, color="red", linestyle="--")
        ax.set_yticks([])
        ax.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout()
    plt.show()

def r2_clustering(X, labels):
    X_arr = np.array(X)
    overall_mean = X_arr.mean(axis=0)
    total_ss = ((X_arr - overall_mean) ** 2).sum()
    between_ss = 0.0
    
    for cluster_id in np.unique(labels):
        cluster = X_arr[labels == cluster_id]
        if cluster.shape[0] == 0:
            continue
        cluster_mean = cluster.mean(axis=0)
        between_ss += cluster.shape[0] * ((cluster_mean - overall_mean) ** 2).sum()
        
    return between_ss / total_ss if total_ss > 0 else 0.0

def evaluer_r2_ch(data, methode='kmeans', k_range=[2, 3, 4, 5, 6, 7], colonnes_id=['codecommune', 'dep']):
    df_travail = data.copy()
    
    for col in colonnes_id:
        if col in df_travail.columns:
            df_travail = df_travail.set_index(col, append=True)

    df_numerique = (
        df_travail.select_dtypes(include=[np.number])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    scaler = StandardScaler()
    matrice_scaled = scaler.fit_transform(df_numerique)
    
    X = pd.DataFrame(
        matrice_scaled, 
        columns=df_numerique.columns, 
        index=df_numerique.index
    )

    r2_scores = []
    ch_scores = []

    for k in k_range:
        if methode == 'kmeans':
            modele = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = modele.fit_predict(X)
        elif methode == 'gmm':
            modele = GaussianMixture(n_components=k, random_state=42, covariance_type='full')
            labels = modele.fit_predict(X)
        else:
            modele = methode(n_clusters=k)
            labels = modele.fit_predict(X)

        r2_scores.append(r2_clustering(X, labels))
        ch_scores.append(calinski_harabasz_score(X, labels))

    fig, ax = plt.subplots(1, 2, figsize=(14, 5), dpi=120)

    ax[0].plot(k_range, r2_scores, marker="o", linestyle="-", color="#1f77b4")
    ax[0].set_title(f"R² du clustering ({methode.upper()})")
    ax[0].set_xlabel("Nombre de clusters")
    ax[0].set_ylabel("R²")
    ax[0].set_xticks(k_range)
    ax[0].grid(alpha=0.3)

    ax[1].plot(k_range, ch_scores, marker="o", linestyle="-", color="#ff7f0e")
    ax[1].set_title(f"Calinski-Harabasz ({methode.upper()})")
    ax[1].set_xlabel("Nombre de clusters")
    ax[1].set_ylabel("Score Calinski-Harabasz")
    ax[1].set_xticks(k_range)
    ax[1].grid(alpha=0.3)

    plt.tight_layout()
    plt.show()
    
    
def appliquer_clustering(data, methode='kmeans', n_clusters=4, random_state=42, colonnes_id=['codecommune', 'dep']):
    df_travail = data.copy()
    
    for col in colonnes_id:
        if col in df_travail.columns:
            df_travail = df_travail.set_index(col, append=True)

    df_numerique = (
        df_travail.select_dtypes(include=[np.number])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    scaler = StandardScaler()
    matrice_scaled = scaler.fit_transform(df_numerique)
    
    donnees_clustering = pd.DataFrame(
        matrice_scaled,
        columns=df_numerique.columns,
        index=df_numerique.index
    )

    if methode == 'kmeans':
        modele = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
        labels = modele.fit_predict(donnees_clustering)
    elif methode == 'gmm':
        modele = GaussianMixture(n_components=n_clusters, random_state=random_state, covariance_type='full')
        labels = modele.fit_predict(donnees_clustering)
    else:
        modele = methode(n_clusters=n_clusters)
        labels = modele.fit_predict(donnees_clustering)

    df_travail['Cluster'] = labels + 1
    
    repartition = df_travail['Cluster'].value_counts().sort_index()
    print(f"Répartition des communes dans les {n_clusters} clusters ({methode.upper()}) :")
    print("-" * 45)
    print(repartition)
    
    return df_travail