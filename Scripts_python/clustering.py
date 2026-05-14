import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import seaborn as sns
import prince 
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe

 
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, silhouette_samples, calinski_harabasz_score
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.neighbors import kneighbors_graph
from joblib import Parallel, delayed
from scipy import sparse



""" 
Ce fichier comprend les fonctions utilisées dans la partie clustering 
"""

VOTE_KEYWORDS = ["vote", "pvoix", "pervote"]
ID_COLS_DEFAULT = ["codecommune", "dep"]

"""
Transformation des données 
"""
def _exclure_colonnes_vote(df_num):
    """Retourne la liste des colonnes de vote présentes dans df_num."""
    return [c for c in df_num.columns if any(k in c.lower() for k in VOTE_KEYWORDS)]

def preparer_donnees_clustering(data, colonnes_id=None, exclure_vote=False, fillna_method="zero"):
    # retourne le df normalisé, le df non normalisé avec les colonnes id en index et les colonnes de votes retirées si exclure_vote=True
    #data -> dataframe
    #colonnes_id -> Colonnes identifiantes à mettre en index (défaut : ['codecommune', 'dep']).
    #exclure_vote : bool
    if colonnes_id is None:
        colonnes_id = ID_COLS_DEFAULT
 
    df_travail = data.copy()
    for col in colonnes_id:
        if col in df_travail.columns:
            df_travail = df_travail.set_index(col, append=True)
 
    df_num = df_travail.select_dtypes(include=[np.number]).replace([np.inf, -np.inf], np.nan)
 
    vote_cols = []
    if exclure_vote:
        vote_cols = _exclure_colonnes_vote(df_num)
        df_num = df_num.drop(columns=vote_cols, errors="ignore")
        if vote_cols:
            print(f"  [clustering] Colonnes de vote exclues ({len(vote_cols)}) : {vote_cols}")
 
    if fillna_method == "median":
        df_num = df_num.fillna(df_num.median())
    else:
        df_num = df_num.fillna(0)
 
    scaler = StandardScaler()
    donnees_scaled = pd.DataFrame(
        scaler.fit_transform(df_num),
        columns=df_num.columns,
        index=df_num.index,
    )
    return donnees_scaled, df_travail, vote_cols

"""
Métriques
"""

def r2_clustering(data, labels):
    #calcule le R2 du clustering
    data = np.array(data)
    labels = np.array(labels)
    overall_mean = data.mean(axis=0)
    total_ss = ((data - overall_mean) ** 2).sum()
    if total_ss == 0:
        return 0.0
    between_ss = sum(
        data[labels == cid].shape[0] * ((data[labels == cid].mean(axis=0) - overall_mean) ** 2).sum()
        for cid in np.unique(labels)
        if data[labels == cid].shape[0] > 0
    )
    return between_ss / total_ss

"""
visualisation des scores
"""

#on pré-définit les styles
_SCORE_STYLES = {
    "WSS":        {"marker": "o", "color": "#2b8cbe", "ylabel": "WSS"},
    "Silhouette": {"marker": "s", "color": "#5fba7d", "ylabel": "Score silhouette"},
    "R2":         {"marker": "o", "color": "#1f77b4", "ylabel": "R²"},
    "CH":         {"marker": "^", "color": "#d7191c", "ylabel": "Score Calinski-Harabasz"},
    "BIC":        {"marker": "d", "color": "#fdae61", "ylabel": "BIC"},
}

def plot_scores_evaluation(k_range, scores_dict, methode=""):
    #affiche les courbes d'évaluation du clustering pour n'importe quelle méthode
    #k_range=valeurs à tester, score_dict= dictionnaire des scores, méthode= string pour ajouter dans le titre
    noms = list(scores_dict.keys())
    n = len(noms)
    ncols = min(n, 2)
    nrows = int(np.ceil(n / ncols))
 
    fig, axes = plt.subplots(nrows, ncols, figsize=(7 * ncols, 5 * nrows), dpi=110)
    axes = np.array(axes).flatten()
 
    for i, nom in enumerate(noms):
        style = _SCORE_STYLES.get(nom, {"marker": "o", "color": "#555555", "ylabel": nom})
        axes[i].plot(list(k_range), scores_dict[nom], marker=style["marker"],
                     linestyle="-", color=style["color"])
        titre = f"{nom}" + (f" ({methode})" if methode else "")
        axes[i].set_title(titre, fontsize=13)
        axes[i].set_xlabel("Nombre de clusters (k)")
        axes[i].set_ylabel(style["ylabel"])
        axes[i].set_xticks(list(k_range))
        axes[i].grid(alpha=0.35)
 
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])
 
    plt.tight_layout()
    plt.show()

def plot_silhouette_detail(data, labels_par_k, methode=""):
    #affiche les diagrammes de silhouette détaillés (un graphe par valeur de k)
    data = np.array(data)
    liste_k = sorted(labels_par_k.keys())
    ncols = 2
    nrows = int(np.ceil(len(liste_k) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(15, 6 * nrows))
    axes = np.array(axes).flatten()
 
    for i, k in enumerate(liste_k):
        ax = axes[i]
        labels = np.array(labels_par_k[k])
        score_moyen = silhouette_score(data, labels)
        scores_indiv = silhouette_samples(data, labels)
 
        y_lower = 10
        for cluster_id in np.unique(labels):
            scores_c = np.sort(scores_indiv[labels == cluster_id])
            if scores_c.size == 0:
                continue
            y_upper = y_lower + scores_c.size
            couleur = cm.nipy_spectral(float(cluster_id) / max(k, 1))
            ax.fill_betweenx(np.arange(y_lower, y_upper), 0, scores_c,
                             facecolor=couleur, edgecolor=couleur, alpha=0.7)
            ax.text(-0.05, y_lower + 0.5 * scores_c.size, str(int(cluster_id)),
                    fontsize=9, va="center")
            y_lower = y_upper + 10
 
        label_methode = f" {methode}" if methode else ""
        ax.set_title(f"k = {k}{label_methode} (Score moyen = {score_moyen:.3f})", fontsize=13)
        ax.set_xlabel("Coefficient de silhouette")
        ax.set_ylabel("Clusters")
        ax.axvline(x=score_moyen, color="red", linestyle="--")
        ax.set_yticks([])
        ax.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])
        ax.set_xlim([-0.15, 1])
 
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])
 
    plt.tight_layout()
    plt.show()

_GEOJSON_URL = (
    "https://raw.githubusercontent.com/gregoiredavid/france-geojson"
    "/master/communes.geojson"
)

def tracer_carte_clusters(df_data, colonne_cluster, titre, url_geojson=None, cmap_name="tab20",figsize=(15, 15), dpi=150):
    # trace la carte de france colorée par cluster 
    # colonne_cluster : Nom de la colonne contenant les labels de cluster 
    try:
        import geopandas as gpd
    except ImportError:
        raise ImportError("geopandas est requis pour tracer les cartes.")
 
    if url_geojson is None:
        url_geojson = _GEOJSON_URL
 
    df = df_data.copy()
    df["codecommune"] = df["codecommune"].astype(str).str.zfill(5)
 
    france = gpd.read_file(url_geojson)
    carte = france.merge(df, left_on="code", right_on="codecommune", how="inner")
 
    categories = sorted(carte[colonne_cluster].unique())
    n_cat = len(categories)
    cmap = cm.get_cmap(cmap_name, n_cat)
 
    fig, ax = plt.subplots(1, 1, figsize=figsize, dpi=dpi)
    carte.plot(
        column=colonne_cluster,
        ax=ax,
        categorical=True,
        categories=categories,
        cmap=cmap,
        legend=True,
        linewidth=0,
        edgecolor="none",
        legend_kwds={
            "title": colonne_cluster,
            "loc": "upper left",
            "bbox_to_anchor": (1, 1),
            "frameon": False,
        },
    )
    ax.set_axis_off()
    plt.title(titre, fontsize=18, fontweight="bold", pad=20)
    plt.tight_layout()
    plt.show()

def tracer_cartes_avec_sans_vote(df_avec, df_sans, colonne_cluster, methode="",url_geojson=None, cmap_name="tab20",figsize=(24, 14), dpi=150):
    #Affiche côte à côte la carte clustering avec variables de vote et sans variables de vote.
    try:
        import geopandas as gpd
    except ImportError:
        raise ImportError("geopandas est requis pour tracer les cartes.")
 
    if url_geojson is None:
        url_geojson = _GEOJSON_URL
 
    france = gpd.read_file(url_geojson)
 
    def _preparer(df):
        d = df.copy()
        d["codecommune"] = d["codecommune"].astype(str).str.zfill(5)
        return france.merge(d, left_on="code", right_on="codecommune", how="inner")
 
    carte_avec = _preparer(df_avec)
    carte_sans = _preparer(df_sans)
 
    categories = sorted(set(carte_avec[colonne_cluster].unique()) |
                        set(carte_sans[colonne_cluster].unique()))
    n_cat = len(categories)
    cmap = cm.get_cmap(cmap_name, n_cat)
 
    fig, axes = plt.subplots(1, 2, figsize=figsize, dpi=dpi)
 
    for ax, carte, sous_titre in zip(
        axes,
        [carte_avec, carte_sans],
        [f"{methode} — avec variables de vote", f"{methode} — sans variables de vote"],
    ):
        carte.plot(
            column=colonne_cluster,
            ax=ax,
            categorical=True,
            categories=categories,
            cmap=cmap,
            legend=True,
            linewidth=0,
            edgecolor="none",
            legend_kwds={
                "title": colonne_cluster,
                "loc": "upper left",
                "bbox_to_anchor": (1, 1),
                "frameon": False,
            },
        )
        ax.set_axis_off()
        ax.set_title(sous_titre, fontsize=15, fontweight="bold")
 
    plt.tight_layout()
    plt.show()

def tracer_cartes_multiples(cartes_config, url_geojson=None, cmap_name="tab10",figsize=None, dpi=150):

    # Affiche plusieurs cartes côte à côte.
    #cartes_config prends une liste de tuple (df_data, colonne_cluster,titre)
    try:
        import geopandas as gpd
    except ImportError:
        raise ImportError("geopandas est requis pour tracer les cartes.")
 
    if url_geojson is None:
        url_geojson = _GEOJSON_URL
 
    france = gpd.read_file(url_geojson)
    n = len(cartes_config)
    if figsize is None:
        figsize = (14 * n, 14)
 
    # On détermine toutes les catégories pour une colormap cohérente entre cartes
    all_categories = set()
    merged_list = []
    for df_data, colonne_cluster, _ in cartes_config:
        df = df_data.copy()
        df["codecommune"] = df["codecommune"].astype(str).str.zfill(5)
        merged = france.merge(df, left_on="code", right_on="codecommune", how="inner")
        merged_list.append((merged, colonne_cluster))
        all_categories |= set(merged[colonne_cluster].unique())
 
    categories = sorted(all_categories)
    cmap = cm.get_cmap(cmap_name, len(categories))
 
    fig, axes = plt.subplots(1, n, figsize=figsize, dpi=dpi)
    if n == 1:
        axes = [axes]
 
    for ax, (merged, colonne_cluster), (_, _, titre) in zip(axes, merged_list, cartes_config):
        merged.plot(
            column=colonne_cluster,
            ax=ax,
            categorical=True,
            categories=categories,
            cmap=cmap,
            legend=True,
            linewidth=0,
            edgecolor="none",
            legend_kwds={
                "title": colonne_cluster,
                "loc": "upper left",
                "bbox_to_anchor": (1, 1),
                "frameon": False,
            },
        )
        ax.set_axis_off()
        ax.set_title(titre, fontsize=18, fontweight="bold")
 
    plt.tight_layout()
    plt.show()


def comparer_clusters_variables(df_travail, colonne_cluster, n_top=15, colonnes_a_exclure=None):
    #affiche les variables les plus discriminantes 
    #df_travail doit contenir colonne_cluster 
    if colonnes_a_exclure is None:
        colonnes_a_exclure = []
 
    cols_cluster_internes = [c for c in df_travail.columns
                             if "cluster" in c.lower() or "certitude" in c.lower()]
    a_exclure = list(set(colonnes_a_exclure + cols_cluster_internes + [colonne_cluster]))
 
    num_cols = df_travail.select_dtypes(include=[np.number]).columns.difference(a_exclure)
    df_clean = df_travail[num_cols].replace([np.inf, -np.inf], np.nan).fillna(
        df_travail[num_cols].median()
    )
 
    scaler = StandardScaler()
    X_std = pd.DataFrame(scaler.fit_transform(df_clean), columns=num_cols, index=df_clean.index)
    X_std[colonne_cluster] = df_travail[colonne_cluster].values
 
    cluster_means = X_std.groupby(colonne_cluster).mean()
    unique_clusters = sorted(X_std[colonne_cluster].unique())
 
    if len(unique_clusters) == 2:
        c1, c2 = unique_clusters
        diff = cluster_means.loc[c2] - cluster_means.loc[c1]
        result = pd.DataFrame({
            f"mean_cluster_{c1}": cluster_means.loc[c1],
            f"mean_cluster_{c2}": cluster_means.loc[c2],
            f"diff_{c2}_minus_{c1}": diff,
            "abs_diff": diff.abs(),
        }).sort_values("abs_diff", ascending=False)
 
        print(f"Top {n_top} variables discriminantes entre cluster {c1} et {c2} :")
        display(result.head(n_top))
 
        top_vars = result.index[:n_top].tolist()
        plt.figure(figsize=(10, 6))
        sns.barplot(
            x=result.loc[top_vars, f"diff_{c2}_minus_{c1}"].values,
            y=top_vars,
            palette="coolwarm",
        )
        plt.axvline(0, color="k", linestyle="--", linewidth=0.8)
        plt.title(f"Top {n_top} variables (différence standardisée : cluster {c2} vs {c1})")
        plt.xlabel("Différence de moyenne standardisée")
        plt.tight_layout()
        plt.show()
 
    else:
        print(f"Moyennes standardisées par cluster (top {n_top} variables, variance inter-cluster) :")
        var_inter = cluster_means.var(axis=0).sort_values(ascending=False)
        top_vars = var_inter.index[:n_top].tolist()
        display(cluster_means[top_vars].T.style.background_gradient(cmap="coolwarm", axis=1))


def table_contingence(labels1, labels2, nom1="Méthode 1", nom2="Méthode 2"):
    # affiche la table de contingence  entre deux vecteurs de labels.
    table = pd.crosstab(
        pd.Series(labels1, name=nom1),
        pd.Series(labels2, name=nom2),
        margins=False,
    )
    print(f"Table de contingence : {nom1} × {nom2}")
    display(table)
    return table

"""
K-means
"""

def fit_kmeans(donnees_scaled, n_clusters, random_state=42, n_init=10):
    #ajuste K-means
    X = np.array(donnees_scaled)
    km = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=n_init)
    labels = km.fit_predict(X) 
    return labels, km
 
def evaluer_kmeans(donnees_scaled, k_range, random_state=42, n_init=10, sample_size=None, random_state_sample=42):
    #Calcule WSS, Silhouette, R² et Calinski-Harabasz pour chaque k.
    #sample_size = Si fourni, calcule le silhouette score sur un sous-échantillon
    k_range = list(k_range)
    X = np.array(donnees_scaled)
    wss, sil, r2_list, ch = [], [], [], []
    labels_par_k = {}
 
    for k in k_range:
        km = KMeans(n_clusters=k, random_state=random_state, n_init=n_init)
        labels = km.fit_predict(X)
        labels_par_k[k] = labels
 
        wss.append(km.inertia_)
        r2_list.append(r2_clustering(X, labels))
        ch.append(calinski_harabasz_score(X, labels))
 
        if sample_size and sample_size < len(labels):
            rng = np.random.RandomState(random_state_sample)
            idx = rng.choice(len(labels), size=sample_size, replace=False)
            sil.append(silhouette_score(X[idx], labels[idx]))
        else:
            sil.append(silhouette_score(X, labels))
 
    return {"WSS": wss, "Silhouette": sil, "R2": r2_list, "CH": ch}, labels_par_k

"""
GMM
"""

def fit_gmm(donnees_scaled, n_components, covariance_type="full", random_state=42):
    #fit GMM
    X = np.array(donnees_scaled)
    gmm = GaussianMixture(n_components=n_components, covariance_type=covariance_type,
                          random_state=random_state)
    gmm.fit(X)
    labels = gmm.predict(X) 
    probas = gmm.predict_proba(X)
    return labels, probas, gmm

def evaluer_gmm(donnees_scaled, k_range, covariance_type="full", random_state=42,sample_size=None, random_state_sample=42):
    #Calcule WSS, Silhouette, R², Calinski-Harabasz et BIC pour chaque k.

    k_range = list(k_range)
    X = np.array(donnees_scaled)
    wss, sil, r2_list, ch, bic = [], [], [], [], []
    labels_par_k = {}
 
    for k in k_range:
        gmm = GaussianMixture(n_components=k, covariance_type=covariance_type,
                              random_state=random_state)
        labels = gmm.fit_predict(X)
        labels_par_k[k] = labels
 
        wss.append(-gmm.score(X) * len(X))
        r2_list.append(r2_clustering(X, labels))
        ch.append(calinski_harabasz_score(X, labels))
        bic.append(gmm.bic(X))
 
        if sample_size and sample_size < len(labels):
            rng = np.random.RandomState(random_state_sample)
            idx = rng.choice(len(labels), size=sample_size, replace=False)
            sil.append(silhouette_score(X[idx], labels[idx]))
        else:
            sil.append(silhouette_score(X, labels))
 
    return {"WSS": wss, "Silhouette": sil, "R2": r2_list, "CH": ch, "BIC": bic}, labels_par_k

"""
SBM
"""

def construire_graphe_similarite(X, n_neighbors=12):
    #Construit une matrice de similarité k-NN symétrique (format CSR).
    X = np.array(X)
    n_neighbors = min(n_neighbors, X.shape[0] - 1)
    A = kneighbors_graph(X, n_neighbors=n_neighbors, mode="connectivity",
                         include_self=False, n_jobs=-1)
    A = 0.5 * (A + A.T)
    A.data[:] = 1
    return A.tocsr()
 


def fit_sbm(A, n_blocks=5, max_iter=20, random_state=42):
    # fais un SBM
    n = A.shape[0]
    rng = np.random.RandomState(random_state)
    z = rng.randint(n_blocks, size=n)
 
    for _ in range(max_iter):
        block_sizes = np.bincount(z, minlength=n_blocks)
 
        m_rs = np.zeros((n_blocks, n_blocks), dtype=float)
        for r in range(n_blocks):
            rows = np.where(z == r)[0]
            if rows.size == 0:
                continue
            sub = A[rows]
            counts = np.bincount(z[sub.indices], minlength=n_blocks, weights=sub.data)
            m_rs[r, :] = counts
 
        p_rs = np.zeros_like(m_rs)
        for r in range(n_blocks):
            for s in range(n_blocks):
                denom = block_sizes[r] * block_sizes[s]
                if r == s:
                    denom = block_sizes[r] * max(block_sizes[r] - 1, 1)
                p_rs[r, s] = m_rs[r, s] / max(denom, 1)
        p_rs = np.clip(p_rs, 1e-6, 1 - 1e-6)
 
        new_z = np.empty_like(z)
        for i in range(n):
            row = A[i]
            neighbors = row.indices
            counts = np.bincount(z[neighbors], minlength=n_blocks)
 
            best_score, best_block = -np.inf, z[i]
            for r in range(n_blocks):
                bsc = block_sizes.copy()
                if z[i] != r:
                    bsc[z[i]] -= 1
                    bsc[r] += 1
                score = 0.0
                for s in range(n_blocks):
                    e_is = counts[s]
                    denom = max(bsc[s] - 1, 1) if s == r else max(bsc[s], 1)
                    score += e_is * np.log(p_rs[r, s]) + (denom - e_is) * np.log(1 - p_rs[r, s])
                if score > best_score:
                    best_score, best_block = score, r
            new_z[i] = best_block
 
        if np.array_equal(z, new_z):
            break
        z = new_z
 
    return z

def _evaluer_sbm_k(k, A, X, max_iter, random_state, sample_size, random_state_sample):
    #evalue SBM pour un k 
    # A -> matrice de similarité X: array normalisé 
    labels = fit_sbm(A, n_blocks=k, max_iter=max_iter, random_state=random_state) + 1
    r2 = r2_clustering(X, labels)
    ch = calinski_harabasz_score(X, labels)
 
    if sample_size and sample_size < len(labels):
        rng = np.random.RandomState(random_state_sample)
        idx = rng.choice(len(labels), size=sample_size, replace=False)
        sil = silhouette_score(X[idx], labels[idx], metric="euclidean")
    else:
        sil = silhouette_score(X, labels, metric="euclidean")
 
    return k, labels, sil, r2, ch

def evaluer_sbm(A, X, k_range, max_iter=20, random_state=42,sample_size=2000, random_state_sample=42, n_jobs=-1):
    #Évalue SBM sur une plage de k (calcul parallèle).
    k_range = list(k_range)
    X = np.array(X)
 
    results = Parallel(n_jobs=n_jobs, prefer="processes")(
        delayed(_evaluer_sbm_k)(k, A, X, max_iter, random_state, sample_size, random_state_sample)
        for k in k_range
    )
    results = sorted(results, key=lambda x: x[0])
 
    labels_par_k = {}
    scores = {"Silhouette": [], "R2": [], "CH": []}
    for k, labels, sil, r2, ch in results:
        labels_par_k[k] = labels
        scores["Silhouette"].append(sil)
        scores["R2"].append(r2)
        scores["CH"].append(ch)
 
    return scores, labels_par_k

"""
code pour la MCA sur le GMM
"""
def discretiser_means(means_df, bin_edges_dict, df_mca_columns):
    #discretisation
    result = pd.DataFrame(index=means_df.index)
    for col in means_df.columns:
        if col in bin_edges_dict:
            result[col] = pd.cut(
                means_df[col],
                bins=bin_edges_dict[col],
                labels=False,
                include_lowest=True,
            ).astype("Int64").astype(str).fillna("NA")
    # Alignement sur toutes les colonnes MCA
    for col in df_mca_columns:
        if col not in result.columns:
            result[col] = "NA"
    return result[df_mca_columns]

def MCA_gmm (df_travail):
    #fonction permettant de faire la MCA avec nos données de Cluster Gmm
    # Préparation
    donnees_scaled, _, _ = preparer_donnees_clustering(df_travail, colonnes_id=[], fillna_method="median")

    df_numerique = ( df_travail.select_dtypes(include=[np.number]).replace([np.inf, -np.inf], np.nan).fillna( df_travail.select_dtypes(include=[np.number]).median()))
    scaler = StandardScaler()
    scaler.fit(df_numerique)

    # Fit GMM
    labels_gmm, probas_gmm, gmm = fit_gmm(donnees_scaled, n_components=4)

    # Means dans l'espace original via inverse_transform
    means_gmm = pd.DataFrame(
        scaler.inverse_transform(gmm.means_),
        columns=df_numerique.columns,
        index=[f"GMM_Cluster_{i+1}" for i in range(gmm.n_components)],
    )

    # Discrétisation de df_travail (sauvegarde des bins pour projeter les means)
    n_bins = 4
    df_num = df_travail.select_dtypes(include=[np.number])
    df_cat = df_travail.select_dtypes(include=["object", "category"])

    df_num_disc = pd.DataFrame(index=df_travail.index)
    bin_edges = {}

    for col in df_num.columns:
        series = df_num[col].replace([np.inf, -np.inf], np.nan).dropna()
        try:
            _, edges = pd.qcut(series, q=n_bins, retbins=True, duplicates="drop")
            df_num_disc[col] = pd.cut(
                df_num[col], bins=edges, labels=False, include_lowest=True
            ).astype("Int64").astype(str)
            bin_edges[col] = edges
        except Exception:
            pass
    df_cat = df_travail.select_dtypes(include=["object", "category"])
    
    cat_utiles = [col for col in df_cat.columns if df_cat[col].nunique() < 50]

    cols_ignorees = set(df_cat.columns) - set(cat_utiles)
    if cols_ignorees:
        print(f"Colonnes exclues de l'ACM car trop précises : {cols_ignorees}")

    df_mca_input = df_num_disc.copy()
    if cat_utiles:
        df_mca_input = pd.concat([df_mca_input, df_cat[cat_utiles].astype(str)], axis=1)
    
    df_mca_input = df_mca_input.fillna("NA")

    #fit MCA 
    mca = prince.MCA(n_components=2, random_state=42)
    mca.fit(df_mca_input)

    # Coordonnées des individus et des modalités
    coords_individus  = mca.row_coordinates(df_mca_input)
    coords_modalites  = mca.column_coordinates(df_mca_input)
    means_gmm_disc = discretiser_means(means_gmm, bin_edges, df_mca_input.columns)

    # Projection comme individus supplémentaires (ON NE FIT PAS DESSUS)
    coords_gmm_proj = mca.row_coordinates(means_gmm_disc)

    print("=== Coordonnées MCA — Centres GMM ===")
    print(coords_gmm_proj.round(4))

    try:
        ev = mca.percentage_of_variance_
        xlabel = f"Dimension 1 ({ev[0]:.1f}% d'inertie)"
        ylabel = f"Dimension 2 ({ev[1]:.1f}% d'inertie)"
    except AttributeError:
        ev = mca.eigenvalues_summary["% of variance"]
        xlabel = f"Dimension 1 ({ev.iloc[0]})"
        ylabel = f"Dimension 2 ({ev.iloc[1]})"

    fig, ax = plt.subplots(figsize=(12, 8), dpi=150)

    ax.set_facecolor('#F8FAFC')
    fig.patch.set_facecolor('white')
    ax.grid(color='#E2E8F0', linestyle='--', linewidth=1, zorder=0)

    ax.axhline(0, color="#94A3B8", linewidth=1.5, linestyle="-", zorder=1)
    ax.axvline(0, color="#94A3B8", linewidth=1.5, linestyle="-", zorder=1)

    idx_sample = np.random.choice(len(coords_individus), size=min(3000, len(coords_individus)), replace=False)
    ax.scatter(
        coords_individus.iloc[idx_sample, 0],
        coords_individus.iloc[idx_sample, 1],
        alpha=0.3, s=20, color="#64748B", edgecolors="none", zorder=2, label="_nolegend_"
    )

    colors_gmm = plt.cm.Set1(np.linspace(0, 1, len(coords_gmm_proj)))

    for i, (idx, row) in enumerate(coords_gmm_proj.iterrows()):
        x, y = row.iloc[0], row.iloc[1]
        
        # Losanges GMM
        ax.scatter(
            x, y,
            s=400, color=colors_gmm[i], marker="D",
            zorder=5, edgecolors="white", linewidths=2.5
        )

        ax.annotate(
            idx, (x, y),
            fontsize=12, fontweight="bold", color="#1E293B",
            xytext=(14, 10), textcoords="offset points", zorder=6,
            path_effects=[pe.withStroke(linewidth=4, foreground="white")]
        )

    for spine in ['top', 'right']:
        ax.spines[spine].set_visible(False)
    for spine in ['left', 'bottom']:
        ax.spines[spine].set_color('#CBD5E1')
        ax.spines[spine].set_linewidth(1.5)


    ax.set_xlabel(xlabel, fontsize=13, fontweight='medium', color='#334155', labelpad=10)
    ax.set_ylabel(ylabel, fontsize=13, fontweight='medium', color='#334155', labelpad=10)
    ax.set_title("Projection des centres GMM dans l'espace MCA", 
                 fontsize=16, fontweight="bold", color="#0F172A", pad=20)

 
    legend_elements = [
        plt.Line2D([0], [0], marker="D", color="w", markerfacecolor="#EF4444",
                   markersize=13, markeredgecolor="white", markeredgewidth=2, label="Centres GMM")
    ]
    ax.legend(handles=legend_elements, fontsize=12, frameon=True, 
              facecolor='white', edgecolor='#E2E8F0', loc='best', borderpad=1)

    plt.tight_layout()
    plt.show()