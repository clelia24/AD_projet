import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from sklearn.preprocessing import StandardScaler



def _palette(n):
    """Retourne n couleurs distinctes."""
    cmap = plt.cm.get_cmap("tab10" if n <= 10 else "tab20", n)
    return [cmap(i) for i in range(n)]


def _compute_barycentres(X_original, labels):
    """
    Calcule le barycentre (moyenne) de chaque cluster dans l'espace original.

    Paramètres
    ----------
    X_original : pd.DataFrame ou np.ndarray
        Données dans l'espace ORIGINAL (avant réduction), non standardisées.
    labels : array-like
        Labels de cluster pour chaque observation.

    Retourne
    -------
    barycentres : pd.DataFrame
        Shape (n_clusters, n_features) — un barycentre par cluster.
    cluster_ids : list
        Identifiants uniques des clusters (triés).
    """
    X = pd.DataFrame(X_original)
    labels = np.array(labels)
    cluster_ids = sorted(np.unique(labels))
    barycentres = pd.DataFrame(
        [X[labels == k].mean().values for k in cluster_ids],
        columns=X.columns if hasattr(X_original, "columns") else range(X.shape[1]),
    )
    return barycentres, cluster_ids



# PROJECTION SUR PCA


def projeter_clusters_pca(
    pca,
    scaler,
    df_original,
    labels_gmm,
    col_variables=None,
    composantes=(0, 1),
    n_variables_top=15,
    figsize=(14, 10),
    titre="Projection GMM sur PCA — barycentres et cercle des corrélations",
    alpha_individus=0.08,
    afficher_individus=True,
    data_pca_coords=None,
):
    """
    Superpose les barycentres GMM au cercle des corrélations PCA.

    Paramètres
    ----------
    pca : sklearn.decomposition.PCA
        Modèle PCA déjà fité.
    scaler : sklearn.preprocessing.StandardScaler
        Scaler utilisé avant la PCA (pour retransformer les barycentres).
    df_original : pd.DataFrame
        Données AVANT standardisation (colonnes = variables utilisées dans PCA).
    labels_gmm : array-like
        Labels GMM pour chaque ligne de df_original.
    col_variables : list, optional
        Noms des colonnes à afficher dans le cercle. Si None, toutes.
    composantes : tuple
        Indices des deux composantes à tracer (défaut: PC1 et PC2).
    n_variables_top : int
        Nombre de variables les mieux représentées à annoter.
    figsize : tuple
    titre : str
    alpha_individus : float
        Transparence des points individuels.
    afficher_individus : bool
        Si True, affiche les individus colorés par cluster en fond.
    data_pca_coords : np.ndarray, optional
        Coordonnées PCA des individus (shape n x n_components).
        Si None, elles sont recalculées depuis df_original.

    Retourne
    -------
    fig, ax : figure matplotlib
    bary_pca : pd.DataFrame
        Coordonnées des barycentres dans l'espace PCA.
    """
    ax1, ax2 = composantes
    col_variables = col_variables or list(df_original.columns)
    df_num = df_original[col_variables].replace([np.inf, -np.inf], np.nan).fillna(0)

    # — Coordonnées PCA des individus
    if data_pca_coords is None:
        X_scaled = scaler.transform(df_num)
        data_pca_coords = pca.transform(X_scaled)

    # — Barycentres → espace original → standardisation → projection PCA
    bary_raw, cluster_ids = _compute_barycentres(df_num, labels_gmm)
    bary_scaled = scaler.transform(bary_raw)
    bary_pca = pd.DataFrame(
        pca.transform(bary_scaled),
        columns=[f"PC{i+1}" for i in range(pca.n_components_)],
    )
    bary_pca.index = [f"Cluster {k}" for k in cluster_ids]

    # — Corrélations variables × composantes (cercle des corrélations)
    loadings = pca.components_.T  # shape (n_features, n_components)
    std_comps = np.sqrt(pca.explained_variance_)
    corr = loadings * std_comps  # corrélation variable–composante

    # Sélection des n_variables_top les mieux représentées sur le plan (ax1, ax2)
    cos2 = corr[:, ax1] ** 2 + corr[:, ax2] ** 2
    top_idx = np.argsort(cos2)[::-1][:n_variables_top]

    # — Figure
    colors = _palette(len(cluster_ids))
    fig, ax = plt.subplots(figsize=figsize)

    # Cercle unité
    theta = np.linspace(0, 2 * np.pi, 300)
    ax.plot(np.cos(theta), np.sin(theta), "grey", lw=0.8, ls="--", alpha=0.5)
    ax.axhline(0, color="grey", lw=0.5, alpha=0.5)
    ax.axvline(0, color="grey", lw=0.5, alpha=0.5)

    # Individus en fond
    if afficher_individus:
        for i, k in enumerate(cluster_ids):
            mask = np.array(labels_gmm) == k
            ax.scatter(
                data_pca_coords[mask, ax1] / (std_comps[ax1] * 3),  # normalisation visuelle
                data_pca_coords[mask, ax2] / (std_comps[ax2] * 3),
                c=[colors[i]],
                alpha=alpha_individus,
                s=8,
                zorder=1,
            )

    # Flèches des variables
    for j in top_idx:
        x, y = corr[j, ax1], corr[j, ax2]
        ax.annotate(
            "",
            xy=(x, y),
            xytext=(0, 0),
            arrowprops=dict(arrowstyle="->", color="steelblue", lw=1.2),
        )
        ax.text(
            x * 1.08,
            y * 1.08,
            col_variables[j],
            fontsize=7,
            ha="center",
            va="center",
            color="steelblue",
        )

    # Barycentres des clusters
    for i, k in enumerate(cluster_ids):
        label = f"Cluster {k}"
        # On normalise les barycentres de la même façon que les individus
        bx = bary_pca.loc[label, f"PC{ax1+1}"] / (std_comps[ax1] * 3)
        by = bary_pca.loc[label, f"PC{ax2+1}"] / (std_comps[ax2] * 3)
        ax.scatter(bx, by, c=[colors[i]], s=200, marker="*", edgecolors="black",
                   linewidths=0.8, zorder=5)
        ax.annotate(
            label,
            (bx, by),
            textcoords="offset points",
            xytext=(6, 6),
            fontsize=9,
            fontweight="bold",
            color=colors[i],
        )

    var1 = pca.explained_variance_ratio_[ax1] * 100
    var2 = pca.explained_variance_ratio_[ax2] * 100
    ax.set_xlabel(f"PC{ax1+1} ({var1:.1f}% variance)", fontsize=11)
    ax.set_ylabel(f"PC{ax2+1} ({var2:.1f}% variance)", fontsize=11)
    ax.set_title(titre, fontsize=13, fontweight="bold")
    ax.set_xlim(-1.2, 1.2)
    ax.set_ylim(-1.2, 1.2)
    ax.set_aspect("equal")

    # Légende
    legend_elements = [
        Line2D([0], [0], marker="*", color="w", markerfacecolor=colors[i],
               markeredgecolor="black", markersize=12, label=f"Cluster {k}")
        for i, k in enumerate(cluster_ids)
    ]
    ax.legend(handles=legend_elements, title="Clusters GMM", loc="lower right", fontsize=8)
    plt.tight_layout()
    return fig, ax, bary_pca



# PROJECTION SUR MCA 


def projeter_clusters_mca(
    mca,
    df_quali,
    labels_gmm,
    composantes=(0, 1),
    figsize=(14, 10),
    titre="Projection GMM sur MCA — barycentres et modalités",
    alpha_individus=0.08,
    afficher_individus=True,
    n_modalites_top=None,
):
    """
    Superpose les barycentres GMM au graphe des modalités MCA (package prince).

    Les barycentres sont calculés dans l'espace des coordonnées MCA individus
    (moyenne des coordonnées des individus de chaque cluster).

    Paramètres
    ----------
    mca : prince.MCA
        Modèle MCA déjà fité sur df_quali.
    df_quali : pd.DataFrame
        Données qualitatives utilisées pour fitter la MCA.
    labels_gmm : array-like
        Labels GMM pour chaque ligne de df_quali.
    composantes : tuple
        Indices des deux axes à tracer.
    figsize : tuple
    titre : str
    alpha_individus : float
    afficher_individus : bool
    n_modalites_top : int ou None
        Nombre de modalités à afficher (None = toutes).

    Retourne
    -------
    fig, ax
    bary_mca : pd.DataFrame
        Coordonnées des barycentres dans l'espace MCA.
    """
    ax1, ax2 = composantes

    # — Coordonnées MCA des individus et des modalités
    coords_ind = mca.row_coordinates(df_quali)    # shape (n, n_components)
    coords_col = mca.column_coordinates(df_quali) # shape (n_modalites, n_components)

    # — Barycentres = moyenne des coordonnées MCA par cluster
    labels_arr = np.array(labels_gmm)
    cluster_ids = sorted(np.unique(labels_arr))
    bary_mca = pd.DataFrame(
        [coords_ind[labels_arr == k].mean().values for k in cluster_ids],
        columns=coords_ind.columns,
    )
    bary_mca.index = [f"Cluster {k}" for k in cluster_ids]

    # — Sélection des modalités à afficher
    if n_modalites_top is not None:
        # On garde les modalités les plus éloignées de l'origine
        dist = coords_col.iloc[:, ax1] ** 2 + coords_col.iloc[:, ax2] ** 2
        coords_col = coords_col.loc[dist.nlargest(n_modalites_top).index]

    colors = _palette(len(cluster_ids))

    fig, ax = plt.subplots(figsize=figsize)
    ax.axhline(0, color="grey", lw=0.5, alpha=0.5)
    ax.axvline(0, color="grey", lw=0.5, alpha=0.5)

    # Individus en fond
    if afficher_individus:
        for i, k in enumerate(cluster_ids):
            mask = labels_arr == k
            ax.scatter(
                coords_ind.iloc[mask, ax1],
                coords_ind.iloc[mask, ax2],
                c=[colors[i]],
                alpha=alpha_individus,
                s=8,
                zorder=1,
            )

    # Modalités (variables qualitatives)
    ax.scatter(
        coords_col.iloc[:, ax1],
        coords_col.iloc[:, ax2],
        c="steelblue",
        marker="^",
        s=60,
        zorder=3,
        label="Modalités",
    )
    for idx, row in coords_col.iterrows():
        ax.annotate(
            str(idx),
            (row.iloc[ax1], row.iloc[ax2]),
            textcoords="offset points",
            xytext=(4, 4),
            fontsize=7,
            color="steelblue",
        )

    # Barycentres des clusters
    for i, k in enumerate(cluster_ids):
        label = f"Cluster {k}"
        bx = bary_mca.loc[label].iloc[ax1]
        by = bary_mca.loc[label].iloc[ax2]
        ax.scatter(bx, by, c=[colors[i]], s=220, marker="*",
                   edgecolors="black", linewidths=0.8, zorder=5)
        ax.annotate(
            label,
            (bx, by),
            textcoords="offset points",
            xytext=(6, 6),
            fontsize=9,
            fontweight="bold",
            color=colors[i],
        )

    # Variance expliquée
    eig = mca.eigenvalues_summary
    try:
        v1 = float(eig["% of variance"].iloc[ax1])
        v2 = float(eig["% of variance"].iloc[ax2])
    except Exception:
        v1 = v2 = float("nan")

    ax.set_xlabel(f"Dim {ax1+1} ({v1:.1f}% variance)", fontsize=11)
    ax.set_ylabel(f"Dim {ax2+1} ({v2:.1f}% variance)", fontsize=11)
    ax.set_title(titre, fontsize=13, fontweight="bold")

    legend_elements = [
        Line2D([0], [0], marker="*", color="w", markerfacecolor=colors[i],
               markeredgecolor="black", markersize=12, label=f"Cluster {k}")
        for i, k in enumerate(cluster_ids)
    ] + [
        Line2D([0], [0], marker="^", color="w", markerfacecolor="steelblue",
               markersize=9, label="Modalités")
    ]
    ax.legend(handles=legend_elements, title="Légende", loc="best", fontsize=8)
    plt.tight_layout()
    return fig, ax, bary_mca



# HEATMAP DES BARYCENTRES


def heatmap_barycentres(
    df_original,
    labels_gmm,
    col_variables=None,
    n_top=20,
    figsize=(14, 7),
    titre="Profil moyen des clusters GMM (variables standardisées)",
):
    """
    Heatmap des barycentres des clusters sur les variables les plus discriminantes.

    Utile pour interpréter les clusters en complément de la projection PCA/MCA.

    Paramètres
    ----------
    df_original : pd.DataFrame
        Données quantitatives originales (avant standardisation).
    labels_gmm : array-like
        Labels de cluster.
    col_variables : list, optional
        Sous-ensemble de colonnes à afficher. Si None, toutes les numériques.
    n_top : int
        Nombre de variables les plus discriminantes à afficher.
    figsize : tuple
    titre : str
    """
    import seaborn as sns

    col_variables = col_variables or list(df_original.select_dtypes(include=np.number).columns)
    df_num = df_original[col_variables].replace([np.inf, -np.inf], np.nan).fillna(0)

    # Standardisation pour comparaison
    scaler = StandardScaler()
    df_scaled = pd.DataFrame(scaler.fit_transform(df_num), columns=col_variables)
    df_scaled["Cluster"] = np.array(labels_gmm)

    bary = df_scaled.groupby("Cluster")[col_variables].mean()

    # Sélection des variables les plus discriminantes (variance inter-clusters)
    var_inter = bary.var(axis=0).nlargest(n_top).index
    bary_top = bary[var_inter]

    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(
        bary_top,
        cmap="RdBu_r",
        center=0,
        annot=True,
        fmt=".2f",
        linewidths=0.5,
        ax=ax,
        cbar_kws={"label": "Score standardisé (z-score)"},
    )
    ax.set_title(titre, fontsize=13, fontweight="bold")
    ax.set_xlabel("Variables", fontsize=10)
    ax.set_ylabel("Cluster GMM", fontsize=10)
    plt.tight_layout()
    return fig, ax

