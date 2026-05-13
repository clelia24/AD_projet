import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import prince as pr
from adjustText import adjust_text
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import random

"""
PCA - les fonctions suivantes sont utilisées pour la PCA
"""

def transfo_pca(data, n_components=5):
        # Sélection des colonnes numériques
    data_num = data.select_dtypes(include=['float64', 'int64'])

    # Remplacement des valeurs infinies par NaN
    data_num = data_num.replace([np.inf, -np.inf], np.nan)

    # Remplacement des valeurs manquantes par la moyenne
    data_num = data_num.fillna(data_num.mean())

    # Standardisation des données
    scaler = StandardScaler()

    X_scaled = scaler.fit_transform(data_num)

    return X_scaled


def run_pca(X_scaled, n_components=5):

    # PCA
    pca = PCA(n_components=n_components)

    X_pca = pca.fit_transform(X_scaled)

    # Affichage des dimensions
    print("-- PCA --")

    print(f"Dimension initiale : {X_scaled.shape}")

    print(f"Dimension après PCA : {X_pca.shape}\n")



    return X_pca, pca

def plot_explained_variance(pca):
        # Variance expliquée
    print("Explained variance :")

    explained_variance = pca.explained_variance_ratio_ 

    for i, var_ratio in enumerate(explained_variance, start=1):
        print(
            f"Composante {i} : "
            f"{var_ratio:.2f} "
            f"({var_ratio*100:.2f}% de la variance totale)"
        )

    print(
        "\nVariance expliquée par chaque composante :",
        explained_variance
    )

    # Graphique
    plt.figure(figsize=(7,5))

    plt.plot(
        range(1, len(explained_variance)+1),
        explained_variance*100,
        marker='o'
    )

    plt.title("Variance expliquée par composante")

    plt.xlabel("Composantes principales")

    plt.ylabel("Variance expliquée")

    plt.grid(True)

    plt.show()



def identifier_colonnes(df):
    """Sépare les variables socio-éco des variables de vote."""
    vote_cols = [c for c in df.select_dtypes(include=[np.number]).columns
                 if any(k in c.lower() for k in ['vote', 'pvoix', 'ratio', 'score', 'blancnul', 'par', 'insr'])]
    
    socio_cols = [c for c in df.select_dtypes(include=[np.number]).columns
                  if c not in vote_cols and c not in ['Bloc_Score', 'Parti_Score']]
    
    vote_supp_cols = [c for c in ['pvoteG', 'pvoteCG', 'pvoteC', 'pvoteCD', 'pvoteD'] if c in df.columns]
    
    return socio_cols, vote_supp_cols


def plot_cumul(pca):
    """Affiche le scree plot (éboulis des valeurs propres)."""
    explained= pca.explained_variance_ratio_ * 100
    cumulative = np.cumsum(explained)
    plt.plot(cumulative, marker='o', color='steelblue')
    plt.title('Cumulative explained variance according to the dimension of the PCA␣')
    plt.xlabel('Number of components in the PCA')
    plt.ylabel('Cumulative explained variance');
    plt.axhline(80, color='red', linestyle='--', linewidth=0.8, label='Seuil 80%')


def plot_cercle_correlations(pca, feature_names, top_n=15):
    """Affiche le cercle des corrélations pour les top variables."""
    loadings = pca.components_
    explained = pca.explained_variance_ratio_ * 100
    
    cos2 = loadings[0]**2 + loadings[1]**2
    top_idx = np.argsort(cos2)[-top_n:]
    
    fig, ax = plt.subplots(figsize=(8, 8))
    circle = plt.Circle((0, 0), 1, color='grey', fill=False, linestyle='--')
    ax.add_patch(circle)

    for i in top_idx:
        ax.annotate('', xy=(loadings[0, i], loadings[1, i]), xytext=(0, 0),
                    arrowprops=dict(arrowstyle='->', color='steelblue', lw=1.5))
        ax.text(loadings[0, i]*1.07, loadings[1, i]*1.07,
                feature_names[i], fontsize=8, ha='center', color='steelblue')

    ax.axhline(0, color='k', linewidth=0.5)
    ax.axvline(0, color='k', linewidth=0.5)
    ax.set_xlim(-1.2, 1.2)
    ax.set_ylim(-1.2, 1.2)
    ax.set_xlabel(f'PC1 ({explained[0]:.1f}%)')
    ax.set_ylabel(f'PC2 ({explained[1]:.1f}%)')
    ax.set_title(f'Cercle des corrélations (top {top_n} variables)')
    ax.set_aspect('equal')
    plt.tight_layout()
    return fig

def plot_biplot_complet(pca, coords, df_orig, socio_cols, vote_supp_cols, sample_size=3000):
    """Affiche le biplot avec individus colorés et variables de vote supplémentaires."""
    explained = pca.explained_variance_ratio_ * 100
    
    # Échantillonnage pour la lisibilité
    sample_idx = df_orig.sample(n=min(sample_size, len(df_orig)), random_state=42).index
    # On récupère les positions entières des index échantillonnés
    sample_pos = [df_orig.index.get_loc(i) for i in sample_idx]

    couleurs_blocs = {
        'pvoteG': '#d73027', 'pvoteCG': '#C46B7A', 'pvoteC': '#FFA500',
        'pvoteCD': '#91bfdb', 'pvoteD': '#4575b4',
    }
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Affichage des individus
    if 'Bloc_Dominant' in df_orig.columns:
        bloc_col = df_orig.loc[sample_idx, 'Bloc_Dominant']
        for bloc, couleur in couleurs_blocs.items():
            mask = (bloc_col == bloc).values
            if mask.any():
                # On filtre coords par les positions de l'échantillon puis par le masque
                sub_coords = coords[sample_pos][mask]
                ax.scatter(sub_coords[:, 0], sub_coords[:, 1], c=couleur, alpha=0.3, s=8, label=bloc)
        ax.legend(title='Bloc dominant', markerscale=2)
    else:
        ax.scatter(coords[sample_pos, 0], coords[sample_pos, 1], alpha=0.2, s=8)

    # Variables de vote en supplémentaires
    df_vote_supp = df_orig[vote_supp_cols].replace([np.inf, -np.inf], np.nan).fillna(0)
    X_supp_scaled = StandardScaler().fit_transform(df_vote_supp)
    vote_loadings = np.corrcoef(X_supp_scaled.T, coords[:, :2].T)[:len(vote_supp_cols), len(vote_supp_cols):]

    scale = np.abs(coords[:, :2]).max() * 0.6
    for j, vname in enumerate(vote_supp_cols):
        vx, vy = vote_loadings[j, 0] * scale, vote_loadings[j, 1] * scale
        ax.annotate('', xy=(vx, vy), xytext=(0, 0), arrowprops=dict(arrowstyle='->', color='black', lw=2))
        ax.text(vx*1.1, vy*1.1, vname, fontsize=9, fontweight='bold')

    ax.axhline(0, color='k', linewidth=0.5)
    ax.axvline(0, color='k', linewidth=0.5)
    ax.set_xlabel(f'PC1 ({explained[0]:.1f}%)')
    ax.set_ylabel(f'PC2 ({explained[1]:.1f}%)')
    ax.set_title('Biplot PCA — individus (bloc dominant) + variables de vote supplémentaires')
    plt.tight_layout()
    


"""
MCA- les fonctions suivantes sont utilisées pour la MCA 
"""

def enlever_dummies(data, debut, nom_col, nom_manquant):
    col_a_nett= [c for c in data.columns if c.startswith(debut)]
    data[nom_col]=data[col_a_nett].idxmax(axis=1)
    data.loc[data[col_a_nett].sum(axis=1) == 0, nom_col] = nom_manquant
    data=data.drop(col_a_nett, axis=1, errors='ignore')
    data[nom_col]=data[nom_col].str.replace(debut,'', regex=False)
    return data 


def MCA(data, nb_compo=2):
    mca = pr.MCA(n_components=nb_compo, random_state=42)
    mca = mca.fit(data)

    print("="*50)
    print("STATISTIQUES DE L'ACM")
    stats = pd.DataFrame({
        'Valeur Propre': mca.eigenvalues_,
        '% Variance': mca.percentage_of_variance_ ,
        '% Cumulé': mca.percentage_of_variance_.cumsum()
    })
    print(stats)
    print("="*50)

    col_coords = mca.column_coordinates(data)
    plt.figure(figsize=(10, 7))
    plt.scatter(col_coords[0], col_coords[1], c='red', s=50, alpha=0.7, edgecolors='white')

    texts = []
    binaires = ['0', '1', '0.0', '1.0', 'True', 'False', 'Oui', 'Non', 'O', 'N']

    for index in col_coords.index:
        label_original = str(index)
        clean_label = label_original
        
        for col in data.columns:
            if col in label_original:
                valeur = label_original.replace(col, "").strip('_')
                
                if valeur in binaires or valeur == "":
                    clean_label = f"{col}: {valeur}"
                else:
                    clean_label = valeur
                break 

        clean_label = clean_label.replace('_', ' ')
        
        x = col_coords.loc[index, 0]
        y = col_coords.loc[index, 1]
        texts.append(plt.text(x, y, clean_label, fontsize=10, fontweight='bold'))


    adjust_text(texts, arrowprops=dict(arrowstyle='->', color='gray', lw=0.5, alpha=0.5))

    plt.axhline(0, color='black', linestyle='--', linewidth=1, alpha=0.3)
    plt.axvline(0, color='black', linestyle='--', linewidth=1, alpha=0.3)
    
    v1 = mca.percentage_of_variance_[0] * 100
    v2 = mca.percentage_of_variance_[1] * 100
    plt.xlabel(f"Dimension 1 ({v1:.2f}%)")
    plt.ylabel(f"Dimension 2 ({v2:.2f}%)")
    plt.title("ACM : Carte des modalités ")
    plt.grid(True, linestyle=':', alpha=0.5)
    
    plt.show()
    return mca

def analyse_dimensionnelle_mca(data, seuils=[50, 80]): 
   
    n_modalites = sum(data.nunique())
    n_vars = len(data.columns)
    n_max = n_modalites - n_vars
    
    mca = pr.MCA(n_components=n_max, random_state=42)
    mca = mca.fit(data)
    variance_cumulee = mca.percentage_of_variance_.cumsum()
    if variance_cumulee.max() <= 1: 
        variance_cumulee = variance_cumulee * 100

    print("\n--- RÉSULTATS ---")
    for seuil in seuils:
        idx = np.where(variance_cumulee >= seuil)[0]
        if len(idx) > 0:
            nb_dims = idx[0] + 1
            print(f"Pour {seuil}% de variance : il faut {nb_dims} dimensions.")
        else:
            print(f"Seuil {seuil}% non atteint (Max: {variance_cumulee.max():.2f}%)")

    plt.figure(figsize=(10, 5))
    plt.plot(range(1, len(variance_cumulee) + 1), variance_cumulee, marker='o')
    plt.axhline(y=50, color='r', linestyle=':', label='Objectif 50%')
    plt.axhline(y=80, color='g', linestyle=':', label='Objectif 80%')
    plt.ylim(0, 105) 
    plt.title("Variance cumulée")
    plt.legend()
    plt.grid(True)
    plt.show()


def plot_mca_modalities_fortes(mca, data, use_contribution=True):
    coords = mca.column_coordinates(data)
    if use_contribution:
        stat = mca.column_contributions_.sum(axis=1) 
        title = "Contribution des Modalités (Poids sur les axes)"
        cmap = 'YlOrRd'
    else:
        cos2_df = mca.column_cosine_similarities(data)
        stat = cos2_df.sum(axis=1)
        title = "Qualité de représentation (cos²)"
        cmap = 'YlOrRd'

    plt.figure(figsize=(10, 7))
    sc = plt.scatter(coords[0], coords[1], c=stat, cmap=cmap, s=100, edgecolors='k', alpha=0.8)
    plt.colorbar(sc, label='Intensité')

    for i, txt in enumerate(coords.index):
        plt.annotate(txt.split('_')[-1], (coords.iloc[i, 0], coords.iloc[i, 1]), fontsize=9)

    plt.axhline(0, color='grey', ls='--'), plt.axvline(0, color='grey', ls='--')
    plt.title(title)
    plt.show()



def plot_mca_variable_importance(mca, data):
    contributions = mca.column_contributions_
    var_data = []
    
    for col in data.columns:
        mask = contributions.index.str.contains(col)
        importance_dim1 = contributions[mask][0].sum()
        importance_dim2 = contributions[mask][1].sum()
        var_data.append({'Variable': col, 'Dim1': importance_dim1, 'Dim2': importance_dim2})
    
    df_var = pd.DataFrame(var_data)
    fig, ax = plt.subplots(figsize=(10, 7))
    
    texts = []
    for i, row in df_var.iterrows():
        ax.arrow(0, 0, row['Dim1'], row['Dim2'], 
                 head_width=max(df_var['Dim1'])*0.02, 
                 color='teal', alpha=0.6, length_includes_head=True)
        
        texts.append(ax.text(row['Dim1'], row['Dim2'], row['Variable'], 
                             fontsize=12, fontweight='bold'))

    adjust_text(texts, 
                only_move={'points':'y', 'text':'xy'}, 
                arrowprops=dict(arrowstyle='->', color='red', lw=0.5))


    plt.xlim(-0.01, df_var['Dim1'].max() * 1.1)
    plt.ylim(-0.01, df_var['Dim2'].max() * 1.1)

    plt.axhline(0, color='black', ls='--', alpha=0.3)
    plt.axvline(0, color='black', ls='--', alpha=0.3)
    plt.xlabel("Contribution à l'Axe 1", fontsize=12)
    plt.ylabel("Contribution à l'Axe 2", fontsize=12)
    plt.title("Importance des Variables", fontsize=14, pad=20)
    plt.grid(True, linestyle=':', alpha=0.4)
    
    plt.show()


"""""
Multiple Factor Analysis - les fonctions suivantes sont utilisées pour la MFA
"""

# 0. DÉFINITION DES GROUPES THÉMATIQUES : à vérifier avec nos vraies colonnes !

 
 
GROUPES_CANDIDATS = {
 
    'economie': [
        'revratio2022',      
        'capitalratio2022',  
        'prixm2ratio2022',   
        'perrsa2021',        
        'pchom2022',         
        'propf2022',         
        'perpropri2022',    
    ],
 
    'demographie': [
        'age2022',                    
        'densite_menages',            
        'popagglo2022',               
        'peretr2022',                 
        'perimmigre2022',             
        'part_etranger',              
        'part_francais',              
        'menage_moyen_3-4',
        'menage_moyen_5 et plus',
        'taille_agglo_moyenne_aglo',
        'taille_agglo_grande_agglo',
        'communeavececole_1.0',
    ],
 
    'csp_diplomes': [
        'pagri2022',         
        'pindp2022',         
        'pcadr2022',         
        'ppint2022',        
        'pempl2022',        
        'pouvr2022',        
        'paind2022',        
        'paica2022',        
        'pouem2022',        
        'pcapi2022',        
        'pbac2022',         
        'psup2022',         
        'perprive2021comm', 
    ],
 
    'participation': [
        'pparratio',           
        'pinsratio',           
        'pblancnulratio',      
        'percrimesdelits2020', 
    ],
 
    'vote': [
        'pvoteG',    
        'pvoteCG',   
        'pvoteC',    
        'pvoteCD',   
        'pvoteD',    
    ],
}
 
# Palette de couleurs pour les blocs politiques
COULEURS_BLOCS = {
    'pvoteG':  '#d73027',
    'pvoteCG': '#C46B7A',
    'pvoteC':  '#FFA500',
    'pvoteCD': '#91bfdb',
    'pvoteD':  '#4575b4',
}
 
# Palette de couleurs pour les groupes thématiques
COULEURS_GROUPES = {
    'economie':     '#2ca02c',
    'demographie':  '#ff7f0e',
    'csp_diplomes': '#9467bd',
    'participation':'#8c564b',
    'vote':         '#d62728',
}

def transfo_mfa(data, n_components=5):
    

    
    data = data.replace([np.inf, -np.inf], np.nan)

    # Remplacement des valeurs manquantes par la moyenne
    data = data.fillna(data.mean())

    # Standardisation des données
    scaler = StandardScaler()

    X_scaled = scaler.fit_transform(data)

    return X_scaled


def _groupes_to_prince(groupes):
    """
    Convertit un dict {nom_groupe: [col1, col2, ...]}
    en dict {col: nom_groupe} attendu par prince >= 0.13
    """
    return {col: groupe for groupe, cols in groupes.items() for col in cols}

def MFA(data, groupes, nb_compo=2):
    """
    Réalise une MFA sur un DataFrame avec des groupes de variables.
    
    Parameters
    ----------
    data : pd.DataFrame
    groupes : dict  -> ex: {'Physique': ['taille', 'poids'], 'Social': ['age', 'revenu']}
    nb_compo : int
    """

    groupes_valides = {}
    for nom, cols in groupes.items():
        existantes = [c for c in cols if c in data.columns]
        if existantes:
            groupes_valides[nom] = existantes

    # 2. Préparation et NETTOYAGE des données
    all_cols = list(np.concatenate(list(groupes_valides.values())))
    X = data[all_cols].copy()
    
    # Remplacement des infinis par NaN puis des NaN par la médiane
    X = X.replace([np.inf, -np.inf], np.nan)
    X = X.fillna(X.median())

    # 2. Calcul du nombre de composantes maximum possible
    n_vars = sum(len(g) for g in groupes_valides.values())
    n_max = min(len(data) - 1, n_vars - 1)

    mfa = pr.MFA(n_components=nb_compo, random_state=42)
    mfa = mfa.fit(X, groups=groupes_valides)

    print("=" * 50)
    print("STATISTIQUES DE LA MFA")
    stats = pd.DataFrame({
        'Valeur Propre': mfa.eigenvalues_,
        '% Variance': mfa.percentage_of_variance_,
        '% Cumulé': mfa.percentage_of_variance_.cumsum()
    })
    print(stats)
    print("=" * 50)

    # --- Carte des individus ---
    row_coords = mfa.row_coordinates(X)
    plt.figure(figsize=(10, 7))
    plt.scatter(row_coords[0], row_coords[1], c='steelblue', s=20, alpha=0.7, edgecolors='white')


    # On n'affiche que 50 noms au hasard pour éviter de saturer la mémoire
    
    n_labels = min(50, len(row_coords))
    sample_indices = random.sample(list(row_coords.index), n_labels)

    texts = []
    for idx in sample_indices:
        x, y = row_coords.loc[idx, 0], row_coords.loc[idx, 1]
        texts.append(plt.text(x, y, str(idx), fontsize=8))

    if texts:
        adjust_text(texts, arrowprops=dict(arrowstyle='->', color='gray', lw=0.5))
    plt.axhline(0, color='black', linestyle='--', linewidth=1, alpha=0.3)
    plt.axvline(0, color='black', linestyle='--', linewidth=1, alpha=0.3)

    v1 = mfa.percentage_of_variance_[0]
    v2 = mfa.percentage_of_variance_[1]
    plt.xlabel(f"Dimension 1 ({v1:.2f}%)")
    plt.ylabel(f"Dimension 2 ({v2:.2f}%)")
    plt.title("MFA : Carte des individus")
    plt.grid(True, linestyle=':', alpha=0.5)
    plt.show()

    return mfa


def analyse_dimensionnelle_mfa(data, groupes, seuils=[50, 80]):
    """
    Trace la variance cumulée et indique le nb de dimensions nécessaires
    pour atteindre les seuils donnés.
    """

    groupes_valides = {}
    for nom, cols in groupes.items():
        existantes = [c for c in cols if c in data.columns]
        if existantes:
            groupes_valides[nom] = existantes

    # 2. Préparation et NETTOYAGE des données
    all_cols = list(np.concatenate(list(groupes_valides.values())))
    X = data[all_cols].copy()
    
    # Remplacement des infinis par NaN puis des NaN par la médiane
    X = X.replace([np.inf, -np.inf], np.nan)
    X = X.fillna(X.median())

    # 2. Calcul du nombre de composantes maximum possible
    n_vars = sum(len(g) for g in groupes_valides.values())
    n_max = min(len(data) - 1, n_vars - 1)

    mfa = pr.MFA(n_components=n_max, random_state=42)
    mfa = mfa.fit(X, 
                  groups=groupes_valides)

    variance_cumulee = mfa.percentage_of_variance_.cumsum()
    if variance_cumulee.max() <= 1:
        variance_cumulee = variance_cumulee * 100

    print("\n--- RÉSULTATS ---")
    for seuil in seuils:
        idx = np.where(variance_cumulee >= seuil)[0]
        if len(idx) > 0:
            print(f"Pour {seuil}% de variance : il faut {idx[0] + 1} dimensions.")
        else:
            print(f"Seuil {seuil}% non atteint (Max: {variance_cumulee.max():.2f}%)")

    plt.figure(figsize=(10, 5))
    plt.plot(range(1, len(variance_cumulee) + 1), variance_cumulee, marker='o')
    plt.axhline(y=50, color='r', linestyle=':', label='Objectif 50%')
    plt.axhline(y=80, color='g', linestyle=':', label='Objectif 80%')
    plt.ylim(0, 105)
    plt.title("MFA – Variance cumulée")
    plt.xlabel("Nombre de dimensions")
    plt.ylabel("% Variance cumulée")
    plt.legend()
    plt.grid(True)
    plt.show()


def plot_mfa_partial_individuals(mfa, data, groupes, n_sample=20):
    """
    Superpose les individus partiels et globaux pour un échantillon de communes.
    """
    # 1. Préparation et NETTOYAGE (Identique à vos fonctions précédentes)
    groupes_valides = {}
    for nom, cols in groupes.items():
        existantes = [c for c in cols if c in data.columns]
        if existantes:
            groupes_valides[nom] = existantes

    all_cols = list(np.concatenate(list(groupes_valides.values())))
    X = data[all_cols].copy()
    X = X.replace([np.inf, -np.inf], np.nan).fillna(X.median())

    # 2. Échantillonnage (IMPORTANT : sinon le graphique est illisible)
    if len(X) > n_sample:
        X = X.sample(n_sample, random_state=42)

    # 3. Récupération des coordonnées
    global_coords = mfa.row_coordinates(X)
    # Correction de l'erreur : prince retourne un DataFrame avec MultiIndex
    partial_coords = mfa.partial_row_coordinates(X)
    
    group_names = list(groupes_valides.keys())
    colors = plt.cm.tab10.colors

    plt.figure(figsize=(12, 8))

    # --- Tracé des individus globaux ---
    plt.scatter(global_coords[0], global_coords[1],
                c='black', s=100, zorder=5, label='Global (Moyenne)', marker='D')

    # --- Tracé des individus partiels ---
    # Dans prince, partial_coords est souvent un DataFrame où l'index 
    # de colonne est un MultiIndex (Groupe, Composante)
    for i, gname in enumerate(group_names):
        # On extrait les coordonnées pour le groupe gname
        # Note : on utilise .loc ou la sélection de niveau selon la version
        try:
            p_x = partial_coords[gname][0]
            p_y = partial_coords[gname][1]
        except KeyError:
            # Sécurité pour certaines versions de prince
            p_x = partial_coords.loc[:, (gname, 0)]
            p_y = partial_coords.loc[:, (gname, 1)]

        plt.scatter(p_x, p_y, color=colors[i % len(colors)], 
                    s=60, alpha=0.7, label=f'Partiel – {gname}')

        # Relier chaque point partiel au point global
        for idx in X.index:
            plt.plot([global_coords.loc[idx, 0], p_x.loc[idx]],
                     [global_coords.loc[idx, 1], p_y.loc[idx]],
                     color=colors[i % len(colors)], lw=1, alpha=0.3)

    # Étiquettes des noms de communes
    for idx in X.index:
        plt.text(global_coords.loc[idx, 0], global_coords.loc[idx, 1], 
                 str(idx), fontsize=9, fontweight='bold')

    plt.axhline(0, color='black', ls='--', alpha=0.3)
    plt.axvline(0, color='black', ls='--', alpha=0.3)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.title(f"MFA – Zoom sur {n_sample} communes (Globaux vs Partiels)")
    plt.xlabel(f"Dim 1 ({mfa.percentage_of_variance_[0]:.2f}%)")
    plt.ylabel(f"Dim 2 ({mfa.percentage_of_variance_[1]:.2f}%)")
    plt.grid(True, linestyle=':', alpha=0.5)
    plt.tight_layout()
    plt.show()

def plot_mfa_variable_importance(mfa, data, groupes):
    """
    Carte de l'importance des groupes de variables sur chaque dimension.
    Utilise mfa.group_contributions_ (compatible avec les versions récentes de prince).
    """
    # 1. Préparation et nettoyage
    groupes_valides = {}
    for nom, cols in groupes.items():
        existantes = [c for c in cols if c in data.columns]
        if existantes:
            groupes_valides[nom] = existantes

    # 2. Récupération des contributions des groupes
    # Dans prince (v0.13+), c'est group_contributions_
    try:
        # On récupère les contributions (souvent normalisées ou brutes)
        Lg = mfa.column_contributions_
    except AttributeError:
        # Si group_contributions_ n'existe pas, on tente de le calculer via la variance par groupe
        # C'est une alternative robuste
        print("Attribut group_contributions_ non trouvé, tentative de récupération via les composantes...")
        return print("Erreur : Impossible de trouver les contributions des groupes dans cet objet MFA.")

    colors = plt.cm.tab10.colors
    fig, ax = plt.subplots(figsize=(10, 7))
    texts = []

    # 3. Tracé
    # Lg est généralement un DataFrame où l'index est le nom du groupe
    for i, gname in enumerate(Lg.index):
        # On récupère les deux premières dimensions
        # Note : Prince utilise parfois des colonnes nommées 0, 1 ou 'dim 0', 'dim 1'
        x = Lg.iloc[i, 0] 
        y = Lg.iloc[i, 1]
        
        ax.arrow(0, 0, x, y,
                 head_width=x*0.05 if x > 0 else 0.01, 
                 color=colors[i % len(colors)],
                 alpha=0.8, length_includes_head=True, lw=2.5)
        
        texts.append(ax.text(x, y, gname, fontsize=12, fontweight='bold',
                             color=colors[i % len(colors)]))

    if texts:
        adjust_text(texts, arrowprops=dict(arrowstyle='->', color='grey', lw=0.5))

    # Ajustement des limites : les contributions sont positives
    max_x = Lg.iloc[:, 0].max() * 1.2
    max_y = Lg.iloc[:, 1].max() * 1.2
    ax.set_xlim(-max_x*0.05, max_x)
    ax.set_ylim(-max_y*0.05, max_y)
    
    ax.axhline(0, color='black', ls='--', alpha=0.3)
    ax.axvline(0, color='black', ls='--', alpha=0.3)
    
    ax.set_xlabel(f"Contribution à l'Axe 1", fontsize=11)
    ax.set_ylabel(f"Contribution à l'Axe 2", fontsize=11)
    ax.set_title("MFA – Importance des groupes (Contributions)", fontsize=13, pad=20)
    ax.grid(True, linestyle=':', alpha=0.4)
    
    plt.show()

def plot_mfa_correlation_circle(mfa, data, groupes):
    """
    Cercle de corrélation des variables quantitatives.
    Une couleur par groupe.
    """
    # 1. Récupération des coordonnées (on enlève les parenthèses)
    col_coords = mfa.column_coordinates_

    group_names = list(groupes.keys())
    colors = plt.cm.tab10.colors

    fig, ax = plt.subplots(figsize=(10, 10))
    # Dessin du cercle unité
    circle = plt.Circle((0, 0), 1, color='grey', fill=False, ls='--', lw=1)
    ax.add_patch(circle)

    texts = []
    # On itère sur les groupes
    for i, (gname, gcols) in enumerate(groupes.items()):
        # On ne sélectionne que les variables du groupe présentes dans les résultats
        existing_cols = [c for c in gcols if c in col_coords.index]
        subset = col_coords.loc[existing_cols]
        
        for var in subset.index:
            # On récupère les coordonnées sur les deux premiers axes (colonnes 0 et 1)
            x, y = subset.iloc[subset.index.get_loc(var), 0], subset.iloc[subset.index.get_loc(var), 1]
            
            ax.arrow(0, 0, x, y, head_width=0.02,
                     color=colors[i % len(colors)], alpha=0.7, length_includes_head=True)
            
            texts.append(ax.text(x, y, var, fontsize=10,
                                 color=colors[i % len(colors)], fontweight='bold'))

    # Légende pour les groupes
    for i, gname in enumerate(group_names):
        ax.plot([], [], color=colors[i % len(colors)], label=gname, lw=2)
    ax.legend(loc='lower right', fontsize=10)

    # Ajustement des textes
    if texts:
        adjust_text(texts, arrowprops=dict(arrowstyle='->', color='gray', lw=0.4, alpha=0.4))

    # Cosmétique
    ax.set_xlim(-1.1, 1.1)
    ax.set_ylim(-1.1, 1.1)
    ax.axhline(0, color='black', ls='--', alpha=0.3)
    ax.axvline(0, color='black', ls='--', alpha=0.3)
    
    v1 = mfa.percentage_of_variance_[0]
    v2 = mfa.percentage_of_variance_[1]
    ax.set_xlabel(f"Dimension 1 ({v1:.2f}%)", fontsize=12)
    ax.set_ylabel(f"Dimension 2 ({v2:.2f}%)", fontsize=12)
    ax.set_title("MFA – Cercle de corrélation des variables", fontsize=14, pad=20)
    ax.grid(True, linestyle=':', alpha=0.4)
    
    plt.show()


#fin de la partie bien, le reste c'est bizarre
def resoudre_groupes(data, vote_supplementaire=True):
    """
    Résout les groupes thématiques en ne gardant que les colonnes présentes
    dans le dataframe. Affiche un rapport de résolution.
 
    Cette fonction est robuste : si une colonne candidate n'existe pas
    (ex. supprimée lors du nettoyage des données), elle est simplement ignorée.
    Le rapport d'affichage permet de vérifier rapidement quelles colonnes
    ont été trouvées ou manquantes.
 
    Paramètres
    ----------
    data                : pd.DataFrame   le dataframe post-transformation
    vote_supplementaire : bool
        Si True  → le groupe 'vote' ne sera PAS inclus dans les données MFA
                   (il sera projeté en supplémentaire après le calcul).
        Si False → le groupe 'vote' est inclus comme groupe ACTIF normal.
 
    Retourne
    --------
    groupes_resolus : dict {nom_groupe: [colonnes_présentes]}
        Dictionnaire des groupes avec uniquement les colonnes existantes.
        Le groupe 'vote' est exclu si vote_supplementaire=True.
    cols_vote : list[str]
        Colonnes du groupe vote (pour la projection supplémentaire).
    """
    cols_data = set(data.columns)
    groupes_resolus = {}
    cols_vote = []
 
    print("=== Résolution des groupes thématiques ===\n")
 
    for nom_groupe, candidats in GROUPES_CANDIDATS.items():
        trouvees   = [c for c in candidats if c in cols_data]
        manquantes = [c for c in candidats if c not in cols_data]
 
        if nom_groupe == 'vote':
            cols_vote = trouvees
            statut = "SUPPLÉMENTAIRE" if vote_supplementaire else "ACTIF"
            print(f"  [{statut}] {nom_groupe.upper():20s} : {len(trouvees):2d} colonnes trouvées")
        else:
            if len(trouvees) == 0:
                print(f"  [IGNORÉ]  {nom_groupe.upper():20s} : aucune colonne trouvée !")
                continue
            groupes_resolus[nom_groupe] = trouvees
            print(f"  [ACTIF]   {nom_groupe.upper():20s} : {len(trouvees):2d} colonnes trouvées")
 
        if manquantes:
            print(f"             → Absentes du dataframe : {manquantes}")
 
    # Ajout du vote en actif si demandé
    if not vote_supplementaire and cols_vote:
        groupes_resolus['vote'] = cols_vote
 
    print(f"\nTotal groupes actifs   : {len(groupes_resolus)}")
    print(f"Total colonnes actives : {sum(len(v) for v in groupes_resolus.values())}")
 
    return groupes_resolus, cols_vote

def preparer_donnees_mfa(data, groupes_resolus, variable_illustrative='Bloc_Dominant'):
    """
    Construit le dataframe et le dictionnaire `groups` pour prince.MFA.
 
    prince.MFA attend :
      - un DataFrame dont les colonnes sont dans l'ordre des groupes
        (toutes les colonnes d'un groupe doivent être CONTIGUËS)
      - un dict `groups` : {nom_groupe: nb_colonnes}
        qui spécifie le nombre de colonnes par groupe dans cet ordre
 
    La variable illustrative (Bloc_Dominant) est mise en INDEX,
    ce qui permet à prince de la retrouver pour les graphes mais
    de ne PAS l'inclure dans le calcul.
 
    Paramètres
    ----------
    data                 : pd.DataFrame
    groupes_resolus      : dict {nom: [colonnes]}   issu de resoudre_groupes()
    variable_illustrative: str
 
    Retourne
    --------
    df_mfa : pd.DataFrame   données ordonnées par groupe, indexées par Bloc_Dominant
    groups : dict           {nom_groupe: nb_colonnes}  pour prince.MFA
    """
    # Ordre des colonnes : toutes celles du groupe 1, puis groupe 2, etc.
    # (contiguïté obligatoire pour prince)
    colonnes_ordonnees = []
    groups = {}
    for nom, cols in groupes_resolus.items():
        colonnes_ordonnees.extend(cols)
        groups[nom] = cols
 
    # Construction du dataframe
    df_mfa = data[colonnes_ordonnees + [variable_illustrative]].copy()
 
    # Nettoyage : valeurs infinies → NaN → médiane
    df_mfa[colonnes_ordonnees] = (
        df_mfa[colonnes_ordonnees]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(df_mfa[colonnes_ordonnees].median())
    )
 
    # Mise en index de la variable illustrative
    df_mfa = df_mfa.set_index(variable_illustrative)
 
    print(f"\nDataFrame MFA : {df_mfa.shape[0]} communes × {df_mfa.shape[1]} variables")
    print(f"Groupes : { {k: v for k, v in groups.items()} }")
 
    return df_mfa, groups

def fit_mfa(df_mfa, groups, n_components=10):
    
    #Ajuste le modèle MFA sur les données.
 
    mfa = pr.MFA(
        n_components=n_components,
        n_iter=10,
        copy=True,
        check_input=True,
        engine='sklearn',
        random_state=42
    )
    mfa = mfa.fit(df_mfa, groups=groups)
    total_vars = sum(len(v) for v in groups.values())
    print(f"MFA ajustée : {n_components} composantes, "
          f"{total_vars} variables, "
          f"{len(groups)} groupes.")
    return mfa

def plot_inertie_mfa(mfa, seuil=80):
    # Affiche le scree plot de la MFA avec les pourcentages de variance expliquée et cumulée.
    explained  = np.array(mfa.percentage_of_variance_)
    cumulative = np.cumsum(explained)
    n_comp = len(explained)
 
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(range(1, n_comp + 1), explained,
           color='steelblue', alpha=0.75, label='Variance expliquée (%)')
    ax.plot(range(1, n_comp + 1), cumulative,
            marker='o', color='darkorange', lw=2, label='Cumulée (%)')
    ax.axhline(seuil, color='red', linestyle='--', lw=1,
               label=f'Seuil {seuil}%')
 
    idx_seuil = np.argmax(cumulative >= seuil)
    if idx_seuil > 0:
        ax.axvline(idx_seuil + 1, color='red', linestyle=':', alpha=0.5)
        ax.text(idx_seuil + 1.3, seuil + 1,
                f'{idx_seuil + 1} axes → {cumulative[idx_seuil]:.1f}%',
                color='red', fontsize=9)
 
    ax.set_xlabel('Composante principale', fontsize=12)
    ax.set_ylabel('Variance expliquée (%)', fontsize=12)
    ax.set_title('MFA — Éboulis des valeurs propres\n'
                 'Économie | Démographie | CSP/Diplômes | Participation | Vote',
                 fontsize=13)
    ax.set_xticks(range(1, n_comp + 1))
    ax.legend(fontsize=10)
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.show()
 
    print(f"\n{'Axe':>5} | {'Variance (%)':>13} | {'Cumulée (%)':>12}")
    print("-" * 37)
    for i, (e, c) in enumerate(zip(explained, cumulative)):
        print(f"  PC{i+1:>2} | {e:>12.2f}% | {c:>11.2f}%")

def plot_individus_mfa(mfa, df_mfa, ax1=0, ax2=1, n_sample=3000, titre=None):
    
    #Projette les communes dans le plan factoriel MFA colorées par bloc dominant.
 
    pct    = mfa.percentage_of_variance_
    coords = mfa.transform(df_mfa)
    if n_sample < len(coords):
        coords = coords.sample(n=n_sample, random_state=42)
 
    fig, ax = plt.subplots(figsize=(10, 8))
    for bloc, couleur in COULEURS_BLOCS.items():
        mask = coords.index == bloc
        if mask.sum() > 0:
            ax.scatter(coords.loc[mask, ax1], coords.loc[mask, ax2],
                       c=couleur, alpha=0.25, s=10, label=bloc)
 
    ax.axhline(0, color='k', lw=0.5, linestyle='--')
    ax.axvline(0, color='k', lw=0.5, linestyle='--')
    ax.set_xlabel(f'Composante {ax1+1} — {pct[ax1]:.1f}%', fontsize=12)
    ax.set_ylabel(f'Composante {ax2+1} — {pct[ax2]:.1f}%', fontsize=12)
    if titre is None:
        titre = (f'MFA — Communes dans le plan ({ax1+1},{ax2+1})\n'
                 f'colorées par bloc politique dominant (2022)')
    ax.set_title(titre, fontsize=13)
 
    handles = [mpatches.Patch(color=c, label=b, alpha=0.8)
               for b, c in COULEURS_BLOCS.items() if b in coords.index.unique()]
    ax.legend(handles=handles, title='Bloc dominant', fontsize=9, title_fontsize=10)
    ax.grid(alpha=0.2)
    plt.tight_layout()
    plt.show()

def plot_contributions_groupes(mfa, df_mfa, n_axes=6):
    
    pct = mfa.percentage_of_variance_[:n_axes]
 
    try:
        partial = mfa.partial_row_coordinates(df_mfa)
    except Exception as e:
        print(f"Impossible de calculer les coordonnées partielles : {e}")
        return
 
    groupes = list(partial.keys())
 
    # Contribution = variance des coords partielles du groupe sur l'axe k
    contribs = {}
    for grp in groupes:
        df_p = partial[grp]
        contribs[grp] = [
            df_p.iloc[:, k].var()
            for k in range(min(n_axes, df_p.shape[1]))
        ]
 
    df_contribs = pd.DataFrame(
        contribs,
        index=[f'PC{k+1}\n({pct[k]:.1f}%)' for k in range(n_axes)]
    )
    # Normalisation : chaque axe somme à 100%
    df_contribs = df_contribs.div(df_contribs.sum(axis=1), axis=0) * 100
 
    couleurs = [COULEURS_GROUPES.get(g, '#888888') for g in df_contribs.columns]
 
    fig, ax = plt.subplots(figsize=(11, 6))
    df_contribs.plot(kind='bar', stacked=True, ax=ax,
                     color=couleurs, edgecolor='white', linewidth=0.5)
 
    contrib_equi = 100 / len(groupes)
    ax.axhline(contrib_equi, color='black', linestyle='--', lw=1, alpha=0.5,
               label=f'Contribution équilibrée ({contrib_equi:.0f}%/groupe)')
 
    ax.set_ylabel('Contribution (%)', fontsize=12)
    ax.set_title('MFA — Contribution de chaque groupe thématique par axe\n'
                 '(barres empilées = 100% par axe)', fontsize=13)
    ax.set_xticklabels(df_contribs.index, rotation=0, fontsize=10)
    ax.legend(title='Groupe thématique', bbox_to_anchor=(1.01, 1),
              loc='upper left', fontsize=9)
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.show()
 
    print("\n=== Contribution des groupes par axe (%) ===")
    print(df_contribs.round(1).to_string())

def plot_cercle_correlations_mfa(mfa, df_mfa, groupes_resolus,
                                  ax1=0, ax2=1, top_n_par_groupe=5):
    """
    Cercle des corrélations coloré par groupe thématique.
 
    Pour chaque variable, on calcule sa corrélation avec les axes factoriels.
    Longueur de la flèche = qualité de représentation (cos² sur le plan).
    Angle entre deux flèches ≈ corrélation entre les deux variables.
 
    La coloration par groupe permet de voir :
    - si les variables d'un même groupe pointent dans la même direction
      (cohérence interne du groupe : ex. tous les indicateurs économiques
       sont-ils du même côté ?)
    - si des variables de groupes différents pointent dans la même direction
      (lien inter-groupes : ex. chômage ET vote à gauche vont-ils ensemble ?)
 
    On affiche les top_n_par_groupe variables les mieux représentées
    dans chaque groupe pour ne pas surcharger le graphe.
 
    Paramètres
    ----------
    mfa              : prince.MFA
    df_mfa           : pd.DataFrame
    groupes_resolus  : dict {nom: [colonnes]}
    ax1, ax2         : int
    top_n_par_groupe : int   nb de variables à afficher par groupe
    """
    pct = mfa.percentage_of_variance_
 
    coords  = mfa.transform(df_mfa).values
    X_std   = StandardScaler().fit_transform(df_mfa.values)
    all_cols = list(df_mfa.columns)
    col_to_idx = {c: i for i, c in enumerate(all_cols)}
 
    # Corrélation de chaque variable avec chaque axe factoriel
    n_axes = coords.shape[1]
    correlations = np.zeros((len(all_cols), n_axes))
    for j in range(len(all_cols)):
        for k in range(n_axes):
            if np.std(coords[:, k]) > 1e-10:
                correlations[j, k] = np.corrcoef(X_std[:, j], coords[:, k])[0, 1]
 
    fig, ax = plt.subplots(figsize=(10, 10))
    circle = plt.Circle((0, 0), 1, color='grey', fill=False, linestyle='--', lw=1)
    ax.add_patch(circle)
 
    for nom_groupe, cols in groupes_resolus.items():
        couleur = COULEURS_GROUPES.get(nom_groupe, '#888888')
        indices = [col_to_idx[c] for c in cols if c in col_to_idx]
        if not indices:
            continue
 
        # Top variables du groupe les mieux projetées sur ce plan
        cos2 = correlations[indices, ax1]**2 + correlations[indices, ax2]**2
        top_local   = np.argsort(cos2)[-top_n_par_groupe:]
        indices_top = [indices[i] for i in top_local]
 
        for idx in indices_top:
            vx = correlations[idx, ax1]
            vy = correlations[idx, ax2]
            ax.annotate('', xy=(vx, vy), xytext=(0, 0),
                        arrowprops=dict(arrowstyle='->', color=couleur, lw=1.5))
            ax.text(vx * 1.09, vy * 1.09, all_cols[idx],
                    fontsize=7, color=couleur, ha='center',
                    fontweight='bold' if nom_groupe == 'vote' else 'normal')
 
    ax.axhline(0, color='k', lw=0.5)
    ax.axvline(0, color='k', lw=0.5)
    ax.set_xlim(-1.3, 1.3)
    ax.set_ylim(-1.3, 1.3)
    ax.set_xlabel(f'Composante {ax1+1} — {pct[ax1]:.1f}%', fontsize=12)
    ax.set_ylabel(f'Composante {ax2+1} — {pct[ax2]:.1f}%', fontsize=12)
    ax.set_title(f'MFA — Cercle des corrélations (plan {ax1+1},{ax2+1})\n'
                 f'top {top_n_par_groupe} variables par groupe thématique', fontsize=12)
    ax.set_aspect('equal')
 
    handles = [mpatches.Patch(color=COULEURS_GROUPES.get(g, '#888'), label=g)
               for g in groupes_resolus.keys()]
    ax.legend(handles=handles, title='Groupe thématique', fontsize=9,
              title_fontsize=10, loc='lower right')
    plt.tight_layout()
    plt.show()


def plot_vote_supplementaire(mfa, df_mfa, data, cols_vote, ax1=0, ax2=1):
    """
    Projette les variables de vote comme variables supplémentaires.
 
    Utilisé quand vote_supplementaire=True : la MFA a été calculée SANS
    les variables de vote. On projette ensuite les scores de vote sur les
    axes pour voir si ces axes "retrouvent" le vote.
 
    Méthode : corrélation de Pearson entre chaque variable de vote
    (standardisée) et les coordonnées factorielles des individus.
    C'est la définition exacte de la projection d'une variable
    supplémentaire quantitative dans l'espace factoriel.
 
    Interprétation :
    - Flèche longue et alignée avec un axe → cet axe socio-éco "prédit"
      ce comportement électoral
    - Deux flèches opposées → les deux blocs s'opposent sur cet axe socio
    - Flèche courte → ce bloc n'est pas structuré par les variables socio
      incluses dans la MFA (vote "flottant" non expliqué par le socio-éco)
 
    Paramètres
    ----------
    mfa       : prince.MFA
    df_mfa    : pd.DataFrame
    data      : pd.DataFrame   le dataframe original (qui contient les cols vote)
    cols_vote : list[str]
    ax1, ax2  : int
    """
    pct    = mfa.percentage_of_variance_
    coords = mfa.transform(df_mfa)
 
    # Récupérer les variables de vote dans le bon ordre (aligné avec df_mfa)
    df_vote = data[cols_vote].iloc[:len(df_mfa)].copy()
    df_vote = (df_vote
               .replace([np.inf, -np.inf], np.nan)
               .fillna(df_vote.median()))
 
    X_vote_std = StandardScaler().fit_transform(df_vote.values)
 
    n_axes = coords.shape[1]
    corr_vote = np.zeros((len(cols_vote), n_axes))
    for j in range(len(cols_vote)):
        for k in range(n_axes):
            if np.std(coords.iloc[:, k]) > 1e-10:
                corr_vote[j, k] = np.corrcoef(
                    X_vote_std[:, j], coords.iloc[:, k]
                )[0, 1]
 
    fig, ax = plt.subplots(figsize=(8, 8))
    circle = plt.Circle((0, 0), 1, color='grey', fill=False, linestyle='--', lw=1)
    ax.add_patch(circle)
 
    for j, col in enumerate(cols_vote):
        vx      = corr_vote[j, ax1]
        vy      = corr_vote[j, ax2]
        couleur = COULEURS_BLOCS.get(col, '#888888')
        ax.annotate('', xy=(vx, vy), xytext=(0, 0),
                    arrowprops=dict(arrowstyle='->', color=couleur, lw=2.5))
        ax.text(vx * 1.12, vy * 1.12, col,
                fontsize=10, color=couleur, fontweight='bold', ha='center')
 
    ax.axhline(0, color='k', lw=0.5)
    ax.axvline(0, color='k', lw=0.5)
    ax.set_xlim(-1.3, 1.3)
    ax.set_ylim(-1.3, 1.3)
    ax.set_xlabel(f'Composante {ax1+1} — {pct[ax1]:.1f}%', fontsize=12)
    ax.set_ylabel(f'Composante {ax2+1} — {pct[ax2]:.1f}%', fontsize=12)
    ax.set_title(f'MFA — Variables de vote en SUPPLÉMENTAIRES (plan {ax1+1},{ax2+1})\n'
                 f'Les axes ont été construits SANS le vote', fontsize=13)
    ax.set_aspect('equal')
    plt.tight_layout()
    plt.show()
 
    # Tableau des corrélations pour l'interprétation
    n_show = min(6, n_axes)
    df_corr = pd.DataFrame(
        corr_vote[:, :n_show],
        index=cols_vote,
        columns=[f'PC{k+1}' for k in range(n_show)]
    )
    print("\n=== Corrélation des blocs de vote avec les axes MFA ===")
    print("(axes construits SANS les variables de vote)\n")
    print(df_corr.round(3).to_string())
    print("\nLecture : valeur proche de ±1 → cet axe socio-éco prédit ce vote.")
    print("          valeur proche de 0  → ce vote n'est pas expliqué par le socio-éco.")


def run_mfa_complet(data, vote_supplementaire=True, n_components=10,
                    n_sample=3000, top_n_cercle=5):
    """
    Pipeline complet MFA avec groupes thématiques multiples.
 
    Enchaîne : résolution des groupes → préparation → ajustement → tous les graphes.
 
    Paramètres
    ----------
    data                : pd.DataFrame   dataframe post-transformation
    vote_supplementaire : bool
        True  (recommandé pour la problématique) → axes construits sur
              Économie + Démographie + CSP/Diplômes + Participation.
              Le vote est projeté en supplémentaire.
              Répond à : "les variables socio-éco permettent-elles
              de retrouver le vote ?"
        False → le vote est un groupe actif comme les autres.
              Répond à : "quelle est la structure commune socio-vote ?"
    n_components        : int   nombre d'axes MFA (défaut 10)
    n_sample            : int   communes à afficher dans les biplots
    top_n_cercle        : int   variables par groupe dans le cercle des corrélations
 
    Retourne
    --------
    mfa       : prince.MFA      modèle ajusté
    df_mfa    : pd.DataFrame    données utilisées
    groupes   : dict            groupes résolus
    cols_vote : list[str]       colonnes du groupe vote
    """
    mode = "Vote SUPPLÉMENTAIRE" if vote_supplementaire else "Vote ACTIF"
    print("=" * 60)
    print(f"  MFA — {mode}")
    print(f"  Sujet : profils socio-éco et vote (Cagé & Piketty 2023)")
    print("=" * 60)
 
    print("\n[1/6] Résolution des groupes thématiques...")
    groupes, cols_vote = resoudre_groupes(data, vote_supplementaire)
 
    print("\n[2/6] Préparation du dataframe MFA...")
    df_mfa, groups = preparer_donnees_mfa(data, groupes)
 
    print("\n[3/6] Ajustement de la MFA...")
    mfa = fit_mfa(df_mfa, groups, n_components)
 
    print("\n[4/6] Éboulis des valeurs propres...")
    plot_inertie_mfa(mfa)
 
    print("\n[5/6] Biplots individus (communes)...")
    plot_individus_mfa(mfa, df_mfa, ax1=0, ax2=1, n_sample=n_sample)
    plot_individus_mfa(mfa, df_mfa, ax1=0, ax2=2, n_sample=n_sample)
 
    print("\n[6/6] Cercle des corrélations + contributions des groupes...")
    plot_cercle_correlations_mfa(mfa, df_mfa, groupes,
                                  ax1=0, ax2=1, top_n_par_groupe=top_n_cercle)
    plot_contributions_groupes(mfa, df_mfa)
 
    if vote_supplementaire and cols_vote:
        print("\n→ Projection du vote en supplémentaire...")
        plot_vote_supplementaire(mfa, df_mfa, data, cols_vote, ax1=0, ax2=1)
 
    print("\n✓ MFA terminée.")
    return mfa, df_mfa, groupes, cols_vote

