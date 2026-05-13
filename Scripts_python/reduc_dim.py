import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import prince as pr
from adjustText import adjust_text
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

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
    #permet de regrouper les variables sous forme de dummies en une seule variable
    col_a_nett= [c for c in data.columns if c.startswith(debut)] 
    data[nom_col]=data[col_a_nett].idxmax(axis=1) # les autres sont en 0, donc ça prend la seule qui est en 1
    data.loc[data[col_a_nett].sum(axis=1) == 0, nom_col] = nom_manquant # si il y en a 0 ça veut dire que c'est celle qui a été enlevé qu'il met 
    data=data.drop(col_a_nett, axis=1, errors='ignore') 
    data[nom_col]=data[nom_col].str.replace(debut,'', regex=False) 
    return data 


def MCA(data, nb_compo=2):
    #code pour faire la MCA de nos données 
    mca = pr.MCA(n_components=nb_compo, random_state=42)
    mca = mca.fit(data)

    #statistiques
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
   #code pour analyser les variables à ajouter pour expliquer la variance 
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
    #plot les contributions/ le cos2 de chaque modfalité
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
    # contribution des variables au deux premiers axes 
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