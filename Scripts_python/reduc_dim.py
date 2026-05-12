import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import prince as pr
from adjustText import adjust_text

"""
PCA - les fonctions suivantes sont utilisées pour la PCA
"""
def blabla():
    return "pipoup"
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