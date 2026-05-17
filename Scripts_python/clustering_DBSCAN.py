from sklearn.cluster import DBSCAN
import numpy as np
import matplotlib.pyplot as plt

"""
Ce fichier contient les fonctions utiles au clustering DBSCAN.
"""

def plot_nb_clusters(data, eps_values):
    """
    Affiche le nombre de clusters détectés par DBSCAN en fonction de différentes valeurs de epsilon (eps).
    """
    eps_range = eps_values
    n_clusters_found = []

    for e in eps_range:
        # min_samples est souvent fixé à 2*dim ou plus
        dbscan = DBSCAN(eps=e, min_samples=5)
        labels = dbscan.fit_predict(data)
        
        # On compte le nombre de clusters (en excluant le bruit identifié par -1)
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_clusters_found.append(n_clusters)

 
    plt.plot(eps_range, n_clusters_found, marker='o', color='#2b8cbe')
    plt.title('Nombre de clusters détectés', fontsize=14)
    plt.xlabel('Valeur de epsilon (eps)')
    plt.ylabel('Nombre de clusters')
    plt.grid(True, linestyle='--', alpha=0.6)

    plt.tight_layout()
    plt.show()