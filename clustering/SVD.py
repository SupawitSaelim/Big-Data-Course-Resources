import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

df = pd.read_csv("./Clustering/fb_live_thailand.csv")
data = df[['num_sads', 'num_reactions']].values
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data)

U, S, VT = np.linalg.svd(data_scaled)

pca_features = np.dot(U[:, :2], np.diag(S[:2]))  # นำคอมโพเนนต์แรกและสองมาใช้

k_values = []
for i in range(2, 10):
    kmeans = KMeans(n_clusters=i, random_state=42)
    kmeans.fit(pca_features)
    score = kmeans.inertia_  # คำนวณ inertia
    k_values.append(score)
    print(f"Inertia Score for k = {i}: {score}")

best_k = np.argmin(k_values) + 2  # ใช้ค่า inertia ต่ำสุด
print("The best k:", best_k)

kmeans = KMeans(n_clusters=best_k, random_state=42)
kmeans.fit(pca_features)

predictions = kmeans.predict(pca_features)

plt.figure(figsize=(10, 6))
plt.scatter(pca_features[:, 0], pca_features[:, 1], c=predictions, cmap='viridis', marker='o', alpha=0.6)
plt.xlabel("Principal Component 1")
plt.ylabel("Principal Component 2")
plt.title("K-means Clustering with SVD")
plt.colorbar(label="Cluster")
plt.show()
