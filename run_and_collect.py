import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV files
file_paths = {
    '50 records': '50averages_results.csv',
    '500 records': '500averages_results.csv',
    '5000 records': '5000averages_results.csv'
}

# Initialize an empty dictionary to hold data
data = {}

# Read all files into a dictionary
for name, path in file_paths.items():
    data[name] = pd.read_csv(path, index_col=0)

# Extracting metrics for plotting
metrics = data['50 records'].columns.tolist()

# Create plots for Nodes_Loaded vs Metric and Nodes_Saved vs Metric
fig, axes = plt.subplots(3, 2, figsize=(14, 12))  # 3 rows, 2 columns

for i, (name, df) in enumerate(data.items()):
    # Nodes_Loaded vs Metric
    axes[i, 0].plot(metrics, df.loc['Nodes_Loaded'], marker='o', label='Nodes_Loaded')
    axes[i, 0].set_title(f"{name}: Loaded Nodes vs d")

    axes[i, 0].grid()

    # Nodes_Saved vs Metric
    axes[i, 1].plot(metrics, df.loc['Nodes_Saved'], marker='o', label='Nodes_Saved', color='orange')
    axes[i, 1].set_title(f"{name}: Saved Nodes vs d")

    axes[i, 1].grid()

plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.show()
