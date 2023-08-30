import pandas as pd
from sklearn.ensemble import IsolationForest

input_df = pd.read_csv('/home/caleb/repo/plausibility/plausibility_cleaned.csv')

# Define the features
features = ['L10_taxonID', 'latitude', 'longitude', 'julian_day']

# Initialize and train the Isolation Forest model
iso_forest = IsolationForest(contamination=0.05, random_state=42)
iso_forest.fit(input_df[features])

# Predict anomaly scores
anomaly_scores = iso_forest.decision_function(input_df[features])

# Convert anomaly scores to a range of [0, 1]
plausibility_scores = (anomaly_scores - anomaly_scores.min()) / (anomaly_scores.max() - anomaly_scores.min())

# Add the plausibility scores to the dataframe
input_df['plausibility_score'] = plausibility_scores

# Display the first few rows with plausibility scores
input_df.head()
