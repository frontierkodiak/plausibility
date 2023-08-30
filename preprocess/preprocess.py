import pandas as pd
from sklearn.preprocessing import StandardScaler
from tqdm import tqdm

def convert_to_julian(df):
    """
    Convert 'observed_on' column of DataFrame to datetime and add a 'julian_day' column.
    
    Parameters:
    - df: DataFrame containing 'observed_on' column
    
    Returns:
    - DataFrame with added 'julian_day' column
    """
    df['observed_on'] = pd.to_datetime(df['observed_on'])
    df['julian_day'] = df['observed_on'].dt.dayofyear
    return df


def remove_outliers_by_species(df, species_col, columns):
    """
    Remove outliers for given columns based on IQR method for each species.
    
    Parameters:
    - df: DataFrame
    - species_col: Column indicating species id
    - columns: List of columns for which to calculate and remove outliers
    
    Returns:
    - DataFrame with outliers removed
    """
    dfs = []
    unique_species = df[species_col].unique()
    
    # Adding progress bar
    for species in tqdm(unique_species, desc="Removing outliers"):
        species_df = df[df[species_col] == species].copy()
        
        for column in columns:
            Q1 = species_df[column].quantile(0.25)
            Q3 = species_df[column].quantile(0.75)
            IQR = Q3 - Q1
            filter_mask = (species_df[column] >= Q1 - 1.5 * IQR) & (species_df[column] <= Q3 + 1.5 * IQR)
            species_df = species_df[filter_mask]
        
        dfs.append(species_df)

    return pd.concat(dfs, axis=0)


def scale_columns(df, columns_to_scale):
    """
    Scale selected columns of the DataFrame.
    
    Parameters:
    - df: DataFrame
    - columns_to_scale: List of columns to scale
    
    Returns:
    - DataFrame with scaled columns
    """
    scaler = StandardScaler()
    df[columns_to_scale] = scaler.fit_transform(df[columns_to_scale])
    return df


# Load the dataset
df = pd.read_csv('/home/caleb/repo/plausibility/plausibility_expanded.csv')

# Convert to Julian Day
print("Converting dates to Julian Days...")
df = convert_to_julian(df)

# Remove outliers
print("Removing outliers for julian_day, latitude, and longitude columns by species...")
df_no_outliers = remove_outliers_by_species(df, 'L10_taxonID', ['julian_day', 'latitude', 'longitude'])

# Display the shape of original and cleaned dataframe
print(f"Original shape: {df.shape}, After outlier removal: {df_no_outliers.shape}")

# Scale columns
print("Scaling columns: latitude, longitude, and julian_day...")
df_no_outliers = scale_columns(df_no_outliers, ['latitude', 'longitude', 'julian_day'])

# Write the cleaned dataframe to a csv file
df_no_outliers.to_csv('/home/caleb/repo/plausibility/plausibility_cleaned.csv', index=False)
print("Processing completed and cleaned data saved to CSV.")
