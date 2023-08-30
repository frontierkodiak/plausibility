import pandas as pd
from tqdm import tqdm
from pathlib import Path
from redis import Redis
import json

def connect_to_redis(host, port):
    r = Redis(host=host, port=port, decode_responses=True)
    return r

def process_metadata(metadata_path, redis_connection):
    # Read the metadata csv file
    metadata_path = Path(metadata_path)
    metadata = pd.read_csv(metadata_path)

    # Prepare columns for the new DataFrame
    columns = metadata.columns.tolist() + ['L{}_taxonID'.format(i) for i in [10, 20, 30, 40]]
    
    new_metadata_data = []

    for _, row in tqdm(metadata.iterrows(), total=metadata.shape[0]):
        # Create new row data
        new_row = row.to_dict()
        taxon_id = new_row['taxon_id']
        
        # Fetch record from Redis
        results = redis_connection.execute_command('FT.SEARCH', 'taxon_ID_idx', '@taxon_id:[{} {}]'.format(taxon_id, taxon_id))
        if results[0] > 0:
            # Parse the JSON data into a dictionary
            redis_record = json.loads(results[2][1])
        else:
            redis_record = None

        if redis_record:
            for i in [10, 20, 30, 40]:
                try:
                    new_row['L{}_taxonID'.format(i)] = redis_record['L{}'.format(i)]['taxon_id']
                except:
                    new_row['L{}_taxonID'.format(i)] = None
        else:
            for i in [10, 20, 30, 40]:
                new_row['L{}_taxonID'.format(i)] = None
        
        new_metadata_data.append(new_row)

    new_metadata = pd.DataFrame(new_metadata_data, columns=columns)
    
    # Drop rows without a L10_taxonID value
    new_metadata.dropna(subset=['L10_taxonID'], inplace=True)
    
    # Drop the original taxon_id column
    new_metadata.drop(columns=['taxon_id'], inplace=True)
    
    # Save the expanded CSV
    new_metadata.to_csv('/home/caleb/repo/plausibility/plausibility_expanded.csv', index=False)

    # Save a sample
    sample = new_metadata.sample(n=3000)
    sample.to_csv('/home/caleb/repo/plausibility/plausibility_expanded_sample.csv', index=False)

if __name__ == "__main__":
    # Connect to Redis
    redis_connection = connect_to_redis("localhost", 6379)

    # Process and save the expanded CSV
    process_metadata('/pond/Polli/ibridaExports/plausibility/plausibility-metadata-v0.csv', redis_connection)
