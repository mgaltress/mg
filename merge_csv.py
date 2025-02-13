import os
import pandas as pd

INPUT_FOLDER = r"C:\Users\mgalt\Desktop\Corktree\FY24"
OUTPUT_FILE = r"C:\Users\mgalt\Desktop\Corktree\FY24\merged_output.csv"

def merge_csv_files(INPUT_FOLDER, OUTPUT_FILE):
    # List all CSV files in the input folder
    all_files = [f for f in os.listdir(INPUT_FOLDER) if f.endswith('.csv')]
    if not all_files:
        print("No CSV files found in the specified folder.")
        return
    
    df_list = []  # Empty list to store DataFrames

    # Loop through each file and read it
    for file in all_files:
        print(f"Processing file: {file}")
        file_path = os.path.join(INPUT_FOLDER, file)
        
        try:
            # Read CSV into DataFrame without treating any column as the index
            df = pd.read_csv(file_path, index_col=False)

            # Ensure we do not have an unwanted 'Unnamed: 0' column by dropping it
            if 'Unnamed: 0' in df.columns:
                df = df.drop(columns=['Unnamed: 0'])
            
            df = df[df['Item Name'].notna()]

            df['Sale Date'] = pd.to_datetime(df['Sale Date'], format='%Y%m%d', errors='coerce')

            # Ensure the DataFrame has the correct columns (same for all files)
            if df_list and list(df.columns) != list(df_list[0].columns):
                print(f"Warning: Columns in {file} do not match the first file's columns.")
                continue  # Skip this file if columns do not match

            df_list.append(df)  # Add DataFrame to list
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue  # Skip this file and continue with others

    # Concatenate all DataFrames into one
    if df_list:
        merged_df = pd.concat(df_list, ignore_index=True)
        
        # Save the merged DataFrame to the output file
        merged_df.to_csv(OUTPUT_FILE, index=False)  # Don't include the index in the output file
        print(f"File merged successfully: {OUTPUT_FILE}")
    else:
        print("No files were successfully processed.")

# Run the function
merge_csv_files(INPUT_FOLDER, OUTPUT_FILE)