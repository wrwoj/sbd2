#!/bin/bash

# Output CSV file
output_file="output_results.csv"

# Initialize CSV header
echo "Run,Inserted_Keys,Skipped_Duplicates,Nodes_Saved,Nodes_Loaded,Nodes_Cache,Pages_Saved,Pages_Loaded,Pages_Cache,Metadata_Loaded,Metadata_Saved" > "$output_file"

# Run the command 10 times
for i in {1..10}; do
    echo "Running iteration $i..."
    
    # Execute the command and capture only the last 15 lines of the output
    output=$(python main.py --testfile commands.txt | tail -n 15)

    # Extract values using grep and awk
    inserted_keys=$(echo "$output" | grep "Inserted" | awk '{print $2}')
    skipped_duplicates=$(echo "$output" | grep "Inserted" | awk '{print $4}')
    nodes_saved=$(echo "$output" | grep "Nodes saved" | awk '{print $5}')
    nodes_loaded=$(echo "$output" | grep "Nodes loaded from disk" | awk '{print $6}')
    nodes_cache=$(echo "$output" | grep "Nodes loaded from cache" | awk '{print $6}')
    pages_saved=$(echo "$output" | grep "Pages saved" | awk '{print $5}')
    pages_loaded=$(echo "$output" | grep "Pages loaded from disk" | awk '{print $6}')
    pages_cache=$(echo "$output" | grep "Pages loaded from cache" | awk '{print $6}')
    metadata_loaded=$(echo "$output" | grep "Metadata loaded" | awk '{print $3}')
    metadata_saved=$(echo "$output" | grep "Metadata saved" | awk '{print $3}')

    # Append results to the CSV file
    echo "$i,$inserted_keys,$skipped_duplicates,$nodes_saved,$nodes_loaded,$nodes_cache,$pages_saved,$pages_loaded,$pages_cache,$metadata_loaded,$metadata_saved" >> "$output_file"
done

echo "Execution completed. Results saved to $output_file."
