import json
from main import *
from collections import defaultdict

# Constants
test_main_file = "test_data.dat"
test_node_file = "test_btree_nodes.dat"
metadata_file = "test_metadata.dat"
node_metadata_file = "test_metadata_nodes.dat"

# Experiment parameters
num_records_list = [100, 500, 1000, 5000]  # Number of records to test
degree_values = [2, 3, 4, 5]  # Degrees of the B-tree to test

def run_experiment():
    results = defaultdict(list)

    for degree in degree_values:
        global d, max_keys, min_keys
        d = degree
        max_keys = 2 * d
        min_keys = d

        for num_records in num_records_list:
            # Initialize files for the experiment
            delete_metadata_files(test_main_file, test_node_file, metadata_file, node_metadata_file)
            generate_main_file(test_main_file, 1)
            add_underutilized_page(0, metadata_file)
            init_btree_nodes_file(test_node_file)
            init_node_metadata(node_metadata_file)
            init_metadata(metadata_file)

            # Reset global counters
            global_counters = {
                "nodes_saved_to_disk": 0,
                "nodes_loaded_from_disk": 0,
                "nodes_loaded_from_cache": 0,
                "pages_saved_to_disk": 0,
                "pages_loaded_from_disk": 0,
                "metadata_loaded": 0,
                "metadata_saved": 0
            }

            # Perform insertions
            addrandom(
                num_records,
                key_min=1,
                key_max=num_records * 2,
                main_file=test_main_file,
                node_filename=test_node_file,
                metadata_filename=metadata_file,
                node_metadata_filename=node_metadata_file
            )
            insertion_counters = global_counters.copy()

            # Perform searches
            search_keys = random.sample(range(1, num_records * 2), 10)
            for key in search_keys:
                search_key(key, None, test_node_file)
            search_counters = global_counters.copy()

            # Perform deletions
            delete_keys = random.sample(range(1, num_records), 10)
            for key in delete_keys:
                delete_key(key, test_node_file, test_main_file)
            deletion_counters = global_counters.copy()

            # Collect results
            results["degree"].append(degree)
            results["num_records"].append(num_records)
            results["insertion_reads"].append(insertion_counters["pages_loaded_from_disk"] + insertion_counters["nodes_loaded_from_disk"])
            results["insertion_writes"].append(insertion_counters["pages_saved_to_disk"] + insertion_counters["nodes_saved_to_disk"])
            results["search_reads"].append(search_counters["pages_loaded_from_disk"] + search_counters["nodes_loaded_from_disk"])
            results["search_writes"].append(search_counters["pages_saved_to_disk"] + search_counters["nodes_saved_to_disk"])
            results["deletion_reads"].append(deletion_counters["pages_loaded_from_disk"] + deletion_counters["nodes_loaded_from_disk"])
            results["deletion_writes"].append(deletion_counters["pages_saved_to_disk"] + deletion_counters["nodes_saved_to_disk"])

            print(f"Degree: {degree}, Records: {num_records}, Insertion Reads: {results['insertion_reads'][-1]}, "
                  f"Insertion Writes: {results['insertion_writes'][-1]}, Search Reads: {results['search_reads'][-1]}, "
                  f"Search Writes: {results['search_writes'][-1]}, Deletion Reads: {results['deletion_reads'][-1]}, "
                  f"Deletion Writes: {results['deletion_writes'][-1]}")

    # Save results to a file
    with open("experiment_results.json", "w") as f:
        json.dump(results, f, indent=4)

    return results

if __name__ == "__main__":
    experiment_results = run_experiment()
    print("Experiment completed. Results saved to 'experiment_results.json'.")