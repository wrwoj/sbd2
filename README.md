# B-Tree Implementation with Persistent Storage

## Overview
This project implements a **B-Tree data structure** that supports persistent storage and efficient insertion, deletion, and search operations. It is designed for use in database indexing and large-scale key-value storage.

## Features
- **Persistent Storage:** Data is stored in files to maintain state across executions.
- **B-Tree Structure:** Supports efficient insertion, search, and deletion operations.
- **LRU Caching:** Uses an in-memory cache to optimize disk access.
- **Page Management:** Implements a paginated storage model to handle large datasets.
- **CLI Interface:** Provides a command-line interface for interacting with the B-Tree.
- **Visualization:** Generates a graphical representation of the B-Tree.

## Dependencies
Ensure you have the following libraries installed:

    sudo apt-get install graphviz feh

## Compilation & Execution
Use a C++ compiler like `g++`:

    g++ -std=c++11 main.cpp -o btree
    ./btree

## Code Structure
    ├── main.py                      # Main execution file
    ├── btree.py                     # B-Tree class implementation
    ├── storage.py                   # Persistent storage and caching
    ├── cli.py                        # Command-line interface for the B-Tree
    ├── utils.py                      # Helper functions for file operations
    ├── visualization.py              # B-Tree visualization tools
    ├── README.md                     # Project documentation

## Commands
The B-Tree can be managed using the CLI. Below are the available commands:

- **CREATE `<base_name>`** - Initializes a new B-Tree with the given base name.
- **LOAD `<base_name>`** - Loads an existing B-Tree.
- **INSERT `<key> <pA> <pB> <pAuB>`** - Inserts a key with associated values.
- **DELETE `<key>`** - Removes a key from the B-Tree.
- **UPDATE `<key> <new_pA> <new_pB> <new_pAuB>`** - Updates an existing key.
- **SEARCH `<key>`** - Searches for a key in the B-Tree.
- **PRINT** - Displays all records in the main storage file.
- **VISUALIZE** - Generates and opens a graphical visualization of the B-Tree.
- **ADDRANDOM `<num_keys>`** - Inserts a specified number of random keys.
- **EXIT** - Exits the program.

## Author
Wiktor Wojtyna
