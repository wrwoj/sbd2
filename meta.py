import os
import struct

# Constants
PAGE_SIZE = 256
NODE_PAGE_SIZE = 256

def read_underutilized_pages(metadata_filename="metadata.dat"):
    """
    Reads and returns the list of underutilized pages from the metadata file.
    """
    if not os.path.exists(metadata_filename):
        print(f"Metadata file '{metadata_filename}' does not exist.")
        return []

    with open(metadata_filename, "rb") as f:
        data = f.read()
        if len(data) < 4:
            return []
        page_count = struct.unpack('i', data[0:4])[0]
        underutilized_pages = []
        offset = 4
        for _ in range(page_count):
            page_num = struct.unpack('i', data[offset:offset + 4])[0]
            offset += 4
            underutilized_pages.append(page_num)
    return underutilized_pages

def read_free_nodes(metadata_filename="metadata_nodes.dat"):
    """
    Reads and returns the list of free node IDs from the metadata nodes file.
    """
    if not os.path.exists(metadata_filename):
        print(f"Metadata nodes file '{metadata_filename}' does not exist.")
        return []

    with open(metadata_filename, "rb") as f:
        data = f.read()
        if len(data) < 4:
            return []
        free_count = struct.unpack('i', data[0:4])[0]
        free_nodes = []
        offset = 4
        for _ in range(free_count):
            node_id = struct.unpack('i', data[offset:offset + 4])[0]
            offset += 4
            free_nodes.append(node_id)
    return free_nodes

def display_metadata(metadata_filename="metadata.dat", metadata_nodes_filename="metadata_nodes.dat"):
    """
    Displays metadata from the specified metadata files.
    """
    print(f"Reading metadata from '{metadata_filename}' and '{metadata_nodes_filename}'...\n")

    underutilized_pages = read_underutilized_pages(metadata_filename)
    print("Underutilized Pages:")
    if underutilized_pages:
        for page_num in underutilized_pages:
            print(f"  - Page {page_num}")
    else:
        print("  No underutilized pages found.")

    free_nodes = read_free_nodes(metadata_nodes_filename)
    print("\nFree Nodes:")
    if free_nodes:
        for node_id in free_nodes:
            print(f"  - Node {node_id}")
    else:
        print("  No free nodes found.")

def read_main_file(file_name="data.dat"):
    """
    Reads and displays basic information about the main file.
    """
    if not os.path.exists(file_name):
        print(f"Main file '{file_name}' does not exist.")
        return

    file_size = os.path.getsize(file_name)
    num_pages = file_size // PAGE_SIZE
    print(f"\nMain File '{file_name}':")
    print(f"  - File Size: {file_size} bytes")
    print(f"  - Number of Pages: {num_pages}")

if __name__ == "__main__":
    # Metadata files
    metadata_filename = "metadata.dat"
    metadata_nodes_filename = "metadata_nodes.dat"
    main_file_name = "data.dat"

    # Display metadata
    display_metadata(metadata_filename, metadata_nodes_filename)

    # Display main file information
    read_main_file(main_file_name)
