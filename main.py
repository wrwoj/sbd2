import struct
import os
import subprocess
import bisect
import sys
import argparse
import random
from collections import OrderedDict
import signal

# Define the LRU cache with a fixed size
CACHE_SIZE = 0
node_cache = OrderedDict()

PAGE_CACHE_SIZE = 10
page_cache = OrderedDict()

def mark_node_dirty(node_id):
    if node_id in node_cache:
        node, _ = node_cache.pop(node_id)
        node_cache[node_id] = (node, True)


global_counters = {
    "nodes_saved_to_disk": 0,
    "nodes_loaded_from_disk": 0,
    "nodes_loaded_from_cache": 0,
    "pages_saved_to_disk": 0,
    "pages_loaded_from_disk": 0,
    "pages_loaded_from_cache": 0,
    "metadata_loaded": 0,
    "metadata_saved": 0
}

current_files = {
    'main_file': "data.dat",
    'node_file': "btree_nodes.dat",
    'metadata_file': "metadata.dat",
    'node_metadata_file': "metadata_nodes.dat"
}


# -----------------------------------------------------------
# Configuration
# -----------------------------------------------------------
record_format = 'i d d d'  # key, P(A), P(B), P(AuB)
record_size = struct.calcsize(record_format)

page_size = 256
max_records_per_page = (page_size - 4) // record_size

# B-tree node parameters
node_page_size = 555
d = 2
max_keys = 2 * d
min_keys = d

root = 0
last_page = 1
metadata_filename = "metadata.dat"


# -----------------------------------------------------------
# Record and Page Classes
# -----------------------------------------------------------
class Record:
    def __init__(self, key, p_a, p_b, p_aub):
        self.key = key
        self.p_a = p_a
        self.p_b = p_b
        self.p_aub = p_aub


class Page:
    def __init__(self, records=None):
        self.records = records if records else []

    def pack(self):
        data = struct.pack('i', len(self.records))
        # records are already kept sorted by key
        for r in self.records:
            data += struct.pack(record_format, r.key, r.p_a, r.p_b, r.p_aub)
        if len(data) < page_size:
            data += b'\x00' * (page_size - len(data))
        return data

    @staticmethod
    def unpack(data):
        n = struct.unpack('i', data[0:4])[0]
        recs = []
        offset = 0
        for _ in range(n):
            chunk = data[offset:offset + record_size]
            key, p_a, p_b, p_aub = struct.unpack(record_format, chunk)
            recs.append(Record(key, p_a, p_b, p_aub))
            offset += record_size
        return Page(recs)


# -----------------------------------------------------------
# BTreeNode Class
# -----------------------------------------------------------
class BTreeNode:
    def __init__(self, node_id, keys=None, children=None, leaf=True, parent_id=-1):
        self.node_id = node_id
        self.keys = keys if keys else []  # keys will be a list of tuples (key, page)
        self.children = children if children else []
        self.leaf = leaf
        self.parent_id = parent_id

    def to_bytes(self):
        n = len(self.keys)
        leaf_byte = 1 if self.leaf else 0
        data = struct.pack('i', self.node_id)
        data += struct.pack('B', leaf_byte)
        data += struct.pack('i', n)
        data += struct.pack('i', self.parent_id)

        # Each key is now (k, p)
        # max_keys keys, each 8 bytes total (2 ints)
        for i in range(max_keys):
            if i < n:
                k, p = self.keys[i]
            else:
                k, p = 0, 0
            data += struct.pack('ii', k, p)

        # max_children = max_keys + 1 children, each 4 bytes
        max_children = max_keys + 1
        for i in range(max_children):
            if i < len(self.children):
                c = self.children[i]
            else:
                c = -1
            data += struct.pack('i', c)

        if len(data) < node_page_size:
            data += b'\x00' * (node_page_size - len(data))

        return data

    @staticmethod
    def from_bytes(data):
        node_id = struct.unpack('i', data[0:4])[0]
        leaf_byte = data[4]
        leaf = (leaf_byte == 1)
        n = struct.unpack('i', data[5:9])[0]
        parent_id = struct.unpack('i', data[9:13])[0]

        keys = []
        offset = 13
        # Each key: (k, p) = 8 bytes
        for i in range(max_keys):
            k, p = struct.unpack('ii', data[offset:offset + 8])
            offset += 8
            if i < n:
                keys.append((k, p))

        children = []
        for i in range(max_keys + 1):
            c = struct.unpack('i', data[offset:offset + 4])[0]
            offset += 4
            if c != -1:
                children.append(c)

        return BTreeNode(node_id, keys, children, leaf, parent_id)


def generate_main_file(filename="data.dat", num_records=0):
    if os.path.exists(filename):
        os.remove(filename)

    all_records = []  # Start with no records

    with open(filename, "wb") as f:
        page = Page(all_records)
        f.write(page.pack())

# Define the page cache with a fixed size and track dirty pages




def read_page(file_path, page_num, size, mode="rb"):
    global page_cache, global_counters, PAGE_CACHE_SIZE

    if  (PAGE_CACHE_SIZE != 0) and (page_num in page_cache):
        # Move to end to mark as recently used
        page_data = page_cache.pop(page_num)
        page_cache[page_num] = page_data
        global_counters["pages_loaded_from_cache"] += 1
        return Page.unpack(page_data)

    # Read from disk
    with open(file_path, mode) as f:
        f.seek(page_num * size)
        page_bytes = f.read(size)
        if not page_bytes:
            # If page does not exist, return empty page
            page = Page()
        else:
            page = Page.unpack(page_bytes)
        global_counters["pages_loaded_from_disk"] += 1

    # Add to cache
    page_data = page.pack()
    page_cache[page_num] = page_data

    if PAGE_CACHE_SIZE != 0 and len(page_cache) > PAGE_CACHE_SIZE:
        evicted_page_num, evicted_data = page_cache.popitem(last=False)
        with open(file_path, "r+b") as f:
            f.seek(evicted_page_num * page_size)
            f.write(evicted_data)
            f.flush()
            global_counters["pages_saved_to_disk"] += 1

    return page


def write_page(file_path, page_num, page):
    global page_cache, global_counters

    if page_num in page_cache:
        page_cache.pop(page_num)
        page_cache[page_num] = page.pack()
    else:
        with open(file_path, "r+b") as f:
            f.seek(page_num * page_size)
            f.write(page.pack())
            f.flush()
            global_counters["pages_saved_to_disk"] += 1


    # Evict if cache size exceeded
    if len(page_cache) > PAGE_CACHE_SIZE:
        evicted_page_num, evicted_data = page_cache.popitem(last=False)
        with open(file_path, "r+b") as f:
            f.seek(evicted_page_num * page_size)
            f.write(evicted_data)
            f.flush()
            global_counters["pages_saved_to_disk"] += 1





def handle_exit_signal(signum, frame):
    """
    Handle signals like SIGINT and SIGTERM to perform clean-up before exiting.
    """
    print("\nReceived interrupt signal. Cleaning up before exiting...")

    # Save any dirty cached nodes to disk
    for node_id, (node, is_dirty) in node_cache.items():
        if is_dirty:
            print(f"Saving dirty node {node_id} to disk...")
            save_node(node, current_files['node_file'], mode="r+b")

    # Save cached pages to disk
    for page_num, page_data in page_cache.items():
        print(f"Flushing page {page_num} to disk...")
        with open(current_files['main_file'], "r+b") as f:
            f.seek(page_num * page_size)
            f.write(page_data)
            f.flush()

    print("All data saved. Exiting program gracefully.")
    sys.exit(0)  # Exit the program cleanly


def insert_record_in_main_file(record, main_file="data.dat", metadata_filename="metadata.dat"):
    global global_counters
    underutilized_pages = load_underutilized_pages(metadata_filename)

    if underutilized_pages:
        page_num = underutilized_pages.pop(0)
        print('from list')
    else:
        global last_page
        add_underutilized_page(last_page, metadata_filename)
        page_num = last_page
        last_page +=1
        print('from filesize')

    page = read_page(main_file, page_num, page_size)



    keys = [r.key for r in page.records]
    pos = bisect.bisect_left(keys, record.key)
    page.records.insert(pos, record)

    if len(page.records) == max_records_per_page:
        print('should remove')
        remove_underutilized_page(page_num, metadata_filename)

    write_page(main_file, page_num, page)

    return page_num


def print_main_file(filename="data.dat"):
    global global_counters
    if not os.path.exists(filename):
        print("Main file does not exist.")
        return
    file_size = os.path.getsize(filename)
    num_pages = file_size // page_size

    with open(filename, "rb") as f:
        for p in range(num_pages):
            data = f.read(page_size)
            global_counters["pages_loaded_from_disk"] += 1  # Increment the counter
            page = Page.unpack(data)
            print(f"Page {p}: {len(page.records)} records")
            for r in page.records:
                print(f"  Key={r.key}, P(A)={r.p_a}, P(B)={r.p_b}, P(A∪B)={r.p_aub}")


def init_btree_nodes_file(node_filename="btree_nodes.dat"):
    if os.path.exists(node_filename):
        print("Node file exists. Loading existing root node...")
        return  # Avoid overwriting existing data
    root_node = BTreeNode(0, keys=[], leaf=True, parent_id=-1)
    save_node(root_node, node_filename, "wb")
    global root
    root = root_node.node_id


def read_node(node_to_read_id, node_filename="btree_nodes.dat"):
    global global_counters
    if node_to_read_id in node_cache:
        global_counters["nodes_loaded_from_cache"] += 1
        node, t = node_cache.pop(node_to_read_id)
        node_cache[node_to_read_id] = (node, t)
        return node

    if not os.path.exists(node_filename):
        return None

    with open(node_filename, "rb") as f:
        f.seek(node_to_read_id * node_page_size)
        data = f.read(node_page_size)
        if len(data) < node_page_size:
            return None

    global_counters["nodes_loaded_from_disk"] += 1
    node = BTreeNode.from_bytes(data)
    node_cache[node_to_read_id] = (node, False)
    if len(node_cache) > CACHE_SIZE:
        evicted_node_id, (evicted_node, dirty) = node_cache.popitem(last=False)
        if CACHE_SIZE > 0 and dirty:
            with open(node_filename, "r+b") as f:
                f.seek(evicted_node_id * node_page_size)
                f.write(evicted_node.to_bytes())
                f.flush()
                global_counters["nodes_saved_to_disk"] += 1

    return node


def save_node(node_to_save, node_filename="btree_nodes.dat", mode="r+b"):
    global global_counters

    if node_to_save.node_id in node_cache:
        node_cache.move_to_end(node_to_save.node_id)
        node_cache[node_to_save.node_id] = (node_to_save, True)
    else:
        with open(node_filename, mode) as f:
            f.seek(node_to_save.node_id * node_page_size)
            f.write(node_to_save.to_bytes())
            f.flush()
        global_counters["nodes_saved_to_disk"] += 1



def load_int_list_from_file(filename):
    if not os.path.exists(filename):
        return []
    with open(filename, "rb") as f:
        data = f.read()
    if len(data) < 4:
        return []
    count = struct.unpack('i', data[0:4])[0]
    int_list = []
    offset = 4
    for _ in range(count):
        val = struct.unpack('i', data[offset:offset + 4])[0]
        offset += 4
        int_list.append(val)
    return int_list


def save_int_list_to_file(filename, int_list):
    with open(filename, "wb") as f:
        f.write(struct.pack('i', len(int_list)))
        for val in int_list:
            f.write(struct.pack('i', val))


def load_underutilized_pages(metadata_filename="metadata.dat"):
    global global_counters
    global_counters["metadata_loaded"] += 1
    return load_int_list_from_file(metadata_filename)


def save_underutilized_pages(pages, metadata_filename="metadata.dat"):
    global global_counters
    save_int_list_to_file(metadata_filename, pages)
    global_counters["metadata_saved"] += 1


def search_key(x, current_node_id=None, node_filename="btree_nodes.dat"):
    if current_node_id is None:
        current_node_id = root
    current_node = read_node(current_node_id, node_filename)
    print(f'current_node={current_node.children}')
    if current_node is None:
        print('1')
        return current_node, 'not found'

    if len(current_node.keys) > 0 and x < current_node.keys[0][0]:
        if current_node.leaf:
            print('2')
            return current_node, 'not found'
        print('5aa5')
        return search_key(x, current_node.children[0], node_filename)

    for i, (k, p) in enumerate(current_node.keys):
        if k == x:
            return current_node, 'found'
        elif k > x:
            if current_node.leaf:
                print('3')
                return current_node, 'not found'
            print('6')
            print(f'important{i}')
            return search_key(x, current_node.children[i], node_filename)

    if current_node.leaf:
        print('4')
        return current_node, 'not found'

    return search_key(x, current_node.children[-1], node_filename)


def add_key_to_node(node, key, page, node_filename="btree_nodes.dat"):
    if node is None:
        return

    keys_only = [k[0] for k in node.keys]
    insert_pos = bisect.bisect_left(keys_only, key)
    node.keys.insert(insert_pos, (key, page))
    save_node(node, node_filename)

    if len(node.keys) > max_keys:
        if node.parent_id == -1:
            split_node(node, node_filename)
        else:
            try_compensation(node, key, page, node_filename=node_filename)


def insert_key(x, a, main_file="data.dat", node_filename="btree_nodes.dat", metadata_filename="metadata.dat", loading= False):
    node, is_found = search_key(x, None, node_filename)
    print(f'POSITION {node.node_id}')
    if is_found == 'found':
        return 'ALREADY EXISTS!'

    if not os.path.exists(main_file):
        return 'ERROR: Main file not found!'

    new_record = Record(x, a[0], a[1], a[2])
    if not loading:
        page_num = insert_record_in_main_file(new_record, main_file, metadata_filename)

    add_key_to_node(node, x, page_num, node_filename)
    return 'OK'


def load_all_nodes(node_filename="btree_nodes.dat"):
    if not os.path.exists(node_filename):
        return []
    global node_cache, global_counters, current_files


    nodes = []
    file_size = os.path.getsize(node_filename)
    node_count = file_size // node_page_size
    for nid in range(node_count):
        node = read_node(nid, node_filename)
        if node is not None:
            nd = {
                "id": node.node_id,
                "leaf": node.leaf,
                "keys": node.keys,
                "children": node.children
            }
            nodes.append(nd)
    return nodes


def generate_dot(nodes, dot_filename="tree.dot"):
    with open(dot_filename, "w") as f:
        f.write("digraph BTree {\n")
        f.write("  node [shape=plaintext];\n")  # Use plaintext shape for HTML labels

        # Define each node with HTML-like labels and ports
        for n in nodes:
            if not n["keys"]:
                label = "<table border='0' cellborder='1' cellspacing='0'>"
                label += "<tr><td port='f0'></td></tr></table>"
            else:
                label = "<table border='0' cellborder='1' cellspacing='0'>"
                label += "<tr>"
                num_keys = len(n["keys"])
                # For each key, create a port for the child before the key
                for i in range(num_keys + 1):
                    label += f"<td port='f{i}'></td>"
                    if i < num_keys:
                        label += f"<td>{n['keys'][i][0]}</td>"
                label += "</tr></table>"

            if n["leaf"]:
                f.write(f'  node{n["id"]} [label=< {label} >, style=filled, fillcolor=lightgrey];\n')
            else:
                f.write(f'  node{n["id"]} [label=< {label} >];\n')

        # Define edges with specific ports to maintain child order
        for n in nodes:
            for i, c in enumerate(n["children"]):
                # Connect to the corresponding port 'fi' in the parent node
                f.write(f'  node{n["id"]}:f{i} -> node{c};\n')

        f.write("}\n")


def visualize_tree(dot_filename="tree.dot", output_png="tree.png"):
    subprocess.run(["dot", "-Tpng", dot_filename, "-o", output_png], check=True)
    subprocess.run(["feh", output_png])


def try_compensation(overflown_node, key, page, node_filename="btree_nodes.dat"):
    parent = read_node(overflown_node.parent_id, node_filename)
    if parent is None:
        return False

    try:
        idx = parent.children.index(overflown_node.node_id)
    except ValueError:
        print(f"Node {overflown_node.node_id} is not a child of its parent {overflown_node.parent_id}.")
        return False

    left_sibling_id = parent.children[idx - 1] if idx > 0 else None
    right_sibling_id = parent.children[idx + 1] if idx < len(parent.children) - 1 else None

    combined_keys = []
    combined_children = []

    if left_sibling_id is not None:
        left_sibling = read_node(left_sibling_id, node_filename)
        if len(left_sibling.keys) < max_keys:
            combined_keys.extend(left_sibling.keys)
            combined_keys.append(parent.keys[idx - 1])
            combined_keys.extend(overflown_node.keys)

            if not overflown_node.leaf:
                combined_children.extend(left_sibling.children)
                combined_children.extend(overflown_node.children)

            mid_index = len(combined_keys) // 2
            parent.keys[idx - 1] = combined_keys[mid_index]

            left_sibling.keys = combined_keys[:mid_index]
            overflown_node.keys = combined_keys[mid_index + 1:]

            if not overflown_node.leaf:
                left_sibling.children = combined_children[:mid_index + 1]
                overflown_node.children = combined_children[mid_index + 1:]

            save_node(left_sibling, node_filename)
            save_node(overflown_node, node_filename)
            save_node(parent, node_filename)
            return True

    if right_sibling_id is not None:
        # print(f'reading node {right_sibling_id} during compensation')
        right_sibling = read_node(right_sibling_id, node_filename)
        if len(right_sibling.keys) < max_keys:
            combined_keys.extend(overflown_node.keys)
            combined_keys.append(parent.keys[idx])
            combined_keys.extend(right_sibling.keys)

            if not overflown_node.leaf:
                combined_children.extend(overflown_node.children)
                combined_children.extend(right_sibling.children)

            mid_index = len(combined_keys) // 2
            parent.keys[idx] = combined_keys[mid_index]

            overflown_node.keys = combined_keys[:mid_index]
            right_sibling.keys = combined_keys[mid_index + 1:]

            if not overflown_node.leaf:
                overflown_node.children = combined_children[:mid_index + 1]
                right_sibling.children = combined_children[mid_index + 1:]

            save_node(overflown_node, node_filename)
            save_node(right_sibling, node_filename)
            save_node(parent, node_filename)
            return True

    split_node(overflown_node, node_filename)
    return False


def create_or_reuse_node(node_data, node_filename="btree_nodes.dat", metadata_filename="metadata_nodes.dat"):
    free_nodes = load_free_nodes(metadata_filename)
    if free_nodes:

        # Use a free node
        node_id = free_nodes.pop(0)
        save_free_nodes(free_nodes, metadata_filename)
    else:
        # No free node, allocate a new one
        file_size = os.path.getsize(node_filename)
        node_id = file_size // node_page_size

    node = BTreeNode(node_id, **node_data)
    save_node(node, node_filename)
    return node_id


def split_node(overflown_node, node_filename="btree_nodes.dat", metadata_filename="metadata_nodes.dat"):
    global root

    # Step 1: Allocate a new node
    new_node_id = get_free_node(metadata_filename)
    if new_node_id is None:
        file_size = os.path.getsize(node_filename)
        new_node_id = file_size // node_page_size

    new_node = BTreeNode(new_node_id, leaf=overflown_node.leaf, parent_id=overflown_node.parent_id)

    # Step 2: Redistribute keys and children
    mid_index = len(overflown_node.keys) // 2
    middle_key = overflown_node.keys[mid_index]

    # Assign keys to the new node
    new_node.keys = overflown_node.keys[mid_index + 1:]
    overflown_node.keys = overflown_node.keys[:mid_index]

    # Assign children to the new node if not a leaf
    if not overflown_node.leaf:
        new_node.children = overflown_node.children[mid_index + 1:]
        overflown_node.children = overflown_node.children[:mid_index + 1]

        # Update parent_id for the children of the new node
        for child_id in new_node.children:
            # print(f'reading node {child_id} during spliting')
            child_node = read_node(child_id, node_filename)
            child_node.parent_id = new_node.node_id
            save_node(child_node, node_filename)

    # Save the updated nodes
    save_node(overflown_node, node_filename)
    save_node(new_node, node_filename)

    # Step 3: Insert the middle key into the parent node
    if overflown_node.parent_id == -1:
        # Create a new root if the overflown node is the root
        new_root_id = get_free_node(metadata_filename)
        if new_root_id is None:
            file_size = os.path.getsize(node_filename)
            new_root_id = file_size // node_page_size

        new_root = BTreeNode(new_root_id, leaf=False, parent_id=-1, children=[overflown_node.node_id, new_node.node_id])
        new_root.keys = [middle_key]

        # Update parent_id of the split nodes
        overflown_node.parent_id = new_root_id
        new_node.parent_id = new_root_id

        save_node(overflown_node, node_filename)
        save_node(new_node, node_filename)
        save_node(new_root, node_filename)

        root = new_root_id  # Update the global root
    else:
        # Insert the middle key into the parent node
        # print(f'reading node {overflown_node.parent_id} during spliting')

        parent_node = read_node(overflown_node.parent_id, node_filename)
        insert_pos = bisect.bisect_left([k[0] for k in parent_node.keys], middle_key[0])

        parent_node.keys.insert(insert_pos, middle_key)
        print(f'INSERTT POSSS {insert_pos}')
        print(f'{new_node.keys}')
        parent_node.children.remove(overflown_node.node_id)
        parent_node.children.insert(insert_pos, new_node.node_id)
        parent_node.children.insert(insert_pos, overflown_node.node_id)

        save_node(parent_node, node_filename)

        # Handle parent overflow if it occurs
        if len(parent_node.keys) > max_keys:
            split_node(parent_node, node_filename, metadata_filename)


def set_root(new_root_id):
    global root
    root = new_root_id


def init_metadata(metadata_filename="metadata.dat"):
    if not os.path.exists(metadata_filename):
        with open(metadata_filename, "wb") as f:
            # Start with zero free pages
            f.write(struct.pack('i', 0))


def add_underutilized_page(page_num, metadata_filename="metadata.dat"):
    """
    Add a page to the list of underutilized pages.
    """
    pages = load_underutilized_pages(metadata_filename)
    if page_num not in pages:  # Avoid duplicates
        bisect.insort(pages, page_num)
        save_underutilized_pages(pages, metadata_filename)


def remove_underutilized_page(page_num, metadata_filename="metadata.dat"):
    """
    Remove a page from the list of underutilized pages.
    """
    pages = load_underutilized_pages(metadata_filename)
    if page_num in pages:
        pages.remove(page_num)
        save_underutilized_pages(pages, metadata_filename)

def remove_record_from_main_file(page_num, key, main_file="data.dat", metadata_filename="metadata.dat"):
    page = read_page(main_file, page_num, page_size)
    page = Page.unpack(data)

    # Binary search for the record by key
    keys = [r.key for r in page.records]
    pos = bisect.bisect_left(keys, key)
    if pos < len(keys) and page.records[pos].key == key:
        del page.records[pos]

        write_page(main_file, page_num, page, mode="r+b")

        if len(page.records) < max_records_per_page:
            add_underutilized_page(page_num, metadata_filename)


def update_record(key, new_pA, new_pB, new_pAuB, node_filename="btree_nodes.dat", main_file="data.dat"):
    node, found = search_key(key, None, node_filename)
    if found == 'not found':
        return 'Not_Found'

    if node is None:
        return 'Error_Node_Read'

    # Find which key matches
    page_num = None
    for (k, p) in node.keys:
        if k == key:
            page_num = p
            break

    if page_num is None:
        return 'Error_Data_Inconsistent'

    file_size = os.path.getsize(main_file)
    num_pages = file_size // page_size
    if page_num < 0 or page_num >= num_pages:
        return 'Error_Invalid_Page'

    data = read_page(main_file, page_num, page_size)
    page = Page.unpack(data)

    # Binary search for the record
    keys = [r.key for r in page.records]
    pos = bisect.bisect_left(keys, key)
    if pos >= len(page.records) or page.records[pos].key != key:
        return 'Error_Invalid_Slot'

    updated_record = Record(key, new_pA, new_pB, new_pAuB)
    page.records[pos] = updated_record

    write_page(main_file, page_num, page, mode="r+b")

    return 'OK'




def init_node_metadata(metadata_filename="metadata_nodes.dat"):
    """
    Initialize the metadata file for free B-tree nodes if it does not exist.
    """
    if not os.path.exists(metadata_filename):
        with open(metadata_filename, "wb") as f:
            f.write(struct.pack('i', 0))  # Start with zero free nodes.


def load_free_nodes(metadata_filename="metadata_nodes.dat"):
    return load_int_list_from_file(metadata_filename)


def save_free_nodes(free_nodes, metadata_filename="metadata_nodes.dat"):
    save_int_list_to_file(metadata_filename, free_nodes)


def add_free_node(node_id, metadata_filename="metadata_nodes.dat"):
    """
    Add a node ID to the list of free nodes in the metadata file.
    """
    free_nodes = load_free_nodes(metadata_filename)
    bisect.insort(free_nodes, node_id)  # Maintain sorted order.
    save_free_nodes(free_nodes, metadata_filename)


def get_free_node(metadata_filename="metadata_nodes.dat"):
    """
    Retrieve and remove a free node ID from the metadata file, if available.
    """
    free_nodes = load_free_nodes(metadata_filename)
    if not free_nodes:
        return None
    free_node_id = free_nodes.pop(0)  # Get the first free node ID.
    save_free_nodes(free_nodes, metadata_filename)
    return free_node_id


def load_all_keys(node_filename="btree_nodes.dat"):
    """
    Traverse the B-tree and collect all existing keys.

    Parameters:
    - node_filename (str): Path to the B-tree nodes file.

    Returns:
    - keys (set): A set containing all existing keys in the B-tree.
    """
    global node_cache, global_counters, current_files


    nodes_to_flush = [(node_id, node) for node_id, (node, is_dirty) in node_cache.items() if is_dirty]

    for node_id, node in nodes_to_flush:
        save_node(node, current_files['node_file'], mode="r+b")
        node_cache[node_id] = (node, False)
    nodes = load_all_nodes(node_filename)
    keys = set()
    for node in nodes:
        for key_tuple in node["keys"]:
            keys.add(key_tuple[0])
    return keys


def addrandom(num_keys, key_min=1, key_max=10000, main_file="data.dat", node_filename="btree_nodes.dat",
              metadata_filename="metadata.dat", node_metadata_filename="metadata_nodes.dat"):
    """
    Generates and inserts a specified number of random records into the B-tree,
    ensuring that no duplicate keys are inserted.

    Parameters:
    - num_keys (int): Number of random keys to generate and insert.
    - key_min (int): Minimum possible key value (inclusive).
    - key_max (int): Maximum possible key value (inclusive).
    - main_file (str): Path to the main data file.
    - node_filename (str): Path to the B-tree nodes file.
    - metadata_filename (str): Path to the metadata file for main data.
    - node_metadata_filename (str): Path to the metadata file for B-tree nodes.

    Returns:
    - inserted (int): Number of successfully inserted keys.
    - skipped (int): Number of keys skipped due to duplication.
    """
    inserted = 0
    skipped = 0
    attempts = 0
    max_attempts = num_keys * 10  # Prevent infinite loops


    while inserted < num_keys and attempts < max_attempts:

        key = random.randint(key_min, key_max)
        pA = round(random.uniform(0.0, 1.0), 4)
        pB = round(random.uniform(0.0, 1.0), 4)
        pAuB = round(random.uniform(0.0, 1.0), 4)



        print(f'Inserting key {key}...')
        result = insert_key(key, (pA, pB, pAuB), main_file, node_filename, metadata_filename)

        if result == 'ALREADY EXISTS!':
            skipped += 1
            print(f"Key {key} insertion skipped: already exists.")
        elif result == 'OK':
            inserted += 1
            print(f"Key {key} inserted successfully.")
        else:
            print(f"Error inserting key {key}: {result}")

        attempts += 1

    print(f"ADDRANDOM completed: Inserted {inserted} keys, Skipped {skipped} duplicates.")
    return inserted, skipped


def load_main_file(main_file, node_filename="btree_nodes.dat", metadata_filename="metadata.dat",
                   node_metadata_filename="metadata_nodes.dat"):
    """
    Load a new main file and rebuild the B-tree from its contents.
    Identify underutilized pages and mark them.
    """
    global last_page  # Ensure we update the global last_page variable
    if not os.path.exists(main_file):
        print(f"Main file '{main_file}' does not exist. Creating a new file.")
        generate_main_file(main_file)
        add_underutilized_page(0, metadata_filename)
        init_btree_nodes_file(node_filename)
        init_node_metadata(node_metadata_filename)
        init_metadata(metadata_filename)
        last_page = 1  # Start with the first page
        return

    print(f"Loading main file '{main_file}' and rebuilding the B-tree...")

    # Clear existing metadata and B-tree files
    delete_metadata_files(metadata_filename, node_metadata_filename, node_filename)
    init_metadata(metadata_filename)
    init_node_metadata(node_metadata_filename)
    init_btree_nodes_file(node_filename)

    # Read and parse the data from the main file
    file_size = os.path.getsize(main_file)
    num_pages = file_size // page_size

    # Set last_page to the next available page
    last_page = num_pages+1
    print(f"File '{main_file}' has {num_pages} pages. Setting last_page to {last_page}.")

    keys_inserted = 0
    underutilized_pages = []

    with open(main_file, "rb") as f:
        for page_num in range(num_pages):
            f.seek(page_num * page_size)
            data = f.read(page_size)
            page = Page.unpack(data)

            # Check for underutilized pages
            if len(page.records) < max_records_per_page:
                underutilized_pages.append(page_num)

            for slot, record in enumerate(page.records):
                # Insert each record into the B-tree
                result = insert_key(record.key, (record.p_a, record.p_b, record.p_aub), main_file, node_filename,
                                    metadata_filename, loading=True)
                if result == 'OK':
                    keys_inserted += 1

    # Save underutilized pages to metadata
    save_underutilized_pages(underutilized_pages, metadata_filename)
    print(f"Identified {len(underutilized_pages)} underutilized pages.")
    print(f"Rebuilt B-tree with {keys_inserted} keys from the main file '{main_file}'.")
    print(f"Last page set to {last_page}.")


def flush_caches():
    """
    Write all cached nodes and pages to disk and clear caches.
    """
    print("\nFlushing all caches to disk...")
    global global_counters

    # Save dirty nodes from node cache
    for node_id, (node, is_dirty) in list(node_cache.items()):
        if is_dirty:
            print(f"Flushing dirty node {node_id} to disk...")
            save_node(node, current_files['node_file'], mode="r+b")
            node_cache[node_id] = (node, False)
        global_counters["nodes_saved_to_disk"] +=1


    # Save pages from page cache
    for page_num, page_data in list(page_cache.items()):
        print(f"Flushing page {page_num} to disk...")
        with open(current_files['main_file'], "r+b") as f:
            f.seek(page_num * page_size)
            f.write(page_data)
            f.flush()
        global_counters["pages_saved_to_disk"] +=1

        page_cache.pop(page_num)

    print("All caches flushed successfully.\n")


def execute_command(command_line, current_files):
    tokens = command_line.strip().split()
    if not tokens:
        return

    command = tokens[0].upper()

    if command == "CREATE":
        if len(tokens) != 2:
            print("Usage: CREATE <base_name>")
            return
        base_name = tokens[1]

        # Generate file names based on the base name
        new_main_file = f"{base_name}_data.dat"
        new_node_file = f"{base_name}_nodes.dat"
        new_metadata_file = f"{base_name}_metadata.dat"
        new_node_metadata_file = f"{base_name}_nodes_metadata.dat"

        # Perform the creation process
        print(f"Creating a new B-tree with base name '{base_name}'...")

        # Delete existing metadata and node files if they exist
        delete_metadata_files(new_metadata_file, new_node_metadata_file, new_node_file, new_main_file)

        # Initialize necessary files
        generate_main_file(new_main_file, 0)  # Start with zero records
        add_underutilized_page(0, new_metadata_file)
        init_btree_nodes_file(new_node_file)
        init_node_metadata(new_node_metadata_file)
        init_metadata(new_metadata_file)

        # Reset global counters
        for key in global_counters:
            global_counters[key] = 0

        # Update the current_files dictionary
        current_files['main_file'] = new_main_file
        current_files['node_file'] = new_node_file
        current_files['metadata_file'] = new_metadata_file
        current_files['node_metadata_file'] = new_node_metadata_file

        print("New B-tree created successfully.")
        return
    elif command == "EXIT":
        print("\nExiting program. Flushing caches and saving data...")
        flush_caches()
        print("Exiting cleanly. Goodbye!")
        sys.exit(0)

    elif command == "FLUSH":
        flush_caches()
        print("FLUSH operation completed.")


    elif command == "LOAD":

        if len(tokens) != 2:
            print("Usage: LOAD <base_name>")

            return

        base_name = tokens[1]

        loaded_main_file = f"{base_name}_data.dat"

        loaded_node_file = f"{base_name}_nodes.dat"

        loaded_metadata_file = f"{base_name}_metadata.dat"

        loaded_node_metadata_file = f"{base_name}_nodes_metadata.dat"

        # Check if all necessary files exist

        if not all(os.path.exists(f) for f in
                   [loaded_main_file, loaded_node_file, loaded_metadata_file, loaded_node_metadata_file]):
            print(f"Error: One or more files for base name '{base_name}' do not exist.")

            return

        # Update the current_files dictionary

        current_files['main_file'] = loaded_main_file

        current_files['node_file'] = loaded_node_file

        current_files['metadata_file'] = loaded_metadata_file

        current_files['node_metadata_file'] = loaded_node_metadata_file

        # Load the B-tree from the main file

        load_main_file(loaded_main_file, loaded_node_file, loaded_metadata_file, loaded_node_metadata_file)

        print(f"B-tree loaded successfully from base name '{base_name}'.")

        return


    elif command == "INSERT":
        if len(tokens) != 5:
            print("Usage: INSERT <key> <pA> <pB> <pAuB>")
            return
        try:
            key = int(tokens[1])
            pA = float(tokens[2])
            pB = float(tokens[3])
            pAuB = float(tokens[4])
        except ValueError:
            print("Invalid arguments. <key> must be an integer and <pA>, <pB>, <pAuB> must be floats.")
            return
        result = insert_key(key, (pA, pB, pAuB), current_files['main_file'], current_files['node_file'], current_files['metadata_file'])
        print(result)

    elif command == "DELETE":
        if len(tokens) != 2:
            print("Usage: DELETE <key>")
            return
        try:
            key = int(tokens[1])
        except ValueError:


            print("Invalid key. <key> must be an integer.")
            return
        result = delete_key(key, current_files['node_file'], current_files['main_file'])
        print(result)

    elif command == "UPDATE":
        if len(tokens) != 5:
            print("Usage: UPDATE <key> <new_pA> <new_pB> <new_pAuB>")
            return
        try:
            key = int(tokens[1])
            new_pA = float(tokens[2])
            new_pB = float(tokens[3])
            new_pAuB = float(tokens[4])
        except ValueError:
            print("Invalid arguments. <key> must be an integer and <new_pA>, <new_pB>, <new_pAuB> must be floats.")
            return
        result = update_record(key, new_pA, new_pB, new_pAuB, current_files['node_file'], current_files['main_file'])
        print(result)

    elif command == "SEARCH":
        if len(tokens) != 2:
            print("Usage: SEARCH <key>")
            return
        try:
            key = int(tokens[1])
        except ValueError:
            print("Invalid key. <key> must be an integer.")
            return
        node, found = search_key(key, None, current_files['node_file'])
        if found == 'found':
            print(f"Key {key} found in node {node.node_id}.")
        else:
            print(f"Key {key} not found.")

    elif command == "PRINT":
        print_main_file(current_files['main_file'])

    elif command == "VISUALIZE":
        nodes = load_all_nodes(current_files['node_file'])
        generate_dot(nodes, "tree.dot")
        visualize_tree("tree.dot", "tree.png")
        print("B-tree visualized as 'tree.png'.")

    elif command == "ADDRANDOM":
        if len(tokens) != 2:
            print("Usage: ADDRANDOM <number_of_keys>")
            return
        try:
            num_keys = int(tokens[1])
            if num_keys <= 0:
                print("Number of keys must be a positive integer.")
                return
        except ValueError:
            print("Invalid number of keys. Please provide an integer.")
            return
        inserted, skipped = addrandom(num_keys, main_file=current_files['main_file'], node_filename=current_files['node_file'],
                                      metadata_filename=current_files['metadata_file'],
                                      node_metadata_filename=current_files['node_metadata_file'])
        print(f"Inserted {inserted} keys, Skipped {skipped} duplicates.")

    elif command == "HELP":
        print("""
Available Commands:
  CREATE <base_name>
      Initialize a new B-tree with the specified base name.
      This will generate the following files:
        - <base_name>_data.dat
        - <base_name>_nodes.dat
        - <base_name>_metadata.dat
        - <base_name>_nodes_metadata.dat
      Existing files with these names will be overwritten.
  LOAD <main_file>
      Load an existing B-tree from the specified main data file.
  INSERT <key> <pA> <pB> <pAuB>
      Insert a new record with the specified key and probabilities.
  DELETE <key>
      Delete the record with the specified key.
  UPDATE <key> <new_pA> <new_pB> <new_pAuB>
      Update the record with the specified key.
  SEARCH <key>
      Search for the record with the specified key.
  PRINT
      Display all records in the main file.
  VISUALIZE
      Generate and display a visual representation of the B-tree.
  ADDRANDOM <number_of_keys>
      Generate and insert a specified number of random records.
  EXIT
      Exit the program.
""")

    elif command == "EXIT":
        sys.exit(0)

    else:
        print(f"Unknown command: {command}. Type 'HELP' for a list of commands.")



def delete_key(x, node_filename="btree_nodes.dat", main_file="data.dat"):
    node, found = search_key(x, None, node_filename)
    if found == 'not found':
        return 'Not_Found'

    if node is None:
        raise ValueError(f"Node {node.node_id} could not be read.")

    # Find key in node
    record_index = None
    page_num = None
    for i, (k, p) in enumerate(node.keys):
        if k == x:
            record_index = i
            page_num = p
            break

    if record_index is None:
        raise ValueError("Key not found in node after searching. Data inconsistency!")

    if node.leaf:
        del node.keys[record_index]
        remove_record_from_main_file(page_num, x, main_file)
        if len(node.keys) == 0:
            if node.parent_id != -1:
                parent = read_node(node.parent_id, node_filename)
                parent.children.remove(node.node_id)
                save_node(parent, node_filename)
                add_free_node(node.node_id)
            else:
                global root
                root = None if len(node.children) == 0 else node.children[0]
        elif len(node.keys) < min_keys:
            handle_underflow(node, node_filename)
        save_node(node, node_filename)
        return 'OK'

    if not node.leaf:
        left_child_id = node.children[record_index]
        right_child_id = node.children[record_index + 1]

        if left_child_id is not None:
            predecessor = get_largest_key(read_node(left_child_id, node_filename), node_filename)
            node.keys[record_index] = predecessor
            save_node(node, node_filename)
            return delete_key(predecessor[0], node_filename, main_file)
        elif right_child_id is not None:
            successor = get_smallest_key(read_node(right_child_id, node_filename), node_filename)
            node.keys[record_index] = successor
            save_node(node, node_filename)
            return delete_key(successor[0], node_filename, main_file)
        handle_underflow(node, node_filename)

    return 'OK'


def handle_underflow(node, node_filename):
    """
    Handles underflow in a B-tree node through compensation or merging.

    Parameters:
    - node (BTreeNode): The node experiencing underflow.
    - node_filename (str): Path to the B-tree nodes file.

    Returns:
    - None
    """
    if node.parent_id != -1:
        # print(f'reading parent node {node.parent_id} when handling underflow.')
        parent = read_node(node.parent_id, node_filename)
    else:
        return

    idx = parent.children.index(node.node_id)

    # Check left and right siblings
    left_sibling_id = parent.children[idx - 1] if idx > 0 else None
    right_sibling_id = parent.children[idx + 1] if idx < len(parent.children) - 1 else None
    # print(f'reading syblings {left_sibling_id}  and {right_sibling_id} when handling underflow')
    left_sibling = read_node(left_sibling_id, node_filename) if left_sibling_id is not None else None
    right_sibling = read_node(right_sibling_id, node_filename) if right_sibling_id is not None else None

    # Try compensation with left sibling
    if left_sibling and len(left_sibling.keys) > min_keys:
        transfer_key_from_left(node, left_sibling, parent, idx - 1, node_filename)
        return

    # Try compensation with right sibling
    if right_sibling and len(right_sibling.keys) > min_keys:
        transfer_key_from_right(node, right_sibling, parent, idx, node_filename)
        return

    # If compensation is not possible, merge with a sibling
    if left_sibling:
        merge_nodes(left_sibling, node, parent, idx - 1, node_filename)
    elif right_sibling:
        merge_nodes(node, right_sibling, parent, idx, node_filename)


def transfer_key_from_left(node, left_sibling, parent, parent_key_idx, node_filename):
    # Transfer key from parent to node
    node.keys.insert(0, parent.keys[parent_key_idx])
    parent.keys[parent_key_idx] = left_sibling.keys.pop()

    # Transfer child from left sibling to node (if not a leaf)
    if not node.leaf:
        # print(f'reading left sibling node {left_sibling.node_id} during transfer.')
        child = read_node(left_sibling.children[-1], node_filename)
        child.parent_id = node.node_id
        save_node(child, node_filename)
        node.children.insert(0, left_sibling.children.pop())

    save_node(node, node_filename)
    save_node(left_sibling, node_filename)
    save_node(parent, node_filename)


def transfer_key_from_right(node, right_sibling, parent, parent_key_idx, node_filename):
    # Transfer key from parent to node
    node.keys.append(parent.keys[parent_key_idx])
    parent.keys[parent_key_idx] = right_sibling.keys.pop(0)

    # Transfer child from right sibling to node (if not a leaf)
    if not node.leaf:
        # print(f'reading right sibling node {right_sibling.node_id} during transfer.')
        child = read_node(right_sibling.children[0], node_filename)
        child.parent_id = node.node_id
        save_node(child, node_filename)
        print(node.node_id)
        print(child.node_id)
        print(child.parent_id)

        print(f'{right_sibling.children}')

        node.children.append(right_sibling.children.pop(0))
        print(f'{right_sibling.node_id}')

        print(f'{right_sibling.children}')

    save_node(node, node_filename)
    save_node(right_sibling, node_filename)
    save_node(parent, node_filename)


def merge_nodes(left_node, right_node, parent, parent_key_idx, node_filename):
    """
    Merges two sibling nodes into one and adjusts the parent.
    """
    merging_key = parent.keys.pop(parent_key_idx)
    left_node.keys.append(merging_key)
    left_node.keys.extend(right_node.keys)
    left_node.children.extend(right_node.children)

    if not right_node.leaf:
        for child_id in right_node.children:
            child_node = read_node(child_id, node_filename)
            child_node.parent_id = left_node.node_id
            save_node(child_node, node_filename)

    print(f'{left_node.keys}')
    print(f'{parent.keys}')

    parent.children.remove(right_node.node_id)
    right_node.keys.clear()
    right_node.children.clear()
    save_node(left_node, node_filename)
    save_node(right_node, node_filename)

    save_node(parent, node_filename)

    if len(parent.keys) < min_keys:
        handle_underflow(parent, node_filename)
    if len(parent.keys) == 0 and parent.parent_id == -1:
        left_node.parent_id = parent.parent_id
        save_node(left_node, node_filename)
        parent.children.clear()
        save_node(parent, node_filename)
        save_node(left_node, node_filename)
        global root
        root = left_node.node_id
    # Mark the right node as free
    add_free_node(right_node.node_id)


def get_largest_key(node, node_filename):
    while not node.leaf:
        node = read_node(node.children[-1], node_filename)
    return node.keys[-1]


def get_smallest_key(node, node_filename):
    while not node.leaf:
        node = read_node(node.children[0], node_filename)
    return node.keys[0]


def delete_metadata_files(*filenames):
    for filename in filenames:
        if os.path.exists(filename):
            try:
                os.remove(filename)
                print(f"Deleted existing file: {filename}")
            except OSError as e:
                print(f"Error deleting file {filename}: {e}")



def print_global_counters():
    print("Global Operation Counters:")
    for key, value in global_counters.items():
        print(f"{key.replace('_', ' ').capitalize()}: {value}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B-tree Management Program with Main File Switching")
    parser.add_argument('-t', '--testfile', type=str, help='Path to the test file containing commands')
    args = parser.parse_args()

    # Initialize current active files
    current_files = {
        'main_file': None,
        'node_file': None,
        'metadata_file': None,
        'node_metadata_file': None
    }

    if not args.testfile:
        # Interactive Mode Initialization
        print("Welcome to the B-tree Management Program!")
        print("Please select an initialization option:")
        print("1. Load Default B-tree Files")
        print("2. Overwrite Default Files by Creating a New B-tree")
        print("3. Create a New B-tree with a Custom Base Name")
        print("4. Load an Existing B-tree with a Custom Base Name")
        choice = input("Enter your choice (1-4): ").strip()

        if choice == "1":
            # Define default base name
            default_base = "default"
            default_main_file = f"{default_base}_data.dat"
            default_node_file = f"{default_base}_nodes.dat"
            default_metadata_file = f"{default_base}_metadata.dat"
            default_node_metadata_file = f"{default_base}_nodes_metadata.dat"

            # Check if default files exist
            if not all(os.path.exists(f) for f in
                       [default_main_file, default_node_file, default_metadata_file, default_node_metadata_file]):
                print("Default files do not exist. Creating a new B-tree with default files.")
                # Initialize default B-tree
                generate_main_file(default_main_file, 0)
                add_underutilized_page(0, default_metadata_file)
                init_btree_nodes_file(default_node_file)
                init_node_metadata(default_node_metadata_file)
                init_metadata(default_metadata_file)
                print("Default B-tree created successfully.")
            else:
                print("Loading existing default B-tree files.")

            # Set current_files to default
            current_files['main_file'] = default_main_file
            current_files['node_file'] = default_node_file
            current_files['metadata_file'] = default_metadata_file
            current_files['node_metadata_file'] = default_node_metadata_file

            # Load the B-tree
            load_main_file(default_main_file, default_node_file, default_metadata_file, default_node_metadata_file)
            print("Default B-tree loaded successfully.")

        elif choice == "2":
            # Overwrite Default Files by Creating a New B-tree with default base name
            default_base = "default"
            default_main_file = f"{default_base}_data.dat"
            default_node_file = f"{default_base}_nodes.dat"
            default_metadata_file = f"{default_base}_metadata.dat"
            default_node_metadata_file = f"{default_base}_nodes_metadata.dat"

            print("Overwriting default files by creating a new B-tree with base name 'default'...")
            # Create a new B-tree with default base name
            create_new_btree = f"CREATE {default_base}"
            execute_command(create_new_btree, current_files)

        elif choice == "3":
            # Create a New B-tree with a Custom Base Name
            base_name = input("Enter a base name for the new B-tree: ").strip()
            if not base_name:
                print("Invalid base name. Exiting.")
                sys.exit(1)
            create_command = f"CREATE {base_name}"
            execute_command(create_command, current_files)

        elif choice == "4":
            # Load an Existing B-tree with a Custom Base Name
            base_name = input("Enter the base name of the B-tree to load: ").strip()
            if not base_name:
                print("Invalid base name. Exiting.")
                sys.exit(1)
            load_command = f"LOAD {base_name}"
            execute_command(load_command, current_files)
        else:
            print("Invalid choice. Exiting.")
            sys.exit(1)

    # Proceed with either batch mode or interactive mode
    if args.testfile:
        if not os.path.exists(args.testfile):
            print(f"Test file '{args.testfile}' does not exist.")
            sys.exit(1)

        with open(args.testfile, 'r') as tf:
            for line_number, line in enumerate(tf, start=1):
                stripped_line = line.strip()
                if not stripped_line or stripped_line.startswith('#'):
                    continue
                print(f">>> {stripped_line}")
                execute_command(stripped_line, current_files)
                print_global_counters()

        print("Batch processing completed.")
    else:
        print("Entering interactive mode. Type 'HELP' for a list of commands or 'EXIT' to quit.")
        while True:
            signal.signal(signal.SIGINT, handle_exit_signal)
            signal.signal(signal.SIGTERM, handle_exit_signal)
            try:
                command_line = input("B-tree> ")
                execute_command(command_line, current_files)
                print_global_counters()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting interactive mode.")
                break
