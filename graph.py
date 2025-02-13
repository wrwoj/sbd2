import re

def extract_function_calls(filename):
    with open(filename, 'r') as file:
        code = file.read()

    # Regex to capture function definitions
    function_def_pattern = r"def (\w+)\(.*?\):"
    functions = re.findall(function_def_pattern, code)

    # Initialize function_calls dictionary with a special entry for __main__
    function_calls = {function: set() for function in functions}
    function_calls["__main__"] = set()

    # Regex to capture function calls
    function_call_pattern = r"(\w+)\("

    current_function = None

    for line in code.splitlines():
        stripped_line = line.strip()

        if stripped_line.startswith("def "):
            match = re.search(function_def_pattern, stripped_line)
            if match:
                current_function = match.group(1)

        elif stripped_line.startswith("if __name__ == \"__main__\":"):
            current_function = "__main__"

        elif current_function:
            calls_in_line = re.findall(function_call_pattern, stripped_line)
            for call in calls_in_line:
                if call in function_calls:  # Only consider internal function calls
                    function_calls[current_function].add(call)

    return function_calls

def generate_dot_from_calls(function_calls, dot_filename="call_graph.dot"):
    with open(dot_filename, "w") as f:
        f.write("digraph CallGraph {\n")
        for function, calls in function_calls.items():
            for call in calls:
                f.write(f'  "{function}" -> "{call}";\n')
        f.write("}\n")

# Main process
if __name__ == "__main__":
    filename = "main.py"  # Replace with your filename
    function_calls = extract_function_calls(filename)

    # Generate DOT file
    dot_file = "call_graph.dot"
    generate_dot_from_calls(function_calls, dot_file)

    # Generate PNG file using Graphviz
    import subprocess
    output_png = "call_graph.png"
    subprocess.run(["dot", "-Tpng", dot_file, "-o", output_png], check=True)
    print(f"Call graph generated: {output_png}")
