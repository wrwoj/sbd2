import ast
import tokenize
import sys
import io
from io import StringIO

def get_docstring_positions(source):
    """
    Parses the source code using AST and collects the positions (start and end line numbers)
    of all docstrings in modules, classes, and functions.

    Args:
        source (str): The original Python source code.

    Returns:
        set: A set of line numbers that are part of docstrings.
    """
    docstring_lines = set()

    class DocstringVisitor(ast.NodeVisitor):
        def visit_Module(self, node):
            self._collect_docstring(node)
            self.generic_visit(node)

        def visit_FunctionDef(self, node):
            self._collect_docstring(node)
            self.generic_visit(node)

        def visit_ClassDef(self, node):
            self._collect_docstring(node)
            self.generic_visit(node)

        def _collect_docstring(self, node):
            docstring = ast.get_docstring(node)
            if docstring:
                # The docstring is the first statement in the body
                first_stmt = node.body[0]
                if isinstance(first_stmt, ast.Expr) and isinstance(first_stmt.value, ast.Str):
                    # Get start and end line numbers
                    start_lineno = first_stmt.lineno
                    # For Python 3.8+, end_lineno is available
                    if hasattr(first_stmt, 'end_lineno'):
                        end_lineno = first_stmt.end_lineno
                    else:
                        # Fallback: estimate based on the number of lines in the docstring
                        end_lineno = start_lineno + docstring.count('\n')
                    for lineno in range(start_lineno, end_lineno + 1):
                        docstring_lines.add(lineno)

    try:
        parsed_ast = ast.parse(source)
    except SyntaxError as e:
        print(f"SyntaxError while parsing the source code: {e}")
        sys.exit(1)

    DocstringVisitor().visit(parsed_ast)
    return docstring_lines

def remove_comments_and_docstrings(source_code):
    """
    Removes all single-line comments and unassigned docstrings from the provided Python source code.
    Preserves all string literals that are part of the code (e.g., dictionary keys, string variables).

    Args:
        source_code (str): The original Python source code.

    Returns:
        str: The source code with comments and unassigned docstrings removed.
    """
    # First, identify all docstring line numbers
    docstring_lines = get_docstring_positions(source_code)

    # Prepare to tokenize the source code
    output_tokens = []
    try:
        tokens = tokenize.generate_tokens(StringIO(source_code).readline)
    except tokenize.TokenError as e:
        print(f"Tokenization error: {e}")
        sys.exit(1)

    for tok in tokens:
        token_type = tok.type
        token_string = tok.string
        start_line, start_col = tok.start
        end_line, end_col = tok.end

        if token_type == tokenize.COMMENT:
            # Skip comments
            continue
        elif token_type == tokenize.STRING:
            # Check if the string token is part of a docstring
            if any(lineno in docstring_lines for lineno in range(start_line, end_line + 1)):
                # Skip docstrings
                continue
        # For all other tokens, include them
        output_tokens.append(tok)

    try:
        cleaned_code = tokenize.untokenize(output_tokens)
    except Exception as e:
        print(f"Error during untokenize: {e}")
        sys.exit(1)

    return cleaned_code

def main():
    if len(sys.argv) != 3:
        print("Usage: python remove_comments_and_docstrings.py <input_file.py> <output_file.py>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            source_code = f.read()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    except IOError as e:
        print(f"IOError while reading '{input_file}': {e}")
        sys.exit(1)

    cleaned_code = remove_comments_and_docstrings(source_code)

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(cleaned_code)
    except IOError as e:
        print(f"IOError while writing to '{output_file}': {e}")
        sys.exit(1)

    print(f"Comments and unassigned docstrings removed. Cleaned code saved to '{output_file}'.")

if __name__ == "__main__":
    main()
