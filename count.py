import json
import argparse
from pathlib import Path


def count_objects_in_file(path: str) -> int:
    """Return the number of top-level JSON objects in the file.

    - If the JSON is a list, returns len(list).
    - If the JSON is a dict/object, returns 1.
    - Otherwise returns 0.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return 0

    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description='Count top-level JSON objects in a single JSON file')
    parser.add_argument('file', help='Path to the JSON file to count')
    args = parser.parse_args()

    path = Path(args.file)
    if not path.is_file():
        print(f"File not found: {path}")
        return

    count = count_objects_in_file(str(path))
    # Print only the number of objects (user requested no extra text)
    print(count)


if __name__ == '__main__':
    main()
