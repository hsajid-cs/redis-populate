import os
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
from typing import Iterable, List

try:
    import redis
except Exception as e:  # pragma: no cover - redis is required at runtime
    raise

load_dotenv()


def connect_redis():
    host = os.getenv("REDIS_HOST")
    port = int(os.getenv("REDIS_PORT", 6379))
    username = os.getenv("REDIS_USERNAME")
    password = os.getenv("REDIS_PASSWORD")

    return redis.Redis(host=host, port=port, username=username, password=password, decode_responses=True)


def chunked(iterable: Iterable, size: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def stream_array_with_ijson(path: Path, key: str):
    # ijson yields items from a JSON stream without loading whole file
    try:
        import ijson
    except Exception:
        raise RuntimeError("ijson not available")

    with path.open('rb') as f:
        prefix = f'{key}.item'
        for item in ijson.items(f, prefix):
            yield item


def load_list_from_json(path: Path, key: str):
    # Fallback: load whole file and return list at key (may use lots of memory)
    with path.open('r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, dict) and key in data and isinstance(data[key], list):
        return data[key]
    # If top level is list and key is not present, return empty
    return []


def push_institutions(r, institutes_iter: Iterable[str], chunk_size: int = 1000):
    total = 0
    r.delete('institutions')
    pipe = r.pipeline()
    for batch in chunked(institutes_iter, chunk_size):
        for inst in batch:
            pipe.rpush('institutions', inst)
        pipe.execute()
        total += len(batch)
    return total


def push_companies_deduped(r, companies_iter: Iterable[str], chunk_size: int = 1000):
    # We'll maintain a Redis SET 'companies:set' to dedupe while preserving insertion order
    # Clear keys first
    r.delete('companies')
    r.delete('companies:set')

    total_added = 0

    for batch in chunked(companies_iter, chunk_size):
        pipe = r.pipeline()
        # Issue SADD commands first
        for c in batch:
            pipe.sadd('companies:set', c)
        sadd_results = pipe.execute()

        # For entries that were newly added (sadd_results == 1), push to list
        to_push = [c for c, res in zip(batch, sadd_results) if res]
        if to_push:
            pipe = r.pipeline()
            for c in to_push:
                pipe.rpush('companies', c)
            pipe.execute()
            total_added += len(to_push)

    return total_added


def iterate_companies_and_institutions(path: Path, use_ijson: bool):
    # Yields two generators: institutions_iter, companies_iter and a boolean
    # indicating whether ijson streaming was actually used.
    if use_ijson:
        # Check ijson is importable up-front. stream_array_with_ijson is a generator
        # function so its body (and import) doesn't run until iteration — that
        # caused a RuntimeError to be raised later during iteration. Importing
        # here forces an immediate failure and lets us fall back safely.
        try:
            import ijson  # type: ignore
        except Exception:
            use_ijson = False
        else:
            institutions = stream_array_with_ijson(path, 'institution')
            companies = stream_array_with_ijson(path, 'companies')
            return institutions, companies, True

    # Fallback load entire lists
    data = None
    with path.open('r', encoding='utf-8') as f:
        data = json.load(f)

    institutions = data.get('institution', []) if isinstance(data, dict) else []
    companies = data.get('companies', []) if isinstance(data, dict) else []
    return iter(institutions), iter(companies), False


def main():
    parser = argparse.ArgumentParser(description='Efficiently push institutions and companies into Redis')
    parser.add_argument('-i', '--input', default='institutes.json', help='Input JSON file (default: institutes.json)')
    parser.add_argument('--chunk', type=int, default=1000, help='Batch size for Redis pipeline (default: 1000)')
    parser.add_argument('--no-ijson', action='store_true', help='Disable ijson streaming even if installed')
    args = parser.parse_args()

    path = Path(args.input)
    if not path.is_file():
        print(f'Input file not found: {path}')
        raise SystemExit(2)

    r = connect_redis()

    use_ijson = (not args.no_ijson)
    try:
        institutes_iter, companies_iter, used_ijson = iterate_companies_and_institutions(path, use_ijson)
    except Exception as e:
        print(f'Error reading input JSON: {e}')
        raise SystemExit(3)

    # Push institutions
    print('Pushing institutions...')
    inst_count = push_institutions(r, institutes_iter, chunk_size=args.chunk)

    # For companies we want existing companies first, then append institutions deduped.
    # We'll first push companies from the file, then also push institutions into companies only if new.
    print('Pushing companies from input (deduplicating using Redis SET)...')
    companies_added = push_companies_deduped(r, companies_iter, chunk_size=args.chunk)

    # Now also append institutions into companies if they are new
    # Re-open institutions iterator if possible
    # Simplest approach: stream institutions again (using ijson) or reload
    try:
        if used_ijson:
            inst_again = stream_array_with_ijson(path, 'institution')
        else:
            # reload file and get institutions list
            with path.open('r', encoding='utf-8') as f:
                data = json.load(f)
            inst_again = iter(data.get('institution', []))
    except Exception:
        inst_again = []

    print('Appending institutions into companies (deduped)...')
    companies_added_from_insts = push_companies_deduped(r, inst_again, chunk_size=args.chunk)

    print(f'Inserted {inst_count} institutions into Redis list "institutions"')
    total_companies = r.llen('companies')
    print(f'Inserted {total_companies} unique companies into Redis list "companies" (newly added from file: {companies_added}, from institutions: {companies_added_from_insts})')


if __name__ == '__main__':
    main()
