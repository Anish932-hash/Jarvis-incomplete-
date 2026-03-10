import os
from pathlib import Path
import fnmatch

class SearchTools:
    """High-performance file + content search with pattern matching."""

    @staticmethod
    def search_files(base_dir: str, pattern: str):
        base = Path(base_dir)
        if not base.exists():
            raise FileNotFoundError(base_dir)

        results = []
        for root, dirs, files in os.walk(base):
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    results.append(str(Path(root) / name))
        return results

    @staticmethod
    def search_text_in_files(base_dir: str, keyword: str, extensions=None):
        if extensions is None:
            extensions = [".txt", ".md", ".py", ".json"]

        matches = []
        for path in Path(base_dir).rglob("*"):
            if path.suffix.lower() in extensions:
                try:
                    with open(path, "r", errors="ignore") as f:
                        if keyword.lower() in f.read().lower():
                            matches.append(str(path))
                except:
                    continue
        return matches

    @staticmethod
    def fuzzy_search(files: list, text: str):
        """Simple scoring-based fuzzy match."""
        results = []
        text = text.lower()

        for f in files:
            f_low = f.lower()
            score = 0
            for c in text:
                if c in f_low:
                    score += 1
            if score > 0:
                results.append((f, score))

        return sorted(results, key=lambda x: x[1], reverse=True)