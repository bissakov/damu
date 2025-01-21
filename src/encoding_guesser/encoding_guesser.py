import json
from pathlib import Path


def guess_encoding(value: str) -> None:
    project_folder = Path(__file__).parent.parent
    encodings_json = project_folder / "src" / "encodings.json"
    with open(encodings_json, "r", encoding="utf-8") as f:
        encodings = json.load(f)

    for encoding1 in encodings[: len(encodings) // 2]:
        for encoding2 in encodings[(len(encodings) // 2) :]:
            if encoding1 == encoding2:
                continue

            try:
                normalized_value = value.encode(encoding1).decode(encoding2)
                print(encoding1, encoding2, normalized_value)
            except Exception:
                continue
