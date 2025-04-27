import json
from pathlib import Path

from src.utils.utils import safe_extract


def guess_encoding(value: str) -> None:
    project_folder = Path(__file__).parent.parent
    encodings_json = project_folder / "encoding_guesser" / "encodings.json"
    with open(encodings_json, "r", encoding="utf-8") as f:
        encodings = json.load(f)

    for encoding1 in encodings[: len(encodings) // 2]:
        for encoding2 in encodings[(len(encodings) // 2) :]:
            if encoding1 == encoding2:
                continue

            try:
                normalized_value = value.encode(encoding1).decode(encoding2)
                print(f"{encoding1=}, {encoding2=}, {normalized_value=}")
            except Exception:
                continue


def main():
    folder = Path(r"E:\damu\downloads\2025-02-26\f284894a-af67-44cb-86f7-67c806260176")
    safe_extract(
        archive_path=folder / "contract.zip", documents_folder=folder / "documents"
    )


if __name__ == "__main__":
    main()
