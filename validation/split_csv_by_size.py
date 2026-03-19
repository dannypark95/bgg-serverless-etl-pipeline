import os
import sys
import csv
from typing import TextIO


MAX_BYTES = 256 * 1024  # 256 KB


def open_new_part(base_path: str, part_idx: int, header: list[str]) -> tuple[TextIO, csv.writer, str]:
    base_dir, base_name = os.path.split(base_path)
    name, ext = os.path.splitext(base_name)
    out_name = f"{name}_part{part_idx:03d}{ext}"
    out_path = os.path.join(base_dir, out_name)
    f = open(out_path, "w", newline="", encoding="utf-8")
    writer = csv.writer(f)
    writer.writerow(header)
    f.flush()
    return f, writer, out_path


def split_csv(input_path: str, max_bytes: int = MAX_BYTES) -> None:
    if not os.path.exists(input_path):
        raise FileNotFoundError(input_path)

    print(f"Splitting {input_path} into <= {max_bytes} bytes parts...")

    with open(input_path, newline="", encoding="utf-8") as src:
        reader = csv.reader(src)
        header = next(reader, None)
        if header is None:
            print("Input CSV is empty, nothing to do.")
            return

        part_idx = 1
        out_f, out_writer, out_path = open_new_part(input_path, part_idx, header)
        current_size = os.path.getsize(out_path)

        try:
            for row in reader:
                # Write row to a temp buffer to know its size impact.
                out_writer.writerow(row)
                out_f.flush()
                new_size = os.path.getsize(out_path)

                if new_size > max_bytes:
                    # Remove the last row from this part: reopen next part and re-write row there.
                    out_f.close()
                    part_idx += 1
                    out_f, out_writer, out_path = open_new_part(input_path, part_idx, header)
                    out_writer.writerow(row)
                    out_f.flush()
                    current_size = os.path.getsize(out_path)
                else:
                    current_size = new_size
        finally:
            out_f.close()

    print(f"Done. Wrote {part_idx} part file(s).")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("Usage: python -m validation.split_csv_by_size <path_to_csv>")
        return 1

    input_path = argv[1]
    split_csv(input_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

