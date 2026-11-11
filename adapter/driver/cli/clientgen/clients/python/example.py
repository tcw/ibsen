"""Writes two entries to the topic "example" on an Ibsen server at localhost:54321 (or the
address given as the first argument), reads them back, and lists the topics."""

import sys

from @@PACKAGE@@ import IbsenClient


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:54321"
    with IbsenClient(target, producer=True) as client:
        result = client.write("example", [b"hello", b"world"])
        print(f"wrote {result.count} entries at offset {result.first_offset}")
        for entry in client.read("example", result.first_offset):
            print(f"{entry.offset}\t{entry.content.decode()}")
        print(f"topics: {client.list()}")


if __name__ == "__main__":
    main()
