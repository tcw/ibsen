// Writes two entries to the topic "example" on an Ibsen server at localhost:54321 (or the
// address given as the first argument), reads them back, and lists the topics.
import { IbsenClient } from "./client";

async function main(): Promise<void> {
  const client = new IbsenClient(process.argv[2] ?? "localhost:54321", { producer: true });
  try {
    const result = await client.write("example", [Buffer.from("hello"), Buffer.from("world")]);
    console.log(`wrote ${result.count} entries at offset ${result.firstOffset}`);
    for (const entry of await client.read("example", result.firstOffset)) {
      console.log(`${entry.offset}\t${entry.content.toString()}`);
    }
    console.log(`topics: ${(await client.list()).join(",")}`);
  } finally {
    client.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
