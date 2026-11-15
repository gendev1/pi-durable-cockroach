// Ported from pi v1.5.2 packages/durable/src/storage/sqlite/statements.ts (MIT, Earendil Works). Rewritten: upstream
// passes rows as one JSON array parameter read with SQLite's `json_each(?)`/`json_extract`, which CockroachDB does
// not have. Here an ID list is one array parameter (`= ANY(?)`), and rows are inserted in multi-row `VALUES` with
// bound cells, in chunks under PostgreSQL's bind limit. The functions keep their upstream names and roles, so
// `commit.ts` calls them unchanged.
import type { Executor, SqlValue } from "./database.ts";
import type { Cell } from "./rows.ts";

/** Runs `sql` once with `values` as its one array parameter; answers its rows. Nothing to read: no statement. */
export async function readRows<T extends object>(
    executor: Executor,
    sql: string,
    values: readonly Cell[],
): Promise<T[]> {
    if (values.length === 0) return [];
    return executor.all<T>(sql, values);
}

/** Runs `sql` (a deletion) once with `values` as its one array parameter. */
export async function runForEach(executor: Executor, sql: string, values: readonly Cell[]): Promise<void> {
    if (values.length === 0) return;
    await executor.run(sql, values);
}

// PostgreSQL's wire protocol binds at most 65535 parameters per statement; stay well under it.
const MAX_PARAMETERS = 8_000;

/** Inserts `rows` (one cell per column, in order) into `table`, then `conflict` (an upsert's clause). */
export async function insertRows(
    executor: Executor,
    table: string,
    columns: readonly string[],
    rows: readonly (readonly Cell[])[],
    conflict = "",
): Promise<void> {
    if (rows.length === 0) return;
    const width = columns.length;
    const size = Math.max(1, Math.floor(MAX_PARAMETERS / width));
    const row = `(${new Array(width).fill("?").join(", ")})`;
    for (let start = 0; start < rows.length; start += size) {
        const chunk = rows.slice(start, start + size);
        await executor.run(
            `INSERT INTO ${table} (${columns.join(", ")}) VALUES ${new Array(chunk.length).fill(row).join(", ")} ${conflict}`,
            ...(chunk.flat() as SqlValue[]),
        );
    }
}
