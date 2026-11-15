// Ported from pi v1.5.2 packages/durable/src/ids.ts (MIT, Earendil Works): not exported by the package.
import type { Id, Seq } from "@earendil-works/pi-durable";

/** Apply an erased ID brand at a trusted numeric allocation or decoding boundary. */
export function idFromNumber<I extends Id<string>>(value: number): I {
	return value as I;
}

/** Apply the erased commit-sequence brand at a trusted storage boundary. */
export function seqFromNumber(value: number): Seq {
	return value as Seq;
}
