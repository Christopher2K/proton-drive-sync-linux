/**
 * Type declarations for the Uint8Array base64/hex methods (ES2026 proposal),
 * installed at runtime by src/polyfill.ts (core-js). TS 5.9's lib does not
 * type them yet; the augmentation is global so the vendored
 * @protontech/crypto TypeScript sources also typecheck.
 */
interface Uint8Array {
  toBase64(options?: { alphabet?: "base64" | "base64url"; omitPadding?: boolean }): string;
  toHex(): string;
}

interface Uint8ArrayConstructor {
  fromBase64(
    base64: string,
    options?: { alphabet?: "base64" | "base64url" },
  ): Uint8Array<ArrayBuffer>;
  fromHex(hex: string): Uint8Array<ArrayBuffer>;
}
