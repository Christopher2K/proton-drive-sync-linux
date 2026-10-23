/**
 * Must be the FIRST import of the process (side-effect, order-sensitive):
 * installs core-js polyfills for Uint8Array.prototype.toBase64/fromBase64/
 * toHex/fromHex, which @protontech/crypto and the Drive SDK require but Node
 * does not yet ship. See docs/research/sdk-client.md §6.
 */
import "@protontech/crypto/polyfill";
