/**
 * ProtonDriveAccount implementation: supplies the SDK with already-unlocked
 * address keys. Ported from the SDK repo's reference implementation
 * (incubating/account/js/src/addresses.ts) — see docs/research/proton-auth.md §3.
 *
 * Key model: the user's armored private keys open with userKeyPassword
 * (bcrypt-derived). Address keys either open directly with userKeyPassword
 * (no Token) or carry a Token encrypted to the user key — decrypt + verify
 * the Token, then use it as the address key's passphrase.
 */
import { CryptoProxy } from "@protontech/crypto";
import { VERIFICATION_STATUS } from "@protontech/crypto/constants";
import type { PrivateKeyReference, PublicKeyReference } from "@protontech/crypto";
import { api, type Session } from "./auth";

export interface AccountAddress {
  email: string;
  addressId: string;
  primaryKeyIndex: number;
  keys: { id: string; key: PrivateKeyReference }[];
}

interface UserData {
  userPrivateKeys: PrivateKeyReference[];
  userPublicKeys: PublicKeyReference[];
  addresses: any[];
}

export class DriveAccount {
  private userDataPromise?: Promise<UserData>;
  private publicKeysByEmail = new Map<string, Promise<PublicKeyReference[]>>();

  constructor(private readonly session: Session) {}

  private async getUserData(): Promise<UserData> {
    this.userDataPromise ??= (async () => {
      const { User } = await api("core/v4/users", { session: this.session });
      const userPrivateKeys: PrivateKeyReference[] = [];
      const userPublicKeys: PublicKeyReference[] = [];
      for (const userKey of User?.Keys ?? []) {
        const privateKey = await CryptoProxy.importPrivateKey({
          armoredKey: userKey.PrivateKey,
          passphrase: this.session.userKeyPassword,
        });
        userPrivateKeys.push(privateKey);
        userPublicKeys.push(
          await CryptoProxy.importPublicKey({
            binaryKey: await CryptoProxy.exportPublicKey({ key: privateKey, format: "binary" }),
          }),
        );
      }
      if (userPrivateKeys.length === 0) throw new Error("No usable user keys");
      const { Addresses } = await api("core/v4/addresses", { session: this.session });
      return { userPrivateKeys, userPublicKeys, addresses: Addresses ?? [] };
    })();
    return this.userDataPromise;
  }

  private async unlockAddressKey(key: any): Promise<{ id: string; key: PrivateKeyReference }> {
    const { userPrivateKeys, userPublicKeys } = await this.getUserData();

    let privateKey: PrivateKeyReference;
    if (!key.Token && key.PrivateKey) {
      // Simple case: address key encrypted directly with the user key password.
      privateKey = await CryptoProxy.importPrivateKey({
        armoredKey: key.PrivateKey,
        passphrase: this.session.userKeyPassword,
      });
    } else {
      // Address key passphrase lives in a Token encrypted+signed to the user key.
      const { data: decryptedToken, verificationStatus } = await CryptoProxy.decryptMessage({
        armoredMessage: key.Token ?? "",
        armoredSignature: key.Signature ?? "",
        decryptionKeys: userPrivateKeys,
        verificationKeys: userPublicKeys,
      });
      if (verificationStatus !== VERIFICATION_STATUS.SIGNED_AND_VALID) {
        throw new Error(`Failed to verify address key ${key.ID}`);
      }
      privateKey = await CryptoProxy.importPrivateKey({
        armoredKey: key.PrivateKey ?? "",
        passphrase: decryptedToken.toString(),
      });
    }
    return { id: key.ID, key: privateKey };
  }

  async getOwnAddress(emailOrAddressId: string): Promise<AccountAddress> {
    const { addresses } = await this.getUserData();
    const address = addresses.find(
      (a: any) => a.ID === emailOrAddressId || a.Email === emailOrAddressId,
    );
    if (!address?.ID || !address?.Email) {
      throw new Error(`Address ${emailOrAddressId} not found`);
    }
    const keys: { id: string; key: PrivateKeyReference }[] = [];
    const errors: unknown[] = [];
    for (const key of address.Keys ?? []) {
      try {
        keys.push(await this.unlockAddressKey(key));
      } catch (e) {
        errors.push(e);
      }
    }
    if (keys.length === 0) throw new Error("No private key found", { cause: errors });
    return { email: address.Email, addressId: address.ID, primaryKeyIndex: 0, keys };
  }

  async getOwnPrimaryAddress(): Promise<AccountAddress> {
    const { addresses } = await this.getUserData();
    if (!addresses[0]?.ID) throw new Error("Missing primary address");
    return this.getOwnAddress(addresses[0].ID);
  }

  async getOwnAddresses(): Promise<AccountAddress[]> {
    const { addresses } = await this.getUserData();
    const out: AccountAddress[] = [];
    for (const a of addresses) {
      if (a?.ID) out.push(await this.getOwnAddress(a.ID));
    }
    return out;
  }

  async hasProtonAccount(email: string): Promise<boolean> {
    return (await this.getPublicKeys(email)).length > 0;
  }

  async getPublicKeys(email: string, forceRefresh = false): Promise<PublicKeyReference[]> {
    if (!forceRefresh && this.publicKeysByEmail.has(email)) {
      return this.publicKeysByEmail.get(email)!;
    }
    const promise = (async () => {
      const { addresses, userPrivateKeys, userPublicKeys } = await this.getUserData();
      const own = addresses.find((a: any) => a.Email === email);
      if (own) {
        const out: PublicKeyReference[] = [];
        for (const key of own.Keys ?? []) {
          const { key: priv } = await this.unlockAddressKey(key);
          out.push(
            await CryptoProxy.importPublicKey({
              binaryKey: await CryptoProxy.exportPublicKey({ key: priv, format: "binary" }),
            }),
          );
        }
        return out;
      }
      void userPrivateKeys;
      void userPublicKeys;
      const res = await api(`core/v4/keys?Email=${encodeURIComponent(email)}`, {
        session: this.session,
      });
      return Promise.all(
        (res.Address?.Keys ?? []).map((k: any) =>
          CryptoProxy.importPublicKey({ armoredKey: k.PublicKey }),
        ),
      );
    })();
    this.publicKeysByEmail.set(email, promise);
    return promise;
  }
}
