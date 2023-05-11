import anyBase from "any-base";
import { v4 as uuid } from "uuid";

const alphabet = "123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ";
const converter = anyBase(anyBase.HEX, alphabet);

/**
 * Generate a unique id, that has a short base58 encoded version (only numbers and letters)
 * as a representation. Thus this is shorter and more catered than a UUIDv4 in places users
 * might need to see and copy it.
 */
export const shortUuid = () => {
  return converter(uuid().replace(/-/g, ""));
};
