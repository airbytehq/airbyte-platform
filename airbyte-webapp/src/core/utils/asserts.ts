export function assertNever(x: never): never {
  throw new Error(`Assert failed. ${String(x)} was not 'never'.`);
}
