export function isDefined<T>(a: T | null | undefined): a is Exclude<T, null | undefined> {
  return a !== undefined && a !== null;
}

export function isHttpUrl(url: string | null | undefined): boolean {
  if (!url) {
    return false;
  }

  try {
    const { protocol } = new URL(url);
    const valid_protocols = ["http:", "https:"];
    return valid_protocols.includes(protocol);
  } catch (_) {
    return false;
  }
}
