// This module defines some conveniences for programmatically constructing regular
// expressions from conventional, reusable parts. It is, emphatically, not intended to
// replace simple literal RegExps or one-off patterns.
export function buildRegExp(...subPatterns: string[]) {
  return new RegExp(subPatterns.join(""));
}

// Conventional, reusable sub-patterns
export const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
