import isEqual from "lodash/isEqual";

export function equal(o1?: unknown, o2?: unknown): boolean {
  return isEqual(o1, o2);
}

export function naturalComparator(a: string, b: string): number {
  return a.localeCompare(b, undefined, { numeric: true });
}

export function naturalComparatorBy<T>(keyF: (obj: T) => string): (a: T, b: T) => number {
  return (a, b) => naturalComparator(keyF(a), keyF(b));
}

export function haveSameShape(objA: unknown, objB: unknown): boolean {
  // Check if both items are not objects or arrays.
  if ((typeof objA !== "object" || objA === null) && (typeof objB !== "object" || objB === null)) {
    return typeof objA === typeof objB;
  }

  // If one is an object and the other isn't, shapes are not the same.
  if (typeof objA !== "object" || typeof objB !== "object" || objA === null || objB === null) {
    return false;
  }

  // Check if both items are arrays. If one is an array and the other isn't, shapes are not the same.
  if (Array.isArray(objA) !== Array.isArray(objB)) {
    return false;
  }

  // If both items are arrays, check if they have the same length.
  if (Array.isArray(objA) && Array.isArray(objB) && objA.length !== objB.length) {
    return false;
  }

  const keysA = Object.keys(objA);
  const keysB = Object.keys(objB);

  // Check if both objects have the same number of keys.
  if (keysA.length !== keysB.length) {
    return false;
  }

  for (const key of keysA) {
    // Check if key exists in both objects.
    if (!Object.prototype.hasOwnProperty.call(objB, key)) {
      return false;
    }

    // Check if sub-objects have the same shape.
    if (!haveSameShape((objA as Record<string, unknown>)[key], (objB as Record<string, unknown>)[key])) {
      return false;
    }
  }

  return true;
}
