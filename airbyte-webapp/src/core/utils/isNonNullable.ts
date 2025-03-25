// Useful for filtering out undefined and null values from arrays
// Example: const filteredArray = array.filter(isNonNullable);
export function isNonNullable<T>(value: T): value is NonNullable<T> {
  return value !== undefined && value !== null;
}
