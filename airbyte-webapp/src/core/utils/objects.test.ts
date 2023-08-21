import { haveSameShape } from "./objects";

describe("haveSameShape", () => {
  // Primitives
  it("should return true for two numbers", () => {
    expect(haveSameShape(1, 2)).toBe(true);
  });

  it("should return true for two strings", () => {
    expect(haveSameShape("a", "b")).toBe(true);
  });

  it("should return true for two booleans", () => {
    expect(haveSameShape(true, false)).toBe(true);
  });

  it("should return false for a number and a string", () => {
    expect(haveSameShape(1, "1")).toBe(false);
  });

  // Arrays
  it("should return true for two empty arrays", () => {
    expect(haveSameShape([], [])).toBe(true);
  });

  it("should return true for arrays with the same shape", () => {
    expect(haveSameShape([1, 2], [3, 4])).toBe(true);
  });

  it("should return false for arrays with different lengths", () => {
    expect(haveSameShape([1], [1, 2])).toBe(false);
  });

  it("should return false for arrays with different shapes", () => {
    expect(haveSameShape([1, { a: 2 }], [1, { a: 2, b: 3 }])).toBe(false);
  });

  // Objects
  it("should return true for two empty objects", () => {
    expect(haveSameShape({}, {})).toBe(true);
  });

  it("should return true for objects with the same shape", () => {
    expect(haveSameShape({ a: 1, b: 2 }, { a: 3, b: 4 })).toBe(true);
  });

  it("should return false for objects with different keys", () => {
    expect(haveSameShape({ a: 1 }, { b: 1 })).toBe(false);
  });

  // Nested structures
  it("should return true for nested structures with the same shape", () => {
    const objA = {
      a: 1,
      b: {
        c: 2,
        d: [3, 4],
      },
    };
    const objB = {
      a: 5,
      b: {
        c: 6,
        d: [7, 8],
      },
    };
    expect(haveSameShape(objA, objB)).toBe(true);
  });

  it("should return false for nested structures with different shapes", () => {
    const objA = {
      a: 1,
      b: {
        c: 2,
      },
    };
    const objB = {
      a: 3,
      b: {
        c: 4,
        d: 5,
      },
    };
    expect(haveSameShape(objA, objB)).toBe(false);
  });

  // Different types
  it("should return false for an array and an object", () => {
    expect(haveSameShape([], {})).toBe(false);
  });

  it("should return false for an object and a number", () => {
    expect(haveSameShape({}, 1)).toBe(false);
  });

  it("should return false for an array and a string", () => {
    expect(haveSameShape([], "a")).toBe(false);
  });
});
