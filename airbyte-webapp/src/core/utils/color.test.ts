import { isSafeHexValue, getTextColorForBackground } from "./color";

describe(`${isSafeHexValue.name}`, () => {
  it("should return false for invalid hex values", () => {
    expect(isSafeHexValue("")).toBe(false);
    expect(isSafeHexValue("123")).toBe(false); // Technically a valid hex color, but we only support 6 characters
    expect(isSafeHexValue("12345")).toBe(false);
    expect(isSafeHexValue("1234567")).toBe(false);
    expect(isSafeHexValue("12345g")).toBe(false);
    expect(isSafeHexValue("12345G")).toBe(false);
  });

  it("should return true for valid hex values", () => {
    expect(isSafeHexValue("123456")).toBe(true);
    expect(isSafeHexValue("abcdef")).toBe(true);
    expect(isSafeHexValue("ABCDEF")).toBe(true);
  });
});

describe(`${getTextColorForBackground.name}`, () => {
  it("should return 'dark' for invalid hex values", () => {
    expect(getTextColorForBackground("")).toBe("dark");
    expect(getTextColorForBackground("123")).toBe("dark");
    expect(getTextColorForBackground("12345")).toBe("dark");
    expect(getTextColorForBackground("1234567")).toBe("dark");
    expect(getTextColorForBackground("12345g")).toBe("dark");
    expect(getTextColorForBackground("12345G")).toBe("dark");
  });

  it("should return 'dark' for light backgrounds", () => {
    expect(getTextColorForBackground("ffffff")).toBe("dark");
    expect(getTextColorForBackground("f0f0f0")).toBe("dark");
    expect(getTextColorForBackground("f0f0ff")).toBe("dark");
  });

  it("should return 'light' for dark backgrounds", () => {
    expect(getTextColorForBackground("000000")).toBe("light");
    expect(getTextColorForBackground("000001")).toBe("light");
    expect(getTextColorForBackground("00000f")).toBe("light");
  });
});
