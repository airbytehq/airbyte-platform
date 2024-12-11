import { isSemanticVersionTags, isVersionUpgraded } from "./utils";

describe(`'${isSemanticVersionTags.name}'`, () => {
  it("should return true for valid semantic version tags", () => {
    expect(isSemanticVersionTags("2.0.0", "1.0.0")).toBe(true);
    expect(isSemanticVersionTags("3.0.0", "3.0.1")).toBe(true);
  });

  it("should return false for invalid semantic version tags", () => {
    expect(isSemanticVersionTags("1", "2.0")).toBe(false);
    expect(isSemanticVersionTags("1.0", "2.0.0")).toBe(false);
    expect(isSemanticVersionTags("1.0.0", "dev")).toBe(false);
    expect(isSemanticVersionTags("dev", "1.0.0")).toBe(false);
    expect(isSemanticVersionTags("1.0.0", "1.0.0-rc1")).toBe(false);
  });
});

describe(`'${isVersionUpgraded.name}'`, () => {
  it("should return true if newVersion is greater than oldVersion", () => {
    expect(isVersionUpgraded("2.0", "1.0")).toBe(true);
    expect(isVersionUpgraded("1.1", "1.0")).toBe(true);
    expect(isVersionUpgraded("1.0.1", "1.0.0")).toBe(true);
    expect(isVersionUpgraded("1.0.0.1", "1.0.0.0")).toBe(true);
  });

  it("should return false if oldVersion is less than or equal to newVersion", () => {
    expect(isVersionUpgraded("1.0", "2.0")).toBe(false);
    expect(isVersionUpgraded("1.0", "1.1")).toBe(false);
    expect(isVersionUpgraded("1.0.0", "1.0.1")).toBe(false);
    expect(isVersionUpgraded("1.0.0.0", "1.0.0.1")).toBe(false);
    expect(isVersionUpgraded("1.0.0", "1.0.0")).toBe(false);
  });
});
