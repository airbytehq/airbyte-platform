import { getWorkspaceColorByIndex } from "./utils";

describe("getWorkspaceColorByIndex", () => {
  it("should return consistent colors for the same index", () => {
    const color1 = getWorkspaceColorByIndex(5);
    const color2 = getWorkspaceColorByIndex(5);
    expect(color1).toBe(color2);
  });

  it("should wrap around when index exceeds palette size", () => {
    // Palette has 10 colors
    const color0 = getWorkspaceColorByIndex(0);
    const color10 = getWorkspaceColorByIndex(10);
    expect(color10).toBe(color0);
  });

  it("should return different colors for consecutive indices", () => {
    const colors = new Set<string>();
    for (let i = 0; i < 10; i++) {
      colors.add(getWorkspaceColorByIndex(i));
    }
    // First 10 indices should produce 10 different colors
    expect(colors.size).toBe(10);
  });
});
