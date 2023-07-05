import { getMatchIndices, sanitizeHtml } from "./VirtualLogs";

describe(`${sanitizeHtml.name}`, () => {
  it("should return a normal logLine as it is", () => {
    expect(sanitizeHtml("just an innocent log line.")).toBe("just an innocent log line.");
  });

  it("should escape HTML inside a log line", () => {
    expect(sanitizeHtml('log with <img src="" onerror="alert(\'danger!\')"> HTML injection')).toBe(
      `log with &lt;img /&gt; HTML injection`
    );
  });
});

describe(`${getMatchIndices.name}`, () => {
  it("should return empty array if no match", () => {
    expect(getMatchIndices("no match", "zzz")).toEqual([]);
  });

  it("should return an array with one match if there is a single match", () => {
    expect(getMatchIndices("a b c d", "a")).toEqual([0]);
  });

  it("should return an array with multiple matches if there are multiple matches", () => {
    expect(getMatchIndices("a b c d a a", "a")).toEqual([0, 8, 10]);
  });
});
