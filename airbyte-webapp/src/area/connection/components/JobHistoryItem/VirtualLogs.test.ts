import { getSearchMatchesInLine, sanitizeHtml } from "./VirtualLogs";

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

describe(`${getSearchMatchesInLine.name}`, () => {
  it("should return empty array if no match", () => {
    expect(getSearchMatchesInLine("no match", "zzz")).toEqual([]);
  });

  it("should return an array with one match if there is a single match", () => {
    expect(getSearchMatchesInLine("a b c d", "a")).toEqual([{ precedingNewlines: 0, characterOffsetLeft: 0 }]);
  });

  it("should return an array with multiple matches if there are multiple matches", () => {
    expect(getSearchMatchesInLine("a b c d a a", "a")).toEqual([
      { precedingNewlines: 0, characterOffsetLeft: 0 },
      { precedingNewlines: 0, characterOffsetLeft: 8 },
      { precedingNewlines: 0, characterOffsetLeft: 10 },
    ]);
  });

  it("should calculate the correct preceding newlines", () => {
    expect(getSearchMatchesInLine("a b c\na b a", "a")).toEqual([
      { precedingNewlines: 0, characterOffsetLeft: 0 },
      { precedingNewlines: 1, characterOffsetLeft: 0 },
      { precedingNewlines: 1, characterOffsetLeft: 4 },
    ]);
  });
});
