import { render } from "test-utils";

import { CleanedLogLines } from "./useCleanLogs";
import { Row, sanitizeHtml } from "./VirtualLogs";

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

const mockLogLines: CleanedLogLines = [
  {
    lineNumber: 1,
    original: "2021-08-11 12:00:00 source: just an innocent log line.",
    text: "source: just an innocent log line.",
    source: "source",
  },
  {
    lineNumber: 2,
    original: '2021-08-11 12:00:01 source: log with <img src="" onerror="alert(\'danger!\')"> HTML injection',
    text: 'source: log with <img src="" onerror="alert(\'danger!\')"> HTML injection',
    source: "source",
  },
];

describe(`${Row.name}`, () => {
  const PARTIAL_REGEX = "[";

  it("should not throw if a partial regex expression is provided as searchTerm with formatted logs", () => {
    expect(() => {
      render(Row(0, mockLogLines[0], { searchTerm: PARTIAL_REGEX, showStructuredLogs: false }));
    }).not.toThrow();
  });

  // We are using react-highlight-words to highlight the search term in the logs. This library will throw by default if
  // the search term is not a valid regex expression. This test is inteded to guard against that.
  it("should not throw if a partial regex expression is provided as searchTerm with structured logs", () => {
    expect(() => {
      render(Row(0, mockLogLines[0], { searchTerm: PARTIAL_REGEX, showStructuredLogs: true }));
    }).not.toThrow();
  });
});
