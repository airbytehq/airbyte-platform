import { sanitizeHtml } from "./VirtualLogs";

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
