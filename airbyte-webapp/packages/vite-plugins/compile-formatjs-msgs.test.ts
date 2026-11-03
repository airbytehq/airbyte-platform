import { parse } from "@formatjs/icu-messageformat-parser";

import { compileFormatJsMessages } from "./compile-formatjs-msgs";

const transform = (code: string, id = "/src/locales/en.json") => {
  const hook = compileFormatJsMessages().transform;
  if (typeof hook !== "function") {
    throw new Error("Expected a transform hook");
  }
  return hook.call({} as never, code, id, {});
};

describe("compileFormatJsMessages", () => {
  it("compiles raw JSON before Vite's JSON transform", async () => {
    expect(compileFormatJsMessages().enforce).toBe("pre");
    const message = "Hello {name}";
    expect(await transform(JSON.stringify({ greeting: message }))).toBe(JSON.stringify({ greeting: parse(message) }));
  });

  it("preserves an empty locale", async () => {
    expect(await transform("{}")).toBe("{}");
  });

  it("propagates malformed JSON errors", () => {
    expect(() => transform("{")).toThrow(SyntaxError);
  });

  it("does not transform other JSON files", async () => {
    expect(await transform('{"message":"Hello"}', "/src/data.json")).toBeUndefined();
  });
});
