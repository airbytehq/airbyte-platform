import { isDefined, isHttpUrl } from "./common";

it.each([
  [null, false],
  [undefined, false],
  ["", true],
  ["0", true],
  [0, true],
  [[], true],
  [{}, true],
])("should pass .isDefined(%i)", (a, expected) => {
  expect(isDefined(a)).toEqual(expected);
});

it.each([
  ["https://www.example.com", true],
  ["http://example.com", true],
  ["ftp://example.com", false],
  ["www.example.com", false],
  ["example.com", false],
  ["invalid-url", false],
  [null, false],
  [undefined, false],
  ["", false],
])("should pass .isUrl(%s)", (url, expected) => {
  expect(isHttpUrl(url)).toEqual(expected);
});
