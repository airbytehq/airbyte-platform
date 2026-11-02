import { prepareMarkdown, resolveRootAbsoluteImageSrc } from "./DocumentationPanel";

const testMarkdown = `## Some documentation

<!-- env:cloud -->
### Only relevant for Cloud

some specific documentation that only applies for cloud.
<!-- /env:cloud -->

<!-- env:oss -->
### Only relevant for OSS users

some specific documentation that only applies for OSS users
<!-- /env:oss -->`;

describe("prepareMarkdown", () => {
  it("should prepare markdown for cloud", () => {
    expect(prepareMarkdown(testMarkdown, "cloud")).toBe(`## Some documentation

<!-- env:cloud -->
### Only relevant for Cloud

some specific documentation that only applies for cloud.
<!-- /env:cloud -->

`);
  });
  it("should prepare markdown for oss", () => {
    expect(prepareMarkdown(testMarkdown, "oss")).toBe(`## Some documentation



<!-- env:oss -->
### Only relevant for OSS users

some specific documentation that only applies for OSS users
<!-- /env:oss -->`);
  });
});

describe("resolveRootAbsoluteImageSrc", () => {
  it("should load a root-absolute image from the docs site", () => {
    expect(resolveRootAbsoluteImageSrc("/.gitbook/assets/destination/databricks/open_sql_warehouse.png")).toBe(
      "https://docs.airbyte.com/.gitbook/assets/destination/databricks/open_sql_warehouse.png"
    );
  });

  it("should keep the query string of a root-absolute image", () => {
    expect(resolveRootAbsoluteImageSrc("/.gitbook/assets/image.png?raw=true")).toBe(
      "https://docs.airbyte.com/.gitbook/assets/image.png?raw=true"
    );
  });

  it("should leave an absolute image URL unchanged", () => {
    expect(resolveRootAbsoluteImageSrc("https://example.com/image.png")).toBe("https://example.com/image.png");
  });

  it("should leave a protocol-relative image URL unchanged", () => {
    expect(resolveRootAbsoluteImageSrc("//example.com/image.png")).toBe("//example.com/image.png");
  });

  it("should leave a data URI unchanged", () => {
    expect(resolveRootAbsoluteImageSrc("data:image/png;base64,AAAA")).toBe("data:image/png;base64,AAAA");
  });

  it("should leave an empty image source unchanged", () => {
    expect(resolveRootAbsoluteImageSrc("")).toBe("");
  });
});
