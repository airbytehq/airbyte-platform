import { AppActionCodes } from "hooks/services/AppMonitoringService";

import { fetchDocumentation } from "./Documentation";

const trackAction = jest.fn();

const originalFetch = global.fetch;

describe("fetchDocumentation", () => {
  const documentationUrl = "/docs/integrations/destinations/firestore.md";
  const validContentTypeHeader: [string, string] = ["Content-Type", "text/markdown; charset=UTF-8"];
  const invalidContenttypeHeader: [string, string] = ["Content-Type", "text/html; charset=utf-8"];

  afterAll(() => {
    global.fetch = originalFetch;
  });

  it("should throw and track a custom action if a non markdown content-type is returned", async () => {
    global.fetch = jest.fn().mockResolvedValue({
      status: 200,
      ok: true,
      headers: new Headers([invalidContenttypeHeader]),
      text: () => Promise.resolve("Some mock text content"),
    });

    await expect(fetchDocumentation(documentationUrl, trackAction)).rejects.toThrow();
    expect(trackAction).toHaveBeenCalledWith(AppActionCodes.CONNECTOR_DOCUMENTATION_NOT_MARKDOWN, {
      url: documentationUrl,
      contentType: "text/html; charset=utf-8",
    });
  });

  it("should throw and track a custom action if fetch fails", async () => {
    global.fetch = jest.fn().mockResolvedValue({
      status: 404,
      ok: false,
      headers: new Headers([validContentTypeHeader]),
      text: () => Promise.resolve("Some mock markdown content"),
    });

    await expect(fetchDocumentation(documentationUrl, trackAction)).rejects.toThrow();
    expect(trackAction).toHaveBeenCalledWith(AppActionCodes.CONNECTOR_DOCUMENTATION_FETCH_ERROR, {
      url: documentationUrl,
      status: 404,
    });
  });

  it("should not throw if valid markdown is returned", async () => {
    global.fetch = jest.fn().mockResolvedValue({
      status: 200,
      ok: true,
      headers: new Headers([validContentTypeHeader]),
      text: () => Promise.resolve("Some mock markdown content"),
    });

    await expect(fetchDocumentation(documentationUrl, trackAction)).resolves.not.toThrow();
    expect(trackAction).toHaveBeenCalledWith(AppActionCodes.CONNECTOR_DOCUMENTATION_FETCH_ERROR, {
      url: documentationUrl,
      status: 404,
    });
  });
});
