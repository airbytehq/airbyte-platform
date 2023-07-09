import { AppActionCodes } from "hooks/services/AppMonitoringService";

import { fetchDocumentation } from "./Documentation";

const trackAction = jest.fn();

describe("fetchDocumentation", () => {
  const documentationUrl = "/docs/integrations/destinations/firestore.md";
  const validContentTypeHeader: [string, string] = ["Content-Type", "text/markdown; charset=UTF-8"];
  const invalidContenttypeHeader: [string, string] = ["Content-Type", "text/html; charset=utf-8"];

  afterEach(() => {
    jest.resetAllMocks();
  });

  it("should throw on non markdown content-type", async () => {
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

  it("should track a custom action if fetch fails", async () => {
    const documentationUrl = "/docs/integrations/destinations/firestore.md";

    global.fetch = jest.fn().mockResolvedValue({
      status: 404,
      ok: false,
      headers: new Headers([validContentTypeHeader]),
      text: () => Promise.resolve("Some mock text content"),
    });

    await expect(fetchDocumentation(documentationUrl, trackAction)).resolves.not.toThrow();
    expect(trackAction).toHaveBeenCalledWith(AppActionCodes.CONNECTOR_DOCUMENTATION_FETCH_ERROR, {
      url: documentationUrl,
      status: 404,
    });
  });
});
