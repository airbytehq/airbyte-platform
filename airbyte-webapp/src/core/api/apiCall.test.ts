import { fetchApiCall } from "./apiCall";

describe("fetchApiCall request body serialization", () => {
  const originalFetch = global.fetch;

  afterEach(() => {
    global.fetch = originalFetch;
  });

  const mockFetch = () => {
    let requestBody: BodyInit | null | undefined;
    global.fetch = jest.fn(async (_input, init) => {
      requestBody = init?.body;
      return { status: 204 } as Response;
    }) as jest.MockedFunction<typeof fetch>;

    return () => requestBody;
  };

  it("serializes a populated object as JSON", async () => {
    const getRequestBody = mockFetch();

    await fetchApiCall(
      { url: "/test", method: "POST", data: { organizationId: "org-123" } },
      { getAccessToken: async () => null },
      "https://api.airbyte.test"
    );

    expect(getRequestBody()).toBe('{"organizationId":"org-123"}');
  });

  it("serializes a string as a JSON string literal", async () => {
    const getRequestBody = mockFetch();

    await fetchApiCall(
      { url: "/test", method: "POST", data: "raw-string" },
      { getAccessToken: async () => null },
      "https://api.airbyte.test"
    );

    expect(getRequestBody()).toBe('"raw-string"');
  });

  it("serializes an empty object as JSON", async () => {
    const getRequestBody = mockFetch();

    await fetchApiCall(
      { url: "/test", method: "POST", data: {} },
      { getAccessToken: async () => null },
      "https://api.airbyte.test"
    );

    expect(getRequestBody()).toBe("{}");
  });

  it("serializes an object containing only undefined values as JSON", async () => {
    const getRequestBody = mockFetch();

    await fetchApiCall(
      { url: "/test", method: "POST", data: { organizationId: undefined } },
      { getAccessToken: async () => null },
      "https://api.airbyte.test"
    );

    expect(getRequestBody()).toBe("{}");
  });

  it("passes Blob bodies through unchanged", async () => {
    const getRequestBody = mockFetch();
    const blob = new Blob(["test"]);

    await fetchApiCall(
      { url: "/test", method: "POST", data: blob },
      { getAccessToken: async () => null },
      "https://api.airbyte.test"
    );

    expect(getRequestBody()).toBe(blob);
  });

  it("omits the body when no data is provided", async () => {
    const getRequestBody = mockFetch();

    await fetchApiCall(
      { url: "/test", method: "POST" },
      { getAccessToken: async () => null },
      "https://api.airbyte.test"
    );

    expect(getRequestBody()).toBeUndefined();
  });
});
