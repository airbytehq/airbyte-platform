import { extractCustomerRedirectUrl } from "./oauthStateUtils";

describe("extractCustomerRedirectUrl", () => {
  it("returns null for null state", () => {
    expect(extractCustomerRedirectUrl(null)).toBeNull();
  });

  it("returns null for empty string state", () => {
    expect(extractCustomerRedirectUrl("")).toBeNull();
  });

  it("returns null for state without redirect part", () => {
    expect(extractCustomerRedirectUrl("abc123")).toBeNull();
  });

  it("returns null for state with empty redirect value", () => {
    expect(extractCustomerRedirectUrl("abc123|redirect=")).toBeNull();
  });

  it("extracts redirect URL from valid state with standard base64", () => {
    // "https://app.airbyte.ai" base64 encoded
    const url = "https://app.airbyte.ai";
    const encoded = btoa(url);
    const state = `abc123|redirect=${encoded}`;

    expect(extractCustomerRedirectUrl(state)).toBe(url);
  });

  it("extracts redirect URL from state with URL-safe base64 encoding", () => {
    // URL-safe base64 uses - and _ instead of + and /
    const url = "https://app.airbyte.ai/auth_flow?param=value";
    const standardBase64 = btoa(url);
    const urlSafeBase64 = standardBase64.replace(/\+/g, "-").replace(/\//g, "_");
    const state = `abc123|redirect=${urlSafeBase64}`;

    expect(extractCustomerRedirectUrl(state)).toBe(url);
  });

  it("handles frontend-dev URLs correctly", () => {
    const url = "https://frontend-dev-cloud.internal.airbyte.dev";
    const encoded = btoa(url);
    const state = `randomState|redirect=${encoded}`;

    expect(extractCustomerRedirectUrl(state)).toBe(url);
  });

  it("handles deploy preview URLs correctly", () => {
    const url = "https://deploy-preview-18437-cloud.frontend-dev-preview.internal.airbyte.dev";
    const encoded = btoa(url);
    const state = `xyz789|redirect=${encoded}`;

    expect(extractCustomerRedirectUrl(state)).toBe(url);
  });

  it("returns null for malformed base64", () => {
    const state = "abc123|redirect=not-valid-base64!!!";
    expect(extractCustomerRedirectUrl(state)).toBeNull();
  });

  it("returns null when redirect key exists but value is not base64", () => {
    const state = "abc123|redirect=%%%invalid%%%";
    expect(extractCustomerRedirectUrl(state)).toBeNull();
  });

  it("ignores other pipe-separated parts and finds redirect", () => {
    const url = "https://app.airbyte.ai";
    const encoded = btoa(url);
    const state = `abc123|other=value|redirect=${encoded}|more=data`;

    expect(extractCustomerRedirectUrl(state)).toBe(url);
  });

  it("returns first redirect if multiple exist", () => {
    const url1 = "https://first.com";
    const url2 = "https://second.com";
    const state = `abc|redirect=${btoa(url1)}|redirect=${btoa(url2)}`;

    expect(extractCustomerRedirectUrl(state)).toBe(url1);
  });
});
