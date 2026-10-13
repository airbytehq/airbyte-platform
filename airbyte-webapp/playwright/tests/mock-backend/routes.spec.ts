/**
 * Self-tests of the mock backend's route table (playwright/mock/routes.ts). They need no browser page.
 */
import { test, expect } from "@playwright/test";

import { createRouteMatcher, matchRoute } from "../../mock/routes";

test.describe("Mock backend route table", () => {
  test("a path without params wins over a path with params", () => {
    // The table lists the path with params first, so the order of the table does not decide the match.
    const match = createRouteMatcher({
      getThing: { method: "GET", path: "/api/v1/things/{thingId}", pathParams: ["thingId"] },
      getSpecialThing: { method: "GET", path: "/api/v1/things/special", pathParams: [] },
    });

    expect(match("GET", "/api/v1/things/special")).toEqual({ op: "getSpecialThing", params: {} });
    expect(match("GET", "/api/v1/things/42")).toEqual({ op: "getThing", params: { thingId: "42" } });
  });

  test("finds Airbyte API operations by method and path", () => {
    expect(matchRoute("POST", "/api/v1/connections/delete")).toEqual({ op: "deleteConnection", params: {} });
    expect(matchRoute("GET", "/api/v1/user_invitations/by_code/abc")).toEqual({
      op: "getUserInvitation",
      params: { inviteCode: "abc" },
    });
  });

  test("decodes path params", () => {
    expect(matchRoute("GET", "/api/v1/user_invitations/by_code/a%2Fb%20c")).toEqual({
      op: "getUserInvitation",
      params: { inviteCode: "a/b c" },
    });
  });

  test("returns undefined for an unknown path or a wrong method", () => {
    expect(matchRoute("POST", "/api/v1/no_such_endpoint")).toBeUndefined();
    expect(matchRoute("GET", "/api/v1/connections/delete")).toBeUndefined();
  });
});
