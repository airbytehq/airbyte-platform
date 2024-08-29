import type { UserManager } from "oidc-client-ts";

import { renderHook, waitFor } from "@testing-library/react";
import { PropsWithChildren } from "react";

import { TestWrapper } from "test-utils/testutils";

import { useAuthService } from "core/services/auth";

import { initializeUserManager, CloudAuthService } from "./CloudAuthService";

let windowSearchSpy: jest.SpyInstance;

let mockUserManager: UserManager;

const DEFAULT_JEST_WINDOW_ORIGIN = "http://localhost";

jest.mock("oidc-client-ts", () => ({
  UserManager: jest.fn().mockImplementation((options) => {
    // Mock the UserManager contructor to store the latest created instance to access it for testing
    return (mockUserManager = new (jest.requireActual("oidc-client-ts").UserManager)(options));
  }),
  WebStorageStateStore: jest.requireActual("oidc-client-ts").WebStorageStateStore,
}));

describe(`${initializeUserManager.name}()`, () => {
  beforeEach(() => {
    windowSearchSpy = jest.spyOn(window, "location", "get");
  });

  afterEach(() => {
    windowSearchSpy.mockRestore();
  });

  it("should initialize with the correct default realm", () => {
    const userManager = initializeUserManager();
    expect(userManager.settings.authority).toMatch(/auth\/realms\/_airbyte-cloud-users/);
  });

  it("should initialize realm from query params", () => {
    windowSearchSpy.mockImplementation(() => ({
      search: "?realm=another-realm",
    }));
    const userManager = initializeUserManager();
    expect(userManager.settings.authority).toMatch(/auth\/realms\/another-realm/);
  });

  it("should initialize realm based on local storage", () => {
    const mockKey = `oidc.user:${DEFAULT_JEST_WINDOW_ORIGIN}/auth/realms/local-storage-realm:local-storage-client-id`;
    window.localStorage.setItem(mockKey, "no need to populate the value for this test");
    const userManager = initializeUserManager();
    expect(userManager.settings.authority).toMatch(/auth\/realms\/local-storage-realm/);
    window.localStorage.removeItem(mockKey);
  });
});

describe(`${CloudAuthService.name}`, () => {
  const wrapper: React.FC<PropsWithChildren> = ({ children }) => (
    <TestWrapper>
      <CloudAuthService>{children}</CloudAuthService>
    </TestWrapper>
  );

  it("should initialize with the correct default realm", async () => {
    renderHook(() => useAuthService(), { wrapper });
    await waitFor(() => {
      expect(mockUserManager.settings.authority).toMatch(/auth\/realms\/_airbyte-cloud-users/);
    });
  });

  it("should initialize realm from query params", async () => {
    windowSearchSpy = jest.spyOn(window, "location", "get");
    windowSearchSpy.mockImplementation(() => ({
      search: "?realm=another-realm",
    }));

    renderHook(() => useAuthService(), { wrapper });

    await waitFor(() => {
      expect(mockUserManager.settings.authority).toMatch(/auth\/realms\/another-realm/);
    });

    windowSearchSpy.mockRestore();
  });

  it("should initialize realm based on local storage", async () => {
    window.localStorage.setItem(
      `oidc.user:${DEFAULT_JEST_WINDOW_ORIGIN}/auth/realms/local-storage-realm:local-storage-client-id`,
      JSON.stringify({
        id_token:
          "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIxOXpGOG5CVWNJcDNPMTBTQVZvUE1oYnhocThsbmVnRnJaNXBZcEtzT3NzIn0.eyJleHAiOjE2OTU2ODIzMjIsImlhdCI6MTY5NTY4MjAyMiwiYXV0aF90aW1lIjoxNjk1NjgyMDIxLCJqdGkiOiIxMGQ4Y2FkNi1jMWViLTQ3MTUtODFiMS01OGZmYjU3MDgyZGEiLCJpc3MiOiJodHRwczovL2Rldi0yLWNsb3VkLmFpcmJ5dGUuY29tL2F1dGgvcmVhbG1zL3Rlc3QtY29tcGFueS0xIiwiYXVkIjoiYWlyYnl0ZS13ZWJhcHAiLCJzdWIiOiIwYWEzYmVhYi02MDU3LTQ4NjEtYWQ5MS0wNWZlM2U3NmJhMjIiLCJ0eXAiOiJJRCIsImF6cCI6ImFpcmJ5dGUtd2ViYXBwIiwic2Vzc2lvbl9zdGF0ZSI6ImQwZDBlN2FhLWE1YzgtNDgwNi05MzA4LTQ2ZWE5ODRkZmE5ZCIsImF0X2hhc2giOiJ3TjNLSFltcDRDUUs4emN0QkwyMmVnIiwiYWNyIjoiMSIsInNpZCI6ImQwZDBlN2FhLWE1YzgtNDgwNi05MzA4LTQ2ZWE5ODRkZmE5ZCIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJuYW1lIjoiSm9leSBNYXJzaG1lbnQtSG93ZWxsIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWRtaW4tY29tcGFueS0xIiwiZ2l2ZW5fbmFtZSI6IkpvZXkiLCJmYW1pbHlfbmFtZSI6Ik1hcnNobWVudC1Ib3dlbGwiLCJlbWFpbCI6Impvc2VwaCthZG1pbi1jb21wYW55LTFAYWlyYnl0ZS5pbyJ9.eVbTYihQPkwBZugWClsbE6ePayDh5b3wGXYrBAhgxq0Bxe9ZdaJb_3EadqMu2xCCnYam1JgJyvnNoEBVQJlLPoBehByyBMC4xwoaHbNjvwAWHUXPlIvLIct_jo9Mnk-l2l_6uZf5rPAqBlQHQFf5SFIF_l9m7WyFafLQFemnkfy0AzmdO_yaT0LyuCpALHmXUeJuUgILuBM3AQd6IVeAi7zKwRHTk4YjaUbE4fdtFC1x11XGEVfuYJaH3S8-Zyu45vO7MKeK9gsqF6mtkgu66a0FBp8kjy4SsVCyBjoj9IZp_Q428Uy9MYX9JvQL3xEXBilv_ydjnCPp2J1pJ4Gdlw",
        session_state: "d0d0e7aa-a5c8-4806-9308-46ea984dfa9d",
        access_token:
          "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIxOXpGOG5CVWNJcDNPMTBTQVZvUE1oYnhocThsbmVnRnJaNXBZcEtzT3NzIn0.eyJleHAiOjE2OTU2ODIzMjIsImlhdCI6MTY5NTY4MjAyMiwiYXV0aF90aW1lIjoxNjk1NjgyMDIxLCJqdGkiOiJhNTkyZDUyNi0wOTA0LTQ2NjgtOTY2OS1mNGU1ZDI5MGQ4NDQiLCJpc3MiOiJodHRwczovL2Rldi0yLWNsb3VkLmFpcmJ5dGUuY29tL2F1dGgvcmVhbG1zL3Rlc3QtY29tcGFueS0xIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6IjBhYTNiZWFiLTYwNTctNDg2MS1hZDkxLTA1ZmUzZTc2YmEyMiIsInR5cCI6IkJlYXJlciIsImF6cCI6ImFpcmJ5dGUtd2ViYXBwIiwic2Vzc2lvbl9zdGF0ZSI6ImQwZDBlN2FhLWE1YzgtNDgwNi05MzA4LTQ2ZWE5ODRkZmE5ZCIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiaHR0cHM6Ly9sb2NhbGhvc3Q6MzAwMSIsImh0dHBzOi8vZGV2LTItY2xvdWQuYWlyYnl0ZS5jb20iXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImRlZmF1bHQtcm9sZXMtdGVzdC1jb21wYW55LTEiLCJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgZW1haWwgcHJvZmlsZSIsInNpZCI6ImQwZDBlN2FhLWE1YzgtNDgwNi05MzA4LTQ2ZWE5ODRkZmE5ZCIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJuYW1lIjoiSm9leSBNYXJzaG1lbnQtSG93ZWxsIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWRtaW4tY29tcGFueS0xIiwiZ2l2ZW5fbmFtZSI6IkpvZXkiLCJmYW1pbHlfbmFtZSI6Ik1hcnNobWVudC1Ib3dlbGwiLCJlbWFpbCI6Impvc2VwaCthZG1pbi1jb21wYW55LTFAYWlyYnl0ZS5pbyJ9.3OWOx6MqFxETHlbNdzPIggIbTas0eZEojKDWfys_tqokOiLPJEn1t3Vw-YNwNP6Dy8XOtzCW1qTjRU5IhXT2bRMWVJOED3esHg2TBiJFbFOpk27ab-AZ37_h4sFnBCG0AgT1TK_JfdFiIsbXKKiLDPtJcb-EU526gu_CNYSVf0bWE8qhuynvJi6Lsw2PNcoppHX2p-PMBfLR2YillOCIDLL7vuB5X_K_NsVBZFIdHOw5i2qYYM6BI7HcjK7e2VI8TXY9h0OjoJfwmEXteDmrG2S0zmv8glAHLh8ospApZeIBr8Vt0Yek59FHgG1ocqCa4bm7fDWefprMoufitPrYSg",
        refresh_token:
          "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI4NDUzZDBhOC02NzQ2LTRmMDktOGM2Ny02YjhiNmVhZTZjN2IifQ.eyJleHAiOjE2OTU2ODM4MjIsImlhdCI6MTY5NTY4MjAyMiwianRpIjoiN2MxYTkxOWQtZjNlZi00ZTRhLWFlMDUtMWM3MWIyNmE4Y2Q3IiwiaXNzIjoiaHR0cHM6Ly9kZXYtMi1jbG91ZC5haXJieXRlLmNvbS9hdXRoL3JlYWxtcy90ZXN0LWNvbXBhbnktMSIsImF1ZCI6Imh0dHBzOi8vZGV2LTItY2xvdWQuYWlyYnl0ZS5jb20vYXV0aC9yZWFsbXMvdGVzdC1jb21wYW55LTEiLCJzdWIiOiIwYWEzYmVhYi02MDU3LTQ4NjEtYWQ5MS0wNWZlM2U3NmJhMjIiLCJ0eXAiOiJSZWZyZXNoIiwiYXpwIjoiYWlyYnl0ZS13ZWJhcHAiLCJzZXNzaW9uX3N0YXRlIjoiZDBkMGU3YWEtYTVjOC00ODA2LTkzMDgtNDZlYTk4NGRmYTlkIiwic2NvcGUiOiJvcGVuaWQgZW1haWwgcHJvZmlsZSIsInNpZCI6ImQwZDBlN2FhLWE1YzgtNDgwNi05MzA4LTQ2ZWE5ODRkZmE5ZCJ9.WRqTUNC6eY00zkYbK3GGy-wbvON47rWJu6TgXGG1QvA",
        token_type: "Bearer",
        scope: "openid email profile",
        profile: {
          exp: 1695682322,
          iat: 1695682022,
          iss: "https://dev-2-cloud.airbyte.com/auth/realms/test-company-1",
          aud: "airbyte-webapp",
          sub: "0aa3beab-6057-4861-ad91-05fe3e76ba22",
          typ: "ID",
          session_state: "d0d0e7aa-a5c8-4806-9308-46ea984dfa9d",
          sid: "d0d0e7aa-a5c8-4806-9308-46ea984dfa9d",
          email_verified: true,
          name: "Joey Marshment-Howell",
          preferred_username: "admin-company-1",
          given_name: "Joey",
          family_name: "Marshment-Howell",
          email: "joseph+admin-company-1@airbyte.io",
        },
        expires_at: 1695682322,
      })
    );

    renderHook(() => useAuthService(), { wrapper });

    await waitFor(() => {
      expect(mockUserManager.settings.authority).toMatch(/auth\/realms\/local-storage-realm/);
    });
  });
});
