import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { organizationKeys } from "./organizations";
import { scimConfigKeys, useDisableScim, useEnableScim, useGetScimConfig, useRotateScimToken } from "./scimConfig";
import { disableScim, enableScim, getScimConfig, rotateScimToken } from "../generated/AirbyteClient";
import { ScimConfigResponse, ScimConfigStatus, ScimIdpProvider } from "../types/AirbyteClient";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
  getScimConfig: jest.fn(),
  enableScim: jest.fn(),
  rotateScimToken: jest.fn(),
  disableScim: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockGetScimConfig = getScimConfig as jest.MockedFunction<typeof getScimConfig>;
const mockEnableScim = enableScim as jest.MockedFunction<typeof enableScim>;
const mockRotateScimToken = rotateScimToken as jest.MockedFunction<typeof rotateScimToken>;
const mockDisableScim = disableScim as jest.MockedFunction<typeof disableScim>;

const organizationId = "org-123";

const baseScimConfig: ScimConfigResponse = {
  status: ScimConfigStatus.enabled,
  scimBaseUrl: "https://airbyte.example.com/scim/v2",
  available: true,
};

/**
 * A getScimConfig response that never settles, used for the refetch that useEnableScim's
 * fire-and-forget onSuccess triggers via invalidateQueries without awaiting it.
 *
 * The enable-scim token-containment case below asserts that the cache holds no `token` after the
 * mutation resolves. Because onSuccess doesn't await the refetch, mutateAsync resolves regardless of
 * whether it ever settles. If the refetch were allowed to settle with a token-free config, it would
 * overwrite the cache and the assertion would pass even for a hook that wrote the token there in
 * onSuccess — the test could not fail on the one thing it exists to catch. Leaving the refetch
 * pending means the cache still holds whatever the mutation path left behind at assertion time.
 *
 * useRotateScimToken awaits the refetch in onSuccess, so its token-containment case below uses a
 * controllable promise instead — mutateAsync can't resolve until that refetch settles.
 */
const pendingScimConfig = () => new Promise<ScimConfigResponse>(() => {});

describe("scimConfig hooks", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    mockUseCurrentOrganizationId.mockReturnValue(organizationId);
  });

  afterEach(() => {
    queryClient.clear();
  });

  describe("useGetScimConfig", () => {
    it("does not call getScimConfig when no organization id is available", () => {
      mockUseCurrentOrganizationId.mockReturnValue(undefined as unknown as string);

      const { result } = renderHook(() => useGetScimConfig({ enabled: true }), { wrapper });

      expect(result.current.data).toBeUndefined();
      expect(mockGetScimConfig).not.toHaveBeenCalled();
    });

    it("calls getScimConfig({ organizationId }) and returns the config", async () => {
      mockGetScimConfig.mockResolvedValue(baseScimConfig);

      const { result } = renderHook(() => useGetScimConfig({ enabled: true }), { wrapper });

      await waitFor(() => expect(result.current.data).toEqual(baseScimConfig));
      expect(mockGetScimConfig).toHaveBeenCalledWith({ organizationId }, {});
    });

    it("does not call getScimConfig when enabled is false even with an organization id", () => {
      const { result } = renderHook(() => useGetScimConfig({ enabled: false }), { wrapper });

      expect(result.current.data).toBeUndefined();
      expect(mockGetScimConfig).not.toHaveBeenCalled();
    });

    it("fires the query once enabled flips to true after mount, matching the flag/permissions-resolve-async gate", async () => {
      mockGetScimConfig.mockResolvedValue(baseScimConfig);

      const { result, rerender } = renderHook(({ enabled }) => useGetScimConfig({ enabled }), {
        wrapper,
        initialProps: { enabled: false },
      });

      expect(result.current.data).toBeUndefined();
      expect(mockGetScimConfig).not.toHaveBeenCalled();

      rerender({ enabled: true });

      await waitFor(() => expect(result.current.data).toEqual(baseScimConfig));
      expect(mockGetScimConfig).toHaveBeenCalledWith({ organizationId }, {});
    });
  });

  describe("useEnableScim", () => {
    it("calls enableScim({ organizationId, idpProvider }) and invalidates the detail and org info keys", async () => {
      mockEnableScim.mockResolvedValue(baseScimConfig);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useEnableScim(), { wrapper });
      await result.current.mutateAsync(ScimIdpProvider.okta);

      expect(mockEnableScim).toHaveBeenCalledWith({ organizationId, idpProvider: ScimIdpProvider.okta }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(scimConfigKeys.detail(organizationId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(organizationKeys.info(organizationId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(organizationKeys.orgInfo(organizationId));
    });
  });

  describe("useRotateScimToken", () => {
    it("calls rotateScimToken({ organizationId }) and invalidates the detail and org info keys", async () => {
      mockRotateScimToken.mockResolvedValue(baseScimConfig);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useRotateScimToken(), { wrapper });
      await result.current.mutateAsync();

      expect(mockRotateScimToken).toHaveBeenCalledWith({ organizationId }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(scimConfigKeys.detail(organizationId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(organizationKeys.info(organizationId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(organizationKeys.orgInfo(organizationId));
    });

    it("does not resolve mutateAsync until the invalidated config refetch settles", async () => {
      mockGetScimConfig.mockResolvedValueOnce(baseScimConfig);
      mockRotateScimToken.mockResolvedValue(baseScimConfig);

      // A controllable stand-in for the invalidation-triggered refetch, so the test can assert
      // mutateAsync is still pending before letting it settle.
      let resolveRefetch: ((value: ScimConfigResponse) => void) | undefined;
      mockGetScimConfig.mockReturnValueOnce(
        new Promise<ScimConfigResponse>((resolve) => {
          resolveRefetch = resolve;
        })
      );

      const { result } = renderHook(
        () => ({
          query: useGetScimConfig({ enabled: true }),
          mutation: useRotateScimToken(),
        }),
        { wrapper }
      );

      await waitFor(() => expect(result.current.query.data).toEqual(baseScimConfig));

      let settled = false;
      const mutatePromise = result.current.mutation.mutateAsync().then(() => {
        settled = true;
      });

      // The refetch has been issued but hasn't settled yet: mutateAsync must still be pending,
      // otherwise the one-time credential modal could open before the cache reflects the rotation.
      await waitFor(() => expect(mockGetScimConfig).toHaveBeenCalledTimes(2));
      expect(settled).toBe(false);

      resolveRefetch?.(baseScimConfig);
      await mutatePromise;

      expect(settled).toBe(true);
    });

    it("still resolves mutateAsync when the invalidation-triggered refetch fails", async () => {
      mockGetScimConfig.mockResolvedValueOnce(baseScimConfig).mockRejectedValueOnce(new Error("refetch failed"));
      mockRotateScimToken.mockResolvedValue({ ...baseScimConfig, token: "rotate-secret-token" });

      const { result } = renderHook(
        () => ({
          query: useGetScimConfig({ enabled: true }),
          mutation: useRotateScimToken(),
        }),
        { wrapper }
      );

      await waitFor(() => expect(result.current.query.data).toEqual(baseScimConfig));

      // A refetch failure after a successful rotation must not reject mutateAsync: the old token is
      // already invalidated server-side, and a rejection here would route the card into its error
      // toast instead of the one-time credential modal, destroying the token.
      const data = await result.current.mutation.mutateAsync();

      expect(data.token).toBe("rotate-secret-token");
    });
  });

  describe("useDisableScim", () => {
    it("calls disableScim({ organizationId }) and invalidates the detail and org info keys", async () => {
      mockDisableScim.mockResolvedValue(undefined);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useDisableScim(), { wrapper });
      await result.current.mutateAsync();

      expect(mockDisableScim).toHaveBeenCalledWith({ organizationId }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(scimConfigKeys.detail(organizationId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(organizationKeys.info(organizationId));
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(organizationKeys.orgInfo(organizationId));
    });
  });

  describe("token containment", () => {
    it("keeps the enable-scim token out of the query cache while returning it from the mutation", async () => {
      mockGetScimConfig.mockResolvedValueOnce(baseScimConfig).mockReturnValueOnce(pendingScimConfig());
      mockEnableScim.mockResolvedValue({ ...baseScimConfig, token: "enable-secret-token" });

      const { result } = renderHook(
        () => ({
          query: useGetScimConfig({ enabled: true }),
          mutation: useEnableScim(),
        }),
        { wrapper }
      );

      await waitFor(() => expect(result.current.query.data).toEqual(baseScimConfig));

      await result.current.mutation.mutateAsync(ScimIdpProvider.okta);

      // Wait for the invalidation-triggered refetch to be issued, but not to settle — it never
      // does, by construction. See pendingScimConfig.
      await waitFor(() => expect(mockGetScimConfig).toHaveBeenCalledTimes(2));

      expect(result.current.mutation.data?.token).toBe("enable-secret-token");
      expect(
        queryClient.getQueryData<ScimConfigResponse>(scimConfigKeys.detail(organizationId))?.token
      ).toBeUndefined();
    });

    it("keeps the rotate-token response out of the query cache while returning it from the mutation", async () => {
      mockGetScimConfig.mockResolvedValueOnce(baseScimConfig);
      mockRotateScimToken.mockResolvedValue({ ...baseScimConfig, token: "rotate-secret-token" });

      // useRotateScimToken awaits the refetch in onSuccess, so mutateAsync won't resolve until it
      // settles — a controllable promise (rather than a never-settling one) lets the test check the
      // cache mid-flight, before letting the refetch resolve.
      let resolveRefetch: ((value: ScimConfigResponse) => void) | undefined;
      mockGetScimConfig.mockReturnValueOnce(
        new Promise<ScimConfigResponse>((resolve) => {
          resolveRefetch = resolve;
        })
      );

      const { result } = renderHook(
        () => ({
          query: useGetScimConfig({ enabled: true }),
          mutation: useRotateScimToken(),
        }),
        { wrapper }
      );

      await waitFor(() => expect(result.current.query.data).toEqual(baseScimConfig));

      const mutatePromise = result.current.mutation.mutateAsync();

      // Wait for the invalidation-triggered refetch to be issued, but not to settle yet.
      await waitFor(() => expect(mockGetScimConfig).toHaveBeenCalledTimes(2));
      expect(
        queryClient.getQueryData<ScimConfigResponse>(scimConfigKeys.detail(organizationId))?.token
      ).toBeUndefined();

      resolveRefetch?.(baseScimConfig);
      const data = await mutatePromise;

      expect(data.token).toBe("rotate-secret-token");
      expect(
        queryClient.getQueryData<ScimConfigResponse>(scimConfigKeys.detail(organizationId))?.token
      ).toBeUndefined();
    });
  });
});
