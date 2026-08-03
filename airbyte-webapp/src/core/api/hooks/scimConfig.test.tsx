import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

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
 * A getScimConfig response that never settles, used for the refetch the mutations trigger via
 * invalidateQueries.
 *
 * The token-containment cases assert that the cache holds no `token` after a mutation resolves. If
 * the refetch were allowed to settle with a token-free config, it would overwrite the cache and the
 * assertion would pass even for a hook that wrote the token there in onSuccess — the test could not
 * fail on the one thing it exists to catch. Leaving the refetch pending means the cache still holds
 * whatever the mutation path left behind at assertion time.
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

      const { result } = renderHook(() => useGetScimConfig(), { wrapper });

      expect(result.current.data).toBeUndefined();
      expect(mockGetScimConfig).not.toHaveBeenCalled();
    });

    it("calls getScimConfig({ organizationId }) and returns the config", async () => {
      mockGetScimConfig.mockResolvedValue(baseScimConfig);

      const { result } = renderHook(() => useGetScimConfig(), { wrapper });

      await waitFor(() => expect(result.current.data).toEqual(baseScimConfig));
      expect(mockGetScimConfig).toHaveBeenCalledWith({ organizationId }, {});
    });
  });

  describe("useEnableScim", () => {
    it("calls enableScim({ organizationId, idpProvider }) and invalidates the detail key", async () => {
      mockEnableScim.mockResolvedValue(baseScimConfig);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useEnableScim(), { wrapper });
      await result.current.mutateAsync(ScimIdpProvider.okta);

      expect(mockEnableScim).toHaveBeenCalledWith({ organizationId, idpProvider: ScimIdpProvider.okta }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(scimConfigKeys.detail(organizationId));
    });
  });

  describe("useRotateScimToken", () => {
    it("calls rotateScimToken({ organizationId }) and invalidates the detail key", async () => {
      mockRotateScimToken.mockResolvedValue(baseScimConfig);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useRotateScimToken(), { wrapper });
      await result.current.mutateAsync();

      expect(mockRotateScimToken).toHaveBeenCalledWith({ organizationId }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(scimConfigKeys.detail(organizationId));
    });
  });

  describe("useDisableScim", () => {
    it("calls disableScim({ organizationId }) and invalidates the detail key", async () => {
      mockDisableScim.mockResolvedValue(undefined);
      const invalidateQueriesSpy = jest.spyOn(queryClient, "invalidateQueries");

      const { result } = renderHook(() => useDisableScim(), { wrapper });
      await result.current.mutateAsync();

      expect(mockDisableScim).toHaveBeenCalledWith({ organizationId }, {});
      expect(invalidateQueriesSpy).toHaveBeenCalledWith(scimConfigKeys.detail(organizationId));
    });
  });

  describe("token containment", () => {
    it("keeps the enable-scim token out of the query cache while returning it from the mutation", async () => {
      mockGetScimConfig.mockResolvedValueOnce(baseScimConfig).mockReturnValueOnce(pendingScimConfig());
      mockEnableScim.mockResolvedValue({ ...baseScimConfig, token: "enable-secret-token" });

      const { result } = renderHook(
        () => ({
          query: useGetScimConfig(),
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
      mockGetScimConfig.mockResolvedValueOnce(baseScimConfig).mockReturnValueOnce(pendingScimConfig());
      mockRotateScimToken.mockResolvedValue({ ...baseScimConfig, token: "rotate-secret-token" });

      const { result } = renderHook(
        () => ({
          query: useGetScimConfig(),
          mutation: useRotateScimToken(),
        }),
        { wrapper }
      );

      await waitFor(() => expect(result.current.query.data).toEqual(baseScimConfig));

      await result.current.mutation.mutateAsync();

      // Wait for the invalidation-triggered refetch to be issued, but not to settle — it never
      // does, by construction. See pendingScimConfig.
      await waitFor(() => expect(mockGetScimConfig).toHaveBeenCalledTimes(2));

      expect(result.current.mutation.data?.token).toBe("rotate-secret-token");
      expect(
        queryClient.getQueryData<ScimConfigResponse>(scimConfigKeys.detail(organizationId))?.token
      ).toBeUndefined();
    });
  });
});
