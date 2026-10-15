import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import {
  billingNotificationSettingsKeys,
  useGetBillingNotificationSettings,
  useUpdateBillingNotificationSettings,
} from "./billingNotificationSettings";
import { getBillingNotificationSettings, updateBillingNotificationSettings } from "../generated/AirbyteClient";
import { BillingNotificationSettingsRead } from "../types/AirbyteClient";

jest.mock("../generated/AirbyteClient", () => ({
  getBillingNotificationSettings: jest.fn(),
  updateBillingNotificationSettings: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

const mockGetSettings = getBillingNotificationSettings as jest.MockedFunction<typeof getBillingNotificationSettings>;
const mockUpdateSettings = updateBillingNotificationSettings as jest.MockedFunction<
  typeof updateBillingNotificationSettings
>;

const organizationId = "org-123";

const savedSettings: BillingNotificationSettingsRead = {
  organizationId,
  configured: true,
  recipientEmail: "billing@example.com",
  syncCreditThreshold: 100,
  invoiceAmountThreshold: 2000,
};

describe("billingNotificationSettings hooks", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  });

  afterEach(() => {
    queryClient.clear();
  });

  describe("useGetBillingNotificationSettings", () => {
    it("reads the settings of the given organization", async () => {
      mockGetSettings.mockResolvedValue(savedSettings);

      const { result } = renderHook(() => useGetBillingNotificationSettings(organizationId), { wrapper });

      await waitFor(() => expect(result.current.data).toEqual(savedSettings));
      expect(mockGetSettings).toHaveBeenCalledWith({ organizationId }, {});
    });
  });

  describe("useUpdateBillingNotificationSettings", () => {
    it("sends the organization id with the new settings", async () => {
      mockUpdateSettings.mockResolvedValue(savedSettings);

      const { result } = renderHook(() => useUpdateBillingNotificationSettings(organizationId), { wrapper });
      await act(async () => {
        await result.current.mutateAsync({
          recipientEmail: "billing@example.com",
          syncCreditThreshold: 100,
          invoiceAmountThreshold: 2000,
        });
      });

      expect(mockUpdateSettings).toHaveBeenCalledWith(
        {
          organizationId,
          recipientEmail: "billing@example.com",
          syncCreditThreshold: 100,
          invoiceAmountThreshold: 2000,
        },
        {}
      );
    });

    it("writes the saved settings into the read cache", async () => {
      mockUpdateSettings.mockResolvedValue(savedSettings);

      const { result } = renderHook(() => useUpdateBillingNotificationSettings(organizationId), { wrapper });
      await act(async () => {
        await result.current.mutateAsync({ syncCreditThreshold: 100, invoiceAmountThreshold: 2000 });
      });

      expect(queryClient.getQueryData(billingNotificationSettingsKeys.detail(organizationId))).toEqual(savedSettings);
    });

    it("leaves the read cache untouched when the save fails", async () => {
      mockUpdateSettings.mockRejectedValue(new Error("boom"));
      queryClient.setQueryData(billingNotificationSettingsKeys.detail(organizationId), {
        ...savedSettings,
        syncCreditThreshold: 5,
      });

      const { result } = renderHook(() => useUpdateBillingNotificationSettings(organizationId), { wrapper });
      await act(async () => {
        await expect(
          result.current.mutateAsync({ syncCreditThreshold: 100, invoiceAmountThreshold: 2000 })
        ).rejects.toThrow("boom");
      });

      expect(queryClient.getQueryData(billingNotificationSettingsKeys.detail(organizationId))).toEqual({
        ...savedSettings,
        syncCreditThreshold: 5,
      });
    });
  });
});
