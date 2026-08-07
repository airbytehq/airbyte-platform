import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook } from "@testing-library/react";
import { ReactNode } from "react";

import { useSetupInstanceConfiguration } from "./instanceConfiguration";
import { setupInstanceConfiguration } from "../generated/AirbyteClient";

jest.mock("../generated/AirbyteClient", () => ({
  setupInstanceConfiguration: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

const mockSetupInstanceConfiguration = setupInstanceConfiguration as jest.MockedFunction<
  typeof setupInstanceConfiguration
>;

describe("useSetupInstanceConfiguration", () => {
  it("omits UI-only security check state from the setup request", async () => {
    const queryClient = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    mockSetupInstanceConfiguration.mockResolvedValue({} as never);
    const { result } = renderHook(() => useSetupInstanceConfiguration(), { wrapper });

    const setupFormValues = {
      email: "test@example.com",
      anonymousDataCollection: false,
      initialSetupComplete: true,
      displaySetupWizard: false,
      organizationName: "Test organization",
      securityCheck: "succeeded",
    };
    await result.current.mutateAsync(setupFormValues);

    expect(mockSetupInstanceConfiguration).toHaveBeenCalledWith(
      {
        email: "test@example.com",
        anonymousDataCollection: false,
        initialSetupComplete: true,
        displaySetupWizard: false,
        organizationName: "Test organization",
      },
      {}
    );

    queryClient.clear();
  });
});
