import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { FormProvider, useForm } from "react-hook-form";

import { mocked, render } from "test-utils";

import { useActivateSsoConfig, useListDomainVerifications, useSSOConfigManagement } from "core/api";
import { useAuthService } from "core/services/auth";
import { useExperiment } from "core/services/Experiment";

import { SSOSettingsValidation } from "./SSOSettingsValidation";
import { isSsoTestCallback } from "./ssoTestUtils";
import { useSSOTestCallback } from "./useSSOTestCallback";
import { SSOFormValuesValidation } from "../UpdateSSOSettingsForm";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: () => "test-organization-id",
}));

// The core/api and core/services/auth barrels cannot be spread from jest.requireActual (their
// import graphs are circular and fail at module evaluation), so the factories list exports explicitly.
jest.mock("core/api", () => ({
  useActivateSsoConfig: jest.fn(),
  useListDomainVerifications: jest.fn(),
  useSSOConfigManagement: jest.fn(),
}));

jest.mock("core/services/auth", () => ({
  useAuthService: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: jest.fn(),
}));

jest.mock("./ssoTestUtils", () => ({
  isSsoTestCallback: jest.fn(),
}));

jest.mock("./useSSOTestCallback", () => ({
  useSSOTestCallback: jest.fn(),
}));

// useExperiment is generic over the experiment key, so a plain jest.Mock cast keeps
// per-key mockImplementation branching simple instead of fighting the overload types.
const mockUseExperiment = useExperiment as unknown as jest.Mock<boolean, [string]>;
const mockUseActivateSsoConfig = mocked(useActivateSsoConfig);
const mockUseListDomainVerifications = mocked(useListDomainVerifications);
const mockUseSSOConfigManagement = mocked(useSSOConfigManagement);
const mockUseAuthService = mocked(useAuthService);
const mockIsSsoTestCallback = mocked(isSsoTestCallback);
const mockUseSSOTestCallback = mocked(useSSOTestCallback);

const mockSsoConfigManagement = (ssoConfig: unknown, isLoading = false) => {
  mockUseSSOConfigManagement.mockReturnValue({ ssoConfig, isLoading } as unknown as ReturnType<
    typeof useSSOConfigManagement
  >);
};

const FormWrapper: React.FC<React.PropsWithChildren> = ({ children }) => {
  const methods = useForm<SSOFormValuesValidation>({
    defaultValues: { companyIdentifier: "", clientId: "", clientSecret: "", discoveryUrl: "" },
  });
  return <FormProvider {...methods}>{children}</FormProvider>;
};

const renderComponent = () =>
  render(
    <FormWrapper>
      <SSOSettingsValidation />
    </FormWrapper>
  );

describe("SSOSettingsValidation", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseExperiment.mockImplementation(() => false);
    mockIsSsoTestCallback.mockReturnValue(false);
    mockUseSSOTestCallback.mockReturnValue({ testResult: null, setTestResult: jest.fn() } as unknown as ReturnType<
      typeof useSSOTestCallback
    >);
    mockSsoConfigManagement(undefined);
    mockUseActivateSsoConfig.mockReturnValue({ mutateAsync: jest.fn() } as unknown as ReturnType<
      typeof useActivateSsoConfig
    >);
    mockUseListDomainVerifications.mockReturnValue({ data: undefined } as unknown as ReturnType<
      typeof useListDomainVerifications
    >);
    mockUseAuthService.mockReturnValue({ logout: jest.fn() } as unknown as ReturnType<typeof useAuthService>);
  });

  it("keeps the card closed when not returning from an SSO test", async () => {
    await renderComponent();

    expect(screen.getByText("Set up SSO")).toBeInTheDocument();
    expect(screen.queryByText("Step 1: Test your configuration")).not.toBeInTheDocument();
  });

  it("opens the card automatically when returning from an SSO test callback", async () => {
    mockIsSsoTestCallback.mockReturnValue(true);

    await renderComponent();

    expect(screen.getByText("Step 1: Test your configuration")).toBeInTheDocument();
    expect(screen.getByText("Step 2: Activate your configuration")).toBeInTheDocument();
  });

  it("shows a loading status icon while the SSO config is loading", async () => {
    mockSsoConfigManagement(undefined, true);

    const { container } = await renderComponent();

    expect(container.querySelector("[data-icon='loading']")).toBeInTheDocument();
    expect(screen.queryByText("Optional")).not.toBeInTheDocument();
  });

  it("shows a check status icon when the SSO config is active", async () => {
    mockSsoConfigManagement({ status: "active" });

    const { container } = await renderComponent();

    expect(container.querySelector("[data-icon='check']")).toBeInTheDocument();
    expect(screen.queryByText("Optional")).not.toBeInTheDocument();
  });

  it("shows the Optional status when SSO is not configured", async () => {
    await renderComponent();

    expect(screen.getByText("Optional")).toBeInTheDocument();
  });

  it("disables the activate button until step 1 is complete", async () => {
    mockIsSsoTestCallback.mockReturnValue(true);
    mockSsoConfigManagement({ status: "draft" });

    await renderComponent();

    expect(screen.getByRole("button", { name: "Activate" })).toBeDisabled();
  });

  it("enables the activate button once step 1 is complete and an email domain is entered", async () => {
    mockIsSsoTestCallback.mockReturnValue(true);
    mockSsoConfigManagement({ status: "draft", clientId: "client-id", clientSecret: "client-secret" });

    await renderComponent();

    const activateButton = screen.getByRole("button", { name: "Activate" });
    expect(activateButton).toBeDisabled();

    await userEvent.type(screen.getByPlaceholderText("example.com"), "example.com");

    expect(activateButton).toBeEnabled();
  });
});
