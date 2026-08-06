import { screen } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { useScimSettingsAccess } from "area/organization/utils";
import { useGetScimConfig } from "core/api";
import { useExperiment } from "core/services/Experiment";

import { SSOAndScimOrganizationSettingsPage } from "./SSOAndScimOrganizationSettingsPage";

// PLAT-1013 chrome must gate on useExperiment alone and never reach for the SCIM config API.
// Both SCIM data entry points are replaced with jest.fn()s so the tests below can assert the
// page shell never invokes either of them. The factories cannot spread jest.requireActual (the
// core/api import graph is circular and fails at module evaluation), so they list exports explicitly.
jest.mock("core/api", () => ({
  useGetScimConfig: jest.fn(),
}));

jest.mock("area/organization/utils", () => ({
  useScimSettingsAccess: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: jest.fn(),
}));

jest.mock("pages/SettingsPage/components/DomainVerification", () => ({
  DomainVerificationSection: () => <div data-testid="domain-verification-section" />,
}));

jest.mock("pages/SettingsPage/components/ScimSettingsCard", () => ({
  ScimSettingsCard: () => <div data-testid="scim-settings-card" />,
}));

jest.mock("pages/SettingsPage/UpdateSSOSettingsForm", () => ({
  UpdateSSOSettingsForm: () => <div data-testid="update-sso-settings-form" />,
}));

// useExperiment is generic over the experiment key, so a plain jest.Mock cast keeps
// per-key mockImplementation branching simple instead of fighting the overload types.
const mockUseExperiment = useExperiment as unknown as jest.Mock<boolean, [string]>;
const mockUseGetScimConfig = mocked(useGetScimConfig);
const mockUseScimSettingsAccess = mocked(useScimSettingsAccess);

const renderPage = () => render(<SSOAndScimOrganizationSettingsPage />);

describe("SSOAndScimOrganizationSettingsPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseExperiment.mockImplementation(() => false);
  });

  it("renders the plain SSO heading when settings.scimProvisioning is off", async () => {
    await renderPage();

    expect(screen.getByRole("heading", { name: "Single Sign-On (SSO)" })).toBeInTheDocument();
    expect(screen.queryByText("Single Sign-On (SSO) and SCIM")).not.toBeInTheDocument();
    expect(screen.getByTestId("update-sso-settings-form")).toBeInTheDocument();
  });

  it("renders the SSO and SCIM heading when settings.scimProvisioning is on", async () => {
    mockUseExperiment.mockImplementation((key) => key === "settings.scimProvisioning");

    await renderPage();

    expect(screen.getByRole("heading", { name: "Single Sign-On (SSO) and SCIM" })).toBeInTheDocument();
  });

  it("does not render the domain verification section when its experiment is off", async () => {
    await renderPage();

    expect(screen.queryByTestId("domain-verification-section")).not.toBeInTheDocument();
  });

  it("renders the domain verification section when its experiment is on", async () => {
    mockUseExperiment.mockImplementation((key) => key === "settings.domainVerification");

    await renderPage();

    expect(screen.getByTestId("domain-verification-section")).toBeInTheDocument();
  });

  it("does not render the SCIM settings card when settings.scimProvisioning is off", async () => {
    await renderPage();

    expect(screen.queryByTestId("scim-settings-card")).not.toBeInTheDocument();
    expect(mockUseGetScimConfig).not.toHaveBeenCalled();
    expect(mockUseScimSettingsAccess).not.toHaveBeenCalled();
  });

  it("renders the SCIM settings card when settings.scimProvisioning is on, with no SCIM API call from the page shell", async () => {
    mockUseExperiment.mockImplementation((key) => key === "settings.scimProvisioning");

    await renderPage();

    // Card-shell assertions (collapsed disclosure, label, docs link) live in ScimSettingsCard.test.tsx,
    // which owns the CollapsibleSettingsCard. The page shell must stay hook-free: these negative
    // assertions prove neither SCIM hook is reached directly from the page.
    expect(screen.getByTestId("scim-settings-card")).toBeInTheDocument();
    expect(mockUseGetScimConfig).not.toHaveBeenCalled();
    expect(mockUseScimSettingsAccess).not.toHaveBeenCalled();
  });
});
