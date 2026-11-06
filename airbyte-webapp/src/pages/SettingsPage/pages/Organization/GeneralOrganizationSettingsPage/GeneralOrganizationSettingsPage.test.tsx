import { screen } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { useExperiment } from "core/services/Experiment";
import { FeatureItem, useFeature } from "core/services/features";

import { GeneralOrganizationSettingsPage } from "./GeneralOrganizationSettingsPage";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: () => "org-1",
}));

jest.mock("core/services/analytics", () => ({
  PageTrackingCodes: { SETTINGS_ORGANIZATION: "Settings.Organization" },
  useTrackPage: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: jest.fn(),
}));

// The test wrapper renders FeatureService from the same module, so the mock keeps a pass-through for it.
jest.mock("core/services/features", () => ({
  FeatureService: ({ children }: React.PropsWithChildren) => children,
  defaultOssFeatures: [],
  FeatureItem: {
    AllowChangeDataplanes: "ALLOW_CHANGE_DATAPLANES",
    DiagnosticsExport: "DIAGNOSTICS_EXPORT",
    SelfManagedRegions: "SELF_MANAGED_REGIONS",
  },
  useFeature: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  useIntent: () => false,
}));

jest.mock("pages/SettingsPage/components/DiagnosticButton", () => ({
  DiagnosticsButton: () => <div data-testid="diagnostics-button" />,
}));

jest.mock("pages/SettingsPage/components/NestedRegionsView", () => ({
  NestedRegionsView: () => <div data-testid="nested-regions-view" />,
}));

jest.mock("pages/SettingsPage/components/RegionsTable", () => ({
  RegionsTable: () => <div data-testid="regions-table" />,
}));

jest.mock("pages/SettingsPage/UpdateOrganizationSettingsForm", () => ({
  UpdateOrganizationSettingsForm: () => <div data-testid="update-organization-settings-form" />,
}));

const mockUseExperiment = useExperiment as unknown as jest.Mock<boolean, [string]>;
const mockUseFeature = mocked(useFeature);

const setFeatures = (features: FeatureItem[]) =>
  mockUseFeature.mockImplementation((feature) => features.includes(feature));

describe("GeneralOrganizationSettingsPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseExperiment.mockImplementation(() => false);
  });

  describe("with the data plane health UI flag off", () => {
    it("renders the nested regions view for a self-managed regions organization", async () => {
      setFeatures([FeatureItem.AllowChangeDataplanes, FeatureItem.SelfManagedRegions]);

      await render(<GeneralOrganizationSettingsPage />);

      expect(screen.getByTestId("nested-regions-view")).toBeInTheDocument();
      expect(screen.queryByTestId("regions-table")).not.toBeInTheDocument();
    });

    it("renders the regions table for a Pro organization", async () => {
      setFeatures([FeatureItem.AllowChangeDataplanes]);

      await render(<GeneralOrganizationSettingsPage />);

      expect(screen.getByTestId("regions-table")).toBeInTheDocument();
    });
  });

  describe("with the data plane health UI flag on", () => {
    beforeEach(() => {
      mockUseExperiment.mockImplementation((key) => key === "platform.dataplane-health-ui");
    });

    it("renders no regions section for a self-managed regions organization", async () => {
      setFeatures([FeatureItem.AllowChangeDataplanes, FeatureItem.SelfManagedRegions]);

      await render(<GeneralOrganizationSettingsPage />);

      expect(screen.queryByTestId("nested-regions-view")).not.toBeInTheDocument();
      expect(screen.queryByTestId("regions-table")).not.toBeInTheDocument();
    });

    it("keeps the regions table for a Pro organization", async () => {
      setFeatures([FeatureItem.AllowChangeDataplanes]);

      await render(<GeneralOrganizationSettingsPage />);

      expect(screen.getByTestId("regions-table")).toBeInTheDocument();
    });
  });

  it("renders no regions section when the organization cannot change data planes", async () => {
    setFeatures([]);

    await render(<GeneralOrganizationSettingsPage />);

    expect(screen.queryByTestId("nested-regions-view")).not.toBeInTheDocument();
    expect(screen.queryByTestId("regions-table")).not.toBeInTheDocument();
  });
});
