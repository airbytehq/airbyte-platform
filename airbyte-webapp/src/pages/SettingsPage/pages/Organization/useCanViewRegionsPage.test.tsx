import { renderHook } from "@testing-library/react";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useExperiment } from "core/services/Experiment";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { useCanViewRegionsPage } from "./useCanViewRegionsPage";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: jest.fn(),
}));

jest.mock("core/services/features", () => ({
  FeatureItem: { SelfManagedRegions: "SelfManagedRegions" },
  useFeature: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: { ManageDataplanes: "ManageDataplanes" },
  useGeneratedIntent: jest.fn(),
}));

const mockUseExperiment = useExperiment as jest.MockedFunction<typeof useExperiment>;
const mockUseFeature = useFeature as jest.MockedFunction<typeof useFeature>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const setup = ({ flag, feature, intent }: { flag: boolean; feature: boolean; intent: boolean }) => {
  (useCurrentOrganizationId as jest.Mock).mockReturnValue("org-1");
  mockUseExperiment.mockImplementation((key) => key === "platform.dataplane-health-ui" && flag);
  mockUseFeature.mockImplementation((item) => item === FeatureItem.SelfManagedRegions && feature);
  mockUseGeneratedIntent.mockImplementation((intentName) => intentName === Intent.ManageDataplanes && intent);
};

describe("useCanViewRegionsPage", () => {
  it("allows a data plane manager with the flag and the feature on", () => {
    setup({ flag: true, feature: true, intent: true });

    expect(renderHook(() => useCanViewRegionsPage()).result.current).toBe(true);
  });

  it.each([
    ["the flag is off", { flag: false, feature: true, intent: true }],
    ["the organization has no self-managed regions", { flag: true, feature: false, intent: true }],
    ["the user cannot manage data planes", { flag: true, feature: true, intent: false }],
  ])("denies when %s", (_, state) => {
    setup(state);

    expect(renderHook(() => useCanViewRegionsPage()).result.current).toBe(false);
  });
});
