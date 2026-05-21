import { renderHook } from "@testing-library/react";

import { TestWrapper as wrapper } from "test-utils/testutils";

import { ConnectionScheduleTimeUnit } from "core/api/types/AirbyteClient";

import { useBasicFrequencyDropdownData, frequencyConfig } from "./useBasicFrequencyDropdownData";

const mockUseFeature = jest.fn();
const mockUseOrganizationPlan = jest.fn();

jest.mock("core/services/features", () => ({
  ...jest.requireActual("core/services/features"),
  useFeature: (...args: unknown[]) => mockUseFeature(...args),
}));

jest.mock("area/organization/utils", () => ({
  ...jest.requireActual("area/organization/utils"),
  useOrganizationPlan: () => mockUseOrganizationPlan(),
}));

describe("#useBasicFrequencyDropdownData", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseFeature.mockReturnValue(false);
    mockUseOrganizationPlan.mockReturnValue({
      isUnifiedTrialPlan: false,
      isStandardTrialPlan: false,
      isStandardPlan: false,
    });
  });

  it("should return only default frequencies when no additional frequency is provided", () => {
    const { result } = renderHook(() => useBasicFrequencyDropdownData(undefined), { wrapper });

    expect(result.current.map((item) => item.value)).toEqual(frequencyConfig);
  });

  it("should return only default frequencies when additional frequency is already present", () => {
    const additionalFrequency = {
      basicSchedule: {
        units: 1,
        timeUnit: ConnectionScheduleTimeUnit.hours,
      },
    };
    const { result } = renderHook(() => useBasicFrequencyDropdownData(additionalFrequency), { wrapper });
    expect(result.current.map((item) => item.value)).toEqual(frequencyConfig);
  });

  it("should include additional frequency when provided and unique", () => {
    const additionalFrequency = {
      basicSchedule: {
        units: 7,
        timeUnit: ConnectionScheduleTimeUnit.minutes,
      },
    };
    const { result } = renderHook(() => useBasicFrequencyDropdownData(additionalFrequency), { wrapper });

    // +1 for additionalFrequency
    expect(result.current.length).toEqual(frequencyConfig.length + 1);
    expect(result.current).toContainEqual(
      expect.objectContaining({
        value: { units: 7, timeUnit: "minutes" },
        "data-testid": "frequency-7-minutes",
      })
    );
  });

  it("should include 15 and 30 minute options when feature flag is enabled", () => {
    mockUseFeature.mockReturnValue(true);

    const { result } = renderHook(() => useBasicFrequencyDropdownData(undefined), { wrapper });

    // Should have 2 more options than default frequencyConfig (15 and 30 minutes)
    expect(result.current.length).toEqual(frequencyConfig.length + 2);

    // First two should be 15 and 30 minute options
    expect(result.current[0].value).toEqual({ units: 15, timeUnit: "minutes" });
    expect(result.current[1].value).toEqual({ units: 30, timeUnit: "minutes" });
  });

  it("marks minute options as available in Plus and Pro for trial plans", () => {
    mockUseFeature.mockReturnValue(true);
    mockUseOrganizationPlan.mockReturnValue({
      isUnifiedTrialPlan: true,
      isStandardTrialPlan: false,
      isStandardPlan: false,
    });

    const { result } = renderHook(() => useBasicFrequencyDropdownData(undefined), { wrapper });

    expect(result.current[0]).toEqual(expect.objectContaining({ availableInPlans: ["plus", "pro"] }));
    expect(result.current[1]).toEqual(expect.objectContaining({ availableInPlans: ["plus", "pro"] }));
  });

  it("marks an existing minute option as available in Plus and Pro for standard plans", () => {
    mockUseOrganizationPlan.mockReturnValue({
      isUnifiedTrialPlan: false,
      isStandardTrialPlan: false,
      isStandardPlan: true,
    });
    const additionalFrequency = {
      basicSchedule: {
        units: 15,
        timeUnit: ConnectionScheduleTimeUnit.minutes,
      },
    };

    const { result } = renderHook(() => useBasicFrequencyDropdownData(additionalFrequency), { wrapper });

    expect(result.current.at(-1)).toEqual(expect.objectContaining({ availableInPlans: ["plus", "pro"] }));
  });
});
