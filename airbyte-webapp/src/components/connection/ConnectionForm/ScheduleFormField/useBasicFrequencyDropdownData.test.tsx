import { renderHook } from "@testing-library/react";

import { TestWrapper as wrapper } from "test-utils/testutils";

import { ConnectionScheduleTimeUnit } from "core/api/types/AirbyteClient";

import { useBasicFrequencyDropdownData, frequencyConfig } from "./useBasicFrequencyDropdownData";

describe("#useBasicFrequencyDropdownData", () => {
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
    expect(result.current).toContainEqual({
      label: "Every 7 minutes",
      value: { units: 7, timeUnit: "minutes" },
      "data-testid": "frequency-7-minutes",
    });
  });
});
