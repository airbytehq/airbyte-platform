import { renderHook } from "@testing-library/react";

import { mockConnection } from "test-utils/mock-data/mockConnection";
import {
  mockDestinationDefinitionSpecification,
  mockDestinationDefinitionVersion,
} from "test-utils/mock-data/mockDestination";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";
import { TestWrapper as wrapper } from "test-utils/testutils";

import { NormalizationType } from "area/connection/types";
import { ConnectionScheduleTimeUnit, OperationRead } from "core/request/AirbyteClient";

import { mapFormPropsToOperation, useFrequencyDropdownData, useInitialValues } from "./formConfig";
import { frequencyConfig } from "./frequencyConfig";

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
}));

describe("#useFrequencyDropdownData", () => {
  it("should return only default frequencies when no additional frequency is provided", () => {
    const { result } = renderHook(() => useFrequencyDropdownData(undefined), { wrapper });
    expect(result.current.map((item) => item.value)).toEqual(["manual", "cron", ...frequencyConfig]);
  });

  it("should return only default frequencies when additional frequency is already present", () => {
    const additionalFrequency = {
      basicSchedule: {
        units: 1,
        timeUnit: ConnectionScheduleTimeUnit.hours,
      },
    };
    const { result } = renderHook(() => useFrequencyDropdownData(additionalFrequency), { wrapper });
    expect(result.current.map((item) => item.value)).toEqual(["manual", "cron", ...frequencyConfig]);
  });

  it("should include additional frequency when provided and unique", () => {
    const additionalFrequency = {
      basicSchedule: {
        units: 7,
        timeUnit: ConnectionScheduleTimeUnit.minutes,
      },
    };
    const { result } = renderHook(() => useFrequencyDropdownData(additionalFrequency), { wrapper });

    // +1 for additionalFrequency, +2 for cron and manual frequencies
    expect(result.current.length).toEqual(frequencyConfig.length + 1 + 2);
    expect(result.current).toContainEqual({ label: "Every 7 minutes", value: { units: 7, timeUnit: "minutes" } });
  });
});

describe("#mapFormPropsToOperation", () => {
  const workspaceId = "asdf";
  const normalization: OperationRead = {
    workspaceId,
    operationId: "asdf",
    name: "asdf",
    operatorConfiguration: {
      operatorType: "normalization",
    },
  };
  const dbtCloudJob: OperationRead = {
    workspaceId,
    operationId: "testDbtCloudJob",
    name: "testDbtCloudJob",
    operatorConfiguration: {
      operatorType: "webhook",
    },
  };

  it("should add any included transformations", () => {
    expect(
      mapFormPropsToOperation(
        {
          transformations: [normalization, dbtCloudJob],
        },
        undefined,
        "asdf"
      )
    ).toEqual([normalization, dbtCloudJob]);
  });

  it("should add a basic normalization if normalization is set to basic", () => {
    expect(
      mapFormPropsToOperation(
        {
          normalization: NormalizationType.basic,
        },

        undefined,
        workspaceId
      )
    ).toEqual([
      {
        name: "Normalization",
        operatorConfiguration: {
          normalization: {
            option: "basic",
          },
          operatorType: "normalization",
        },
        workspaceId,
      },
    ]);
  });

  it("should include any provided initial operations and not include the basic normalization operation when normalization type is basic", () => {
    expect(
      mapFormPropsToOperation(
        {
          normalization: NormalizationType.basic,
        },
        [normalization],
        workspaceId
      )
    ).toEqual([normalization]);
  });

  it("should only include webhook operations from initial operations and not include the basic normalization operation when normalization type is raw", () => {
    expect(
      mapFormPropsToOperation(
        {
          normalization: NormalizationType.raw,
        },
        [normalization],
        workspaceId
      )
    ).toEqual([]);

    expect(
      mapFormPropsToOperation(
        {
          normalization: NormalizationType.raw,
        },
        [normalization, dbtCloudJob],
        workspaceId
      )
    ).toEqual([dbtCloudJob]);
  });

  it("should include provided transformations when normalization type is raw, but not any provided normalizations", () => {
    expect(
      mapFormPropsToOperation(
        {
          normalization: NormalizationType.raw,
          transformations: [normalization],
        },
        [normalization],
        workspaceId
      )
    ).toEqual([normalization]);
  });

  it("should include provided transformations and normalizations when normalization type is basic", () => {
    expect(
      mapFormPropsToOperation(
        {
          normalization: NormalizationType.basic,
          transformations: [normalization],
        },
        [normalization],
        workspaceId
      )
    ).toEqual([normalization, normalization]);
  });

  it("should include provided transformations and default normalization when normalization type is basic and no normalizations have been provided", () => {
    expect(
      mapFormPropsToOperation(
        {
          normalization: NormalizationType.basic,
          transformations: [normalization],
        },
        undefined,
        workspaceId
      )
    ).toEqual([
      {
        name: "Normalization",
        operatorConfiguration: {
          normalization: {
            option: "basic",
          },
          operatorType: "normalization",
        },
        workspaceId,
      },
      normalization,
    ]);
  });
});

describe("#useInitialValues", () => {
  it("should generate initial values w/ no 'not create' mode", () => {
    const { result } = renderHook(() =>
      useInitialValues(mockConnection, mockDestinationDefinitionVersion, mockDestinationDefinitionSpecification)
    );
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should generate initial values w/ 'not create' mode: false", () => {
    const { result } = renderHook(() =>
      useInitialValues(mockConnection, mockDestinationDefinitionVersion, mockDestinationDefinitionSpecification, false)
    );
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should generate initial values w/ 'not create' mode: true", () => {
    const { result } = renderHook(() =>
      useInitialValues(mockConnection, mockDestinationDefinitionVersion, mockDestinationDefinitionSpecification, true)
    );
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeUndefined();
  });

  // This is a low-priority test
  it.todo(
    "should test for supportsDbt+initialValues.transformations and supportsNormalization+initialValues.normalization"
  );
});
