import { renderHook } from "@testing-library/react";

import { mockConnection } from "test-utils/mock-data/mockConnection";
import { mockDestinationDefinitionVersion } from "test-utils/mock-data/mockDestination";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { useInitialFormValues } from "./formConfig";

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
}));

describe("#useInitialFormValues", () => {
  it("should generate initial values w/ no 'not create' mode", () => {
    const { result } = renderHook(() => useInitialFormValues(mockConnection, mockDestinationDefinitionVersion));
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should generate initial values w/ 'not create' mode: false", () => {
    const { result } = renderHook(() => useInitialFormValues(mockConnection, mockDestinationDefinitionVersion, false));
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should generate initial values w/ 'not create' mode: true", () => {
    const { result } = renderHook(() => useInitialFormValues(mockConnection, mockDestinationDefinitionVersion, true));
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  // This is a low-priority test
  it.todo(
    "should test for supportsDbt+initialValues.transformations and supportsNormalization+initialValues.normalization"
  );
});
