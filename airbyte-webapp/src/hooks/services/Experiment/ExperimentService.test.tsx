import { act, renderHook } from "@testing-library/react";
import React from "react";
import { EMPTY, Subject } from "rxjs";

import { Experiments, defaultExperimentValues } from "./experiments";
import { ExperimentProvider, ExperimentService, useExperiment } from "./ExperimentService";

type TestExperimentValueType = Experiments["connector.airbyteCloudIpAddresses"];

const TEST_EXPERIMENT_KEY = "connector.airbyteCloudIpAddresses";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const getExperiment: ExperimentService["getExperiment"] = (key): any => {
  if (key === TEST_EXPERIMENT_KEY) {
    return "10.0.0.0,10.1.0.0";
  }
  throw new Error(`${key} not mocked for testing`);
};

const addContext = jest.fn();
const removeContext = jest.fn();

describe("ExperimentService", () => {
  describe("useExperiment", () => {
    afterEach(() => {
      jest.restoreAllMocks();
    });

    it("should return the value from the ExperimentService if provided", () => {
      const wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
        <ExperimentProvider
          value={{
            addContext,
            removeContext,
            getExperiment,
            getExperimentChanges$: () => EMPTY,
            getAllExperiments: () => ({}),
          }}
        >
          {children}
        </ExperimentProvider>
      );
      const { result } = renderHook(() => useExperiment(TEST_EXPERIMENT_KEY), { wrapper });
      expect(result.current).toEqual("10.0.0.0,10.1.0.0");
    });

    it("should return the defaultValue if ExperimentService provides undefined", () => {
      jest.replaceProperty(defaultExperimentValues, "connector.airbyteCloudIpAddresses", "10.42.0.0");
      const wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
        <ExperimentProvider
          value={{
            addContext,
            removeContext,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            getExperiment: () => undefined as any,
            getExperimentChanges$: () => EMPTY,
            getAllExperiments: () => ({}),
          }}
        >
          {children}
        </ExperimentProvider>
      );
      const { result } = renderHook(() => useExperiment(TEST_EXPERIMENT_KEY), { wrapper });
      expect(result.current).toEqual("10.42.0.0");
    });

    it("should return the default value if no ExperimentService is provided", () => {
      jest.replaceProperty(defaultExperimentValues, "connector.airbyteCloudIpAddresses", "10.42.0.0");
      const { result } = renderHook(() => useExperiment(TEST_EXPERIMENT_KEY));
      expect(result.current).toEqual("10.42.0.0");
    });

    it("should rerender whenever the ExperimentService emits a new value", () => {
      const subject = new Subject<TestExperimentValueType>();
      const wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
        <ExperimentProvider
          value={{
            addContext,
            removeContext,
            getExperiment,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            getExperimentChanges$: () => subject.asObservable() as any,
            getAllExperiments: () => ({}),
          }}
        >
          {children}
        </ExperimentProvider>
      );
      const { result } = renderHook(() => useExperiment(TEST_EXPERIMENT_KEY), {
        wrapper,
      });
      expect(result.current).toEqual("10.0.0.0,10.1.0.0");
      act(() => {
        subject.next("10.10.10.10");
      });
      expect(result.current).toEqual("10.10.10.10");
    });
  });
});
