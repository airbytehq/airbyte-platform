import * as experimentModule from "core/services/Experiment";
import { Experiments } from "core/services/Experiment/experiments";

jest.mock("core/services/Experiment");

/**
 * Takes an object of experiments and values which are used to mock the useExperiment() hook.
 * Any experiment that is not mocked will return its default value.
 */
export function mockExperiments<T extends keyof Experiments>(mockExperiments: { [key in T]: Experiments[key] }) {
  jest
    .spyOn(experimentModule, "useExperiment")
    .mockImplementation(<U extends keyof Experiments>(calledExperimentName: U) => {
      if (calledExperimentName in mockExperiments) {
        return mockExperiments[calledExperimentName];
      }
      return experimentModule.defaultExperimentValues[calledExperimentName];
    });
}
