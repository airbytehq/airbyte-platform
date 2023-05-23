import * as experimentModule from "hooks/services/Experiment";
import { Experiments } from "hooks/services/Experiment/experiments";

jest.mock("hooks/services/Experiment");

/**
 * Takes an object of experiments and values which are used to mock the useExperiment() hook.
 * Any experiment that is not mocked will return its default value defined in the actual useExperiment() call.
 */
export function mockExperiments<T extends keyof Experiments>(mockExperiments: { [key in T]: Experiments[key] }) {
  jest
    .spyOn(experimentModule, "useExperiment")
    .mockImplementation(<U extends keyof Experiments>(calledExperimentName: U, defaultValue: Experiments[U]) => {
      if (calledExperimentName in mockExperiments) {
        return mockExperiments[calledExperimentName];
      }
      return defaultValue;
    });
}
