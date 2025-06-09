import { useCurrentConnection } from "core/api";
import { useExperiment } from "hooks/services/Experiment";

export const useIsDataActivationConnection = (): boolean => {
  const dataActivationUIEnabled = useExperiment("connection.dataActivationUI");
  const connection = useCurrentConnection();
  return dataActivationUIEnabled && connection.destinationActorDefinitionVersion.supportsDataActivation;
};
