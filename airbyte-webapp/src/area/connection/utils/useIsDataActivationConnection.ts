import { useCurrentConnection } from "core/api";

export const useIsDataActivationConnection = (): boolean => {
  const connection = useCurrentConnection();
  return connection.destinationActorDefinitionVersion.supportsDataActivation;
};
