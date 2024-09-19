import { defaultExperimentValues, useExperiment } from "hooks/services/Experiment";

export const useAirbyteCloudIps = () => {
  const ipAddresses =
    useExperiment("connector.airbyteCloudIpAddresses") || defaultExperimentValues["connector.airbyteCloudIpAddresses"];

  return ipAddresses.split(/[ ,]+/).filter(Boolean);
};
