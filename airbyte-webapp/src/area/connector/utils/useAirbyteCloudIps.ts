import { useExperiment } from "hooks/services/Experiment";

export const useAirbyteCloudIps = () => {
  const defaultIpAddresses =
    "34.106.109.131, 34.106.196.165, 34.106.60.246, 34.106.229.69, 34.106.127.139, 34.106.218.58, 34.106.115.240, 34.106.225.141, 13.37.4.46, 13.37.142.60, 35.181.124.238";

  const ipAddresses = useExperiment("connector.airbyteCloudIpAddresses", defaultIpAddresses) || defaultIpAddresses;

  return ipAddresses.split(/[ ,]+/).filter(Boolean);
};
