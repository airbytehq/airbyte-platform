import { useWebappConfig } from "core/config";

export const CLOUD_EDITION = "CLOUD";

export const useIsCloudApp = () => {
  const { edition } = useWebappConfig();
  return edition === CLOUD_EDITION;
};
