import { useWebappConfig } from "core/config";

export const CLOUD_EDITION = "cloud";

export const useIsCloudApp = () => {
  const { edition } = useWebappConfig();
  return edition === CLOUD_EDITION;
};
