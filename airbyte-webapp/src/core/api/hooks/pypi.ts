import { trackError } from "core/utils/datadog";

import { useSuspenseQuery } from "../useSuspenseQuery";

const fetchLatestVersionOfPyPackage = async (packageName: string): Promise<string> => {
  const json = await fetch(`https://pypi.org/pypi/${packageName}/json`).then((resp) => resp.json());
  return json?.info?.version ?? undefined;
};

/**
 * Safely fetches the latest version of the Python CDK
 *
 * If the request fails, it will return undefined
 * @returns the latest version of the Python CDK
 */
export const usePythonCDKVersion = () => {
  return useSuspenseQuery<string | undefined>(
    ["pypi.cdkVersion"],
    () => {
      try {
        return fetchLatestVersionOfPyPackage("airbyte-cdk");
      } catch (e) {
        trackError(e);
        return undefined;
      }
    },
    {
      staleTime: Infinity,
    }
  );
};
