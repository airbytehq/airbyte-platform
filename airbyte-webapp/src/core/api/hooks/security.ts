import { useQuery } from "@tanstack/react-query";

interface SecurityResult {
  status: "open" | "closed" | "default_auth";
}

const securityCheck = async (host: string): Promise<SecurityResult> => {
  return fetch("https://oss.airbyte.com/security-check", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({ host }),
  }).then((resp) => resp.json());
};

/**
 * Execute a security check for the specified host. This will check if that given host is reachable from
 * the internet and if it is behind auth, whether it's using the default username and password.
 */
export const useOssSecurityCheck = (host: string) => {
  return useQuery<SecurityResult>(["oss.securityCheck", host], () => securityCheck(host));
};
