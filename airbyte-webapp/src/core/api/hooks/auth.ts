import { useMutation, useQuery } from "@tanstack/react-query";

import { apiCall } from "../apis";

interface LoginRequestBody {
  username: string;
  password: string;
}

interface LoginResponseBody {
  username: string;
  roles: string[];
  access_token: string;
  token_type: "Bearer";
  expires_in: number;
}

// These API calls are defined in code here because this endpoint is not currently part of the open api spec
export const simpleAuthLogin = (loginRequestBody: LoginRequestBody, options: Parameters<typeof apiCall>[1]) => {
  return apiCall<LoginResponseBody>(
    {
      url: `/login`,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      data: loginRequestBody,
    },
    options
  );
};

export const simpleAuthLogout = (options: Parameters<typeof apiCall>[1]) => {
  return apiCall<null>(
    {
      url: `/logout`,
      method: "POST",
      headers: { "Content-Type": "application/json" },
    },
    options
  );
};

export const simpleAuthRefreshToken = (options: Parameters<typeof apiCall>[1]) => {
  return apiCall<LoginResponseBody>(
    {
      url: `/oauth/access_token`,
      method: "POST",
      headers: { "Content-Type": "application/json" },
    },
    options
  );
};

const simpleAuthRequestOptions = {
  getAccessToken: () => Promise.resolve(null),
  includeCredentials: true,
};

export const useSimpleAuthLogin = () => {
  return useMutation(async (loginRequestBody: LoginRequestBody) =>
    simpleAuthLogin(loginRequestBody, simpleAuthRequestOptions)
  );
};

export const useSimpleAuthLogout = () => {
  return useMutation(async () => simpleAuthLogout(simpleAuthRequestOptions));
};
export const useSimpleAuthTokenRefresh = () => {
  return useQuery(["simpleAuthTokenRefresh"], async () => await simpleAuthRefreshToken(simpleAuthRequestOptions), {
    refetchInterval: 60_000,
    refetchOnWindowFocus: true,
    refetchOnReconnect: true,
  });
};
