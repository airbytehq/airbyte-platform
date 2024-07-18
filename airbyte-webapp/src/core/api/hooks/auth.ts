import { useMutation } from "@tanstack/react-query";

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

// Defined in code here because this endpoint is not currently part of the open api spec
export const login = (loginRequestBody: LoginRequestBody, options: Parameters<typeof apiCall>[1]) => {
  return apiCall<LoginResponseBody>(
    {
      url: `/login`,
      method: "post",
      headers: { "Content-Type": "application/json" },
      data: loginRequestBody,
    },
    options
  );
};

export const simpleAuthLogin = async (email: string, password: string): Promise<LoginResponseBody> => {
  return login({ username: email, password }, { getAccessToken: () => Promise.resolve(null) });
};

export const useSimpleAuthLogin = () => {
  return useMutation(
    async (loginRequestBody: LoginRequestBody) =>
      await simpleAuthLogin(loginRequestBody.username, loginRequestBody.password)
  );
};
