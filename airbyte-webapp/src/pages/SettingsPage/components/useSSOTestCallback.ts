import type { ReactNode } from "react";

import { useEffect, useState } from "react";
import { useIntl } from "react-intl";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useValidateSsoToken } from "core/api";
import { useFormatError } from "core/errors";

import { isSsoTestCallback } from "./ssoTestUtils";
import { useSSOTestManager } from "./useSSOTestManager";

interface TestResult {
  success: boolean;
  message: ReactNode;
}

export const useSSOTestCallback = () => {
  const { formatMessage } = useIntl();
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const userManager = useSSOTestManager();
  const organizationId = useCurrentOrganizationId();
  const { mutateAsync: validateToken } = useValidateSsoToken();
  const formatError = useFormatError();

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const error = params.get("error");

    if (!isSsoTestCallback()) {
      return;
    }

    if (error) {
      setTestResult({
        success: false,
        message: `SSO test failed: ${error}`,
      });
    } else if (userManager) {
      // Use UserManager to handle the OAuth callback
      (async () => {
        try {
          // Let UserManager handle the OAuth callback (validates state, exchanges code for token, etc.)
          const user = await userManager.signinCallback();

          if (!user) {
            throw new Error("No user returned from OAuth callback");
          }

          // Validate the token with the backend
          await validateToken({
            organizationId,
            accessToken: user.access_token,
          });

          // Clean up the test user from storage - we don't need to persist it
          await userManager.removeUser();

          setTestResult({
            success: true,
            message: formatMessage({ id: "settings.organizationSettings.sso.test.success" }),
          });
        } catch (err) {
          setTestResult({
            success: false,
            message: formatError(err),
          });
        }
      })();
    } else {
      setTestResult({
        success: false,
        message: "SSO test failed: Missing realm parameter.",
      });
    }

    // Clean up URL parameters - remove OAuth params but keep the path
    params.delete("sso_test");
    params.delete("realm");
    params.delete("code");
    params.delete("state");
    params.delete("session_state");
    params.delete("iss");

    const newUrl = params.toString() ? `${window.location.pathname}?${params.toString()}` : window.location.pathname;
    window.history.replaceState({}, "", newUrl);
  }, [userManager, organizationId, validateToken, formatError, formatMessage]);

  return { testResult, setTestResult };
};
