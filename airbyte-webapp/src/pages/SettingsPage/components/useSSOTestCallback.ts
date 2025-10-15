import type { ReactNode } from "react";

import { useEffect, useState } from "react";
import { useIntl } from "react-intl";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useExchangeSsoAuthCode } from "core/api";
import { useFormatError } from "core/errors";

import { getSsoTestRealm, isSsoTestCallback } from "./ssoTestUtils";

interface TestResult {
  success: boolean;
  message: ReactNode;
}

export const useSSOTestCallback = () => {
  const { formatMessage } = useIntl();
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const organizationId = useCurrentOrganizationId();
  const { mutateAsync: exchangeAuthCode } = useExchangeSsoAuthCode();
  const formatError = useFormatError();

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const error = params.get("error");
    const authorizationCode = params.get("code");
    const realm = getSsoTestRealm();

    if (!isSsoTestCallback()) {
      return;
    }

    if (error) {
      setTestResult({
        success: false,
        message: `SSO test failed: ${error}`,
      });
    } else if (realm && authorizationCode) {
      // Use server-side token exchange to avoid session cookie issues
      (async () => {
        try {
          // Get the OAuth state parameter from the URL - this is the key for the stored state
          const state = params.get("state");
          if (!state) {
            throw new Error("Missing state parameter from OAuth callback");
          }

          // Retrieve the code_verifier from localStorage (stored by oidc-client-ts during authorization)
          // The storage key format is "sso_test.{state}" where state is the OAuth state parameter
          const storageKey = `sso_test.${state}`;
          // eslint-disable-next-line @airbyte/no-local-storage -- OAuth state managed by external oidc-client-ts library
          const storageData = localStorage.getItem(storageKey);

          if (!storageData) {
            throw new Error("Missing OAuth state from storage");
          }

          const oidcState = JSON.parse(storageData);
          const codeVerifier = oidcState.code_verifier;
          const redirectUri = oidcState.redirect_uri;

          if (!codeVerifier) {
            throw new Error("Missing code_verifier from OAuth state");
          }

          if (!redirectUri) {
            throw new Error("Missing redirect_uri from OAuth state");
          }

          // Exchange the authorization code on the server side
          await exchangeAuthCode({
            organizationId,
            authorizationCode,
            codeVerifier,
            redirectUri,
          });

          // Clean up the OAuth state from storage
          // eslint-disable-next-line @airbyte/no-local-storage -- OAuth state managed by external oidc-client-ts library
          localStorage.removeItem(storageKey);

          setTestResult({
            success: true,
            message: formatMessage({ id: "settings.organizationSettings.sso.test.success" }),
          });
        } catch (err) {
          // Clean up storage on error too
          const state = params.get("state");
          if (state) {
            const storageKey = `sso_test.${state}`;
            // eslint-disable-next-line @airbyte/no-local-storage -- OAuth state managed by external oidc-client-ts library
            localStorage.removeItem(storageKey);
          }

          setTestResult({
            success: false,
            message: formatError(err),
          });
        }
      })();
    } else if (!realm) {
      setTestResult({
        success: false,
        message: "SSO test failed: Missing realm parameter.",
      });
    } else {
      setTestResult({
        success: false,
        message: "SSO test failed: Missing authorization code.",
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
  }, [organizationId, exchangeAuthCode, formatError, formatMessage]);

  return { testResult, setTestResult };
};
