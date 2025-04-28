import { jwtDecode } from "jwt-decode";
import { PropsWithChildren, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { useCookie } from "react-use";

import { ALLOWED_ORIGIN_PARAM } from "pages/embedded/EmbeddedSourceCreatePage/hooks/useEmbeddedSourceParams";

import { AuthContext } from "./AuthContext";

/**
 * This is an auth service for the embedded widget. It is designed to be used in a parent window that posts a message to the embedded widget.
 * This auth service is used to provide an empty user object to the embedded widget to fulfill the AuthContext interface.
 */

const SCOPED_AUTH_TOKEN_COOKIE = "_airbyte_scoped_auth_token";

interface ScopedAuthMessage {
  scopedAuthToken: string;
}

const useScopedAuthToken = () => {
  const [searchParams] = useSearchParams();
  const allowedOriginParam = searchParams.get(ALLOWED_ORIGIN_PARAM);

  const allowedOrigin = allowedOriginParam ? decodeURIComponent(allowedOriginParam) : "";
  const [token, setToken, deleteToken] = useCookie(SCOPED_AUTH_TOKEN_COOKIE);

  // Only set up message listener if we have an allowed origin
  useEffect(() => {
    if (!allowedOrigin) {
      // Clear token if origin becomes invalid
      if (token) {
        deleteToken();
      }
      return;
    }

    window.parent.postMessage("auth_token_request", allowedOrigin);

    // Set a timeout to log if no response is received within 5 seconds
    const timeoutId = setTimeout(() => {
      console.error("No auth token response received from allowed origin after 5 seconds");
    }, 5000);

    const messageHandler = (event: MessageEvent<ScopedAuthMessage>) => {
      try {
        if (event.origin !== allowedOrigin) {
          return;
        }

        if (event.data?.scopedAuthToken) {
          // Clear the timeout as we received a response
          clearTimeout(timeoutId);

          const expiry = jwtDecode(event.data.scopedAuthToken).exp;
          setToken(event.data.scopedAuthToken, {
            expires: expiry ? new Date(expiry * 1000) : new Date(),
          });
        }
      } catch (error) {
        // Silently handle any errors in message processing
        console.debug("Error processing scoped auth message:", error);
      }
    };

    // Use the ref for the event listener
    window.addEventListener("message", messageHandler, false);

    return () => {
      window.removeEventListener("message", messageHandler);
      clearTimeout(timeoutId);
    };
  }, [allowedOrigin, token, deleteToken, setToken]);

  return token;
};

export const EmbeddedAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const scopedAuthToken = useScopedAuthToken();

  return (
    <AuthContext.Provider
      value={{
        authType: "embedded",
        applicationSupport: "none",
        user: null,
        inited: !!scopedAuthToken,
        emailVerified: false,
        provider: null,
        loggedOut: !scopedAuthToken,
        getAccessToken: () => Promise.resolve(scopedAuthToken),
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
