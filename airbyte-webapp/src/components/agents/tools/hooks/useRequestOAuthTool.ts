import { useCallback, useRef, useMemo, useState } from "react";

import { useGetSourceDefinitionSpecificationAsync, useGetDestinationDefinitionSpecificationAsync } from "core/api";
import { CompleteOAuthResponseAuthPayload } from "core/api/types/AirbyteClient";
import { useRunOauthFlow } from "hooks/services/useConnectorAuth";

import { type ClientToolHandler } from "../../../chat/hooks/useChatMessages";
import { TOOL_NAMES } from "../toolNames";

export interface UseRequestOAuthToolParams {
  actorDefinitionId?: string;
  actorType: "source" | "destination";
}

export interface UseRequestOAuthToolReturn {
  handler: ClientToolHandler;
  isOAuthPendingUserAction: boolean;
  isOAuthLoading: boolean;
  startOAuth: () => void;
  cancelOAuth: () => void;
}

/**
 * Response format sent from the OAuth tool back to the agent.
 * This is serialized as JSON and parsed by the RequestOAuthToolRenderer.
 */
export interface OAuthToolResponse {
  success: boolean;
  error?: string;
  auth_payload?: CompleteOAuthResponseAuthPayload;
}

export const useRequestOAuthTool = ({
  actorDefinitionId,
  actorType,
}: UseRequestOAuthToolParams): UseRequestOAuthToolReturn => {
  // Use a ref to avoid stale closure issues with the sendResult callback
  // This ref MUST be created at the top level to persist across re-renders
  const sendResultRef = useRef<((result: string) => void) | null>(null);

  const [oauthState, setOauthState] = useState<{
    isPendingUserAction: boolean;
    isFlowStarted: boolean;
    oauthInputParams: Record<string, unknown>;
    sendResult: ((result: string) => void) | null;
  }>({
    isPendingUserAction: false,
    isFlowStarted: false,
    oauthInputParams: {},
    sendResult: null,
  });

  // Get the connector specification based on actor type
  const { data: sourceSpec } = useGetSourceDefinitionSpecificationAsync(
    actorType === "source" ? actorDefinitionId || null : null
  );
  const { data: destinationSpec } = useGetDestinationDefinitionSpecificationAsync(
    actorType === "destination" ? actorDefinitionId || null : null
  );
  const connectorSpec = actorType === "source" ? sourceSpec : destinationSpec;

  // Use a ref to access the latest connectorSpec in the memoized handler
  // This avoids stale closure issues when the spec loads after initial render
  const connectorSpecRef = useRef(connectorSpec);
  connectorSpecRef.current = connectorSpec;

  const onOAuthComplete = useCallback(
    (authPayload: CompleteOAuthResponseAuthPayload) => {
      // Send OAuth tokens back to agent as JSON
      // Use ref instead of state to avoid stale closure issues
      if (sendResultRef.current) {
        const response = JSON.stringify({
          success: true,
          auth_payload: authPayload,
        });
        sendResultRef.current(response);
      }

      // Reset state and ref
      sendResultRef.current = null;
      setOauthState({
        isPendingUserAction: false,
        isFlowStarted: false,
        oauthInputParams: {},
        sendResult: null,
      });
    },
    [] // No dependencies needed since we're using a ref
  );

  const onOAuthError = useCallback(
    (errorMessage: string) => {
      // Send error back to agent as JSON
      // Use ref instead of state to avoid stale closure issues
      if (sendResultRef.current) {
        const response = JSON.stringify({
          success: false,
          error: errorMessage,
        });
        sendResultRef.current(response);
      }

      // Reset state and ref
      sendResultRef.current = null;
      setOauthState({
        isPendingUserAction: false,
        isFlowStarted: false,
        oauthInputParams: {},
        sendResult: null,
      });
    },
    [] // No dependencies needed since we're using a ref
  );

  // Allow manual cancellation of OAuth flow (e.g., when user closes popup or encounters an error)
  const cancelOAuth = useCallback(() => {
    onOAuthError("OAuth authentication was cancelled by the user.");
  }, [onOAuthError]);

  // The non-null assertion is safe here because:
  // 1. The execute() function checks connectorSpecRef.current before setting isPendingUserAction
  // 2. startOAuth() only calls run() when isPendingUserAction is true
  // 3. Therefore run() is never invoked when connectorSpec is undefined
  const { run, loading } = useRunOauthFlow({
    connector: connectorSpec!,
    onDone: onOAuthComplete,
    onError: onOAuthError,
  });

  // Start OAuth flow when user clicks the button
  const startOAuth = useCallback(() => {
    if (oauthState.isPendingUserAction && !oauthState.isFlowStarted) {
      setOauthState((prev) => ({ ...prev, isFlowStarted: true }));
      run(oauthState.oauthInputParams);
    }
  }, [oauthState.isPendingUserAction, oauthState.isFlowStarted, oauthState.oauthInputParams, run]);

  // Memoize the handler to maintain the same reference across renders
  // This is critical - if the handler object changes during OAuth flow, we lose the ref!
  // We use connectorSpecRef to access the latest spec value without adding dependencies
  const handler: ClientToolHandler = useMemo(
    () => ({
      toolName: TOOL_NAMES.REQUEST_OAUTH_AUTHENTICATION,
      execute: (args: unknown, sendResult) => {
        const { oauth_input_params } = args as {
          oauth_input_params?: Record<string, unknown>;
          provider_name?: string;
        };

        if (!connectorSpecRef.current) {
          // Connector spec not loaded yet
          const errorResponse = JSON.stringify({
            success: false,
            error: "Connector specification not loaded",
          });
          sendResult(errorResponse);
          return;
        }

        // Store sendResult in ref to avoid stale closure issues
        sendResultRef.current = sendResult;

        // Set state to pending - user must click button to start OAuth
        // This gives users control and matches the messaging in the UI
        setOauthState({
          isPendingUserAction: true,
          isFlowStarted: false,
          oauthInputParams: oauth_input_params || {},
          sendResult, // Keep in state for other uses if needed
        });
      },
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [] // EMPTY! Handler should never be recreated to avoid losing the ref during OAuth
  );

  return {
    handler,
    // Only pending user action if we haven't started the flow yet
    isOAuthPendingUserAction: oauthState.isPendingUserAction && !oauthState.isFlowStarted,
    isOAuthLoading: loading,
    startOAuth,
    cancelOAuth,
  };
};
