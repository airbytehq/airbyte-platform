import { BroadcastChannel } from "broadcast-channel";
import { useCallback, useEffect, useRef } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useAsyncFn, useEvent, useUnmount } from "react-use";
import { v4 as uuid } from "uuid";

import { useAnalyticsTrackFunctions } from "area/connector/components/ConnectorForm/components/Sections/auth/useAnalyticsTrackFunctions";
import { useConnectorForm } from "area/connector/components/ConnectorForm/connectorFormContext";
import { OAuthEvent } from "area/connector/types/oauthCallback";
import { OAUTH_BROADCAST_CHANNEL_NAME } from "area/connector/utils/oauthConstants";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentWorkspace } from "area/workspace/utils/useWorkspace";
import { HttpError, useCompleteOAuth, useCompleteOAuthBuilder, useConsentUrls, useConsentUrlsBuilder } from "core/api";
import {
  CompleteOAuthResponse,
  CompleteOAuthResponseAuthPayload,
  DestinationOauthConsentRequest,
  SourceOauthConsentRequest,
  BuilderProjectOauthConsentRequest,
  CompleteConnectorBuilderProjectOauthRequest,
} from "core/api/types/AirbyteClient";
import { ConnectorDefinitionSpecificationRead, ConnectorSpecification } from "core/domain/connector";
import { isSourceDefinitionSpecification } from "core/domain/connector/source";
import { useFormatError } from "core/errors";
import { useNotificationService } from "core/services/Notification";
import { trackError } from "core/utils/datadog";

let windowObjectReference: Window | null = null;

const tabUuid = uuid();
export const OAUTH_REDIRECT_URL = `${window.location.protocol}//${window.location.host}/auth_flow`;

/**
 * Since some OAuth providers clear out the window.opener and window.name properties,
 * we must listen for a message on a broadcast channel to complete the OAuth flow.
 *
 * Since we don't have any context to relate the complete OAuth payload back to the
 * tab that started the flow, we must restrict the OAuth flow to only be in progress
 * for a single tab at a time.
 *
 * We do this by closing the popup and broadcast channels for all other tabs whenever
 * a new OAuth flow is started (using the type: "takeover" event).
 *
 * When navigating away from this page, we close the popup and broadcast channel for
 * this tab specifically by passing the tabUuid in the broadcast channel message
 * with type: "cancel".
 *
 * When the OAuth flow is completed, the popup window emits a message with
 * type: "completed" along with the oauth body, which the single tab that is listening
 * on the broadcast channel receives and uses to complete the OAuth flow.
 *
 * @param url the OAuth consent URL
 */
function openWindow(url: string): Window | null {
  /* if the pointer to the window object in memory does not exist
       or if such pointer exists but the window was closed */

  // Hook does not add type safetiness as we have to dynamically craft the key from the identifier
  // eslint-disable-next-line @airbyte/no-local-storage
  const strWindowFeatures = "toolbar=no,menubar=no,width=600,height=700";
  windowObjectReference = window.open(url, "", strWindowFeatures);
  /* then create it. The new window will be created and
       will be brought on top of any other window. */
  return windowObjectReference;
}

export function useConnectorAuth(): {
  getConsentUrl: (
    connector: ConnectorDefinitionSpecificationRead,
    oAuthInputConfiguration: Record<string, unknown>
  ) => Promise<{
    payload: SourceOauthConsentRequest | DestinationOauthConsentRequest;
    consentUrl: string;
  }>;
  completeOauthRequest: (
    params: SourceOauthConsentRequest | DestinationOauthConsentRequest,
    queryParams: Record<string, unknown>
  ) => Promise<CompleteOAuthResponse>;
} {
  const { formatMessage } = useIntl();
  const workspaceId = useCurrentWorkspaceId();
  const { getDestinationConsentUrl, getSourceConsentUrl } = useConsentUrls();
  const { completeDestinationOAuth, completeSourceOAuth } = useCompleteOAuth();
  const notificationService = useNotificationService();
  const { connectorId } = useConnectorForm();

  return {
    getConsentUrl: async (
      connector: ConnectorDefinitionSpecificationRead,
      oAuthInputConfiguration: Record<string, unknown>
    ): Promise<{
      payload: SourceOauthConsentRequest | DestinationOauthConsentRequest;
      consentUrl: string;
    }> => {
      try {
        if (isSourceDefinitionSpecification(connector)) {
          const payload: SourceOauthConsentRequest = {
            workspaceId,
            sourceDefinitionId: ConnectorSpecification.id(connector),
            redirectUrl: OAUTH_REDIRECT_URL,
            oAuthInputConfiguration,
            sourceId: connectorId,
          };
          const response = await getSourceConsentUrl(payload);

          return { consentUrl: response.consentUrl, payload };
        }
        const payload: DestinationOauthConsentRequest = {
          workspaceId,
          destinationDefinitionId: ConnectorSpecification.id(connector),
          redirectUrl: OAUTH_REDIRECT_URL,
          oAuthInputConfiguration,
          destinationId: connectorId,
        };
        const response = await getDestinationConsentUrl(payload);

        return { consentUrl: response.consentUrl, payload };
      } catch (e) {
        // If this API returns a 404 the OAuth credentials have not been added to the database.
        if (e instanceof HttpError && e.status === 404) {
          if (process.env.NODE_ENV === "development") {
            notificationService.registerNotification({
              id: "oauthConnector.credentialsMissing",
              // Since it's dev only we don't need i18n on this string
              text: "OAuth is not enabled for this connector on this environment.",
            });
          } else {
            // Log error to our monitoring, this should never happen and means OAuth credentials
            // where missed
            trackError(e, {
              id: "oauthConnector.credentialsMissing",
              connectorSpecId: ConnectorSpecification.id(connector),
              workspaceId,
            });
            notificationService.registerNotification({
              id: "oauthConnector.credentialsMissing",
              text: formatMessage({ id: "connector.oauthCredentialsMissing" }),
              type: "error",
            });
          }
        }
        throw e;
      }
    },
    completeOauthRequest: async (
      params: SourceOauthConsentRequest | DestinationOauthConsentRequest,
      queryParams: Record<string, unknown>
    ): Promise<CompleteOAuthResponse> => {
      const payload = {
        ...params,
        queryParams,
      };

      const ret = await ("sourceDefinitionId" in payload
        ? completeSourceOAuth(payload)
        : completeDestinationOAuth(payload));
      return ret;
    },
  };
}

export function useConnectorAuthBuilder(): {
  getConsentUrl: (
    builderProjectId: string,
    oAuthInputConfiguration: Record<string, unknown>
  ) => Promise<{
    payload: BuilderProjectOauthConsentRequest;
    consentUrl: string;
  }>;
  completeOauthRequest: (
    builderProjectId: string,
    queryParams: Record<string, unknown>
  ) => Promise<CompleteOAuthResponse>;
} {
  const { workspaceId } = useCurrentWorkspace();
  const { getConsentUrl } = useConsentUrlsBuilder();
  const { completeOAuth } = useCompleteOAuthBuilder();

  return {
    getConsentUrl: async (
      builderProjectId: string,
      oAuthInputConfiguration: Record<string, unknown>
    ): Promise<{
      payload: BuilderProjectOauthConsentRequest;
      consentUrl: string;
    }> => {
      try {
        const payload: BuilderProjectOauthConsentRequest = {
          workspaceId,
          builderProjectId,
          redirectUrl: OAUTH_REDIRECT_URL,
          oAuthInputConfiguration,
        };
        const response = await getConsentUrl(payload);

        return { consentUrl: response.consentUrl, payload };
      } catch (e) {
        // @TODO: this whole sad path block

        // // If this API returns a 404 the OAuth credentials have not been added to the database.
        // if (e instanceof HttpError && e.status === 404) {
        //   if (process.env.NODE_ENV === "development") {
        //     notificationService.registerNotification({
        //       id: "oauthConnector.credentialsMissing",
        //       // Since it's dev only we don't need i18n on this string
        //       text: "OAuth is not enabled for this connector on this environment.",
        //     });
        //   } else {
        //     // Log error to our monitoring, this should never happen and means OAuth credentials
        //     // where missed
        //     trackError(e, {
        //       id: "oauthConnector.credentialsMissing",
        //       connectorSpecId: ConnectorSpecification.id(connector),
        //       workspaceId,
        //     });
        //     notificationService.registerNotification({
        //       id: "oauthConnector.credentialsMissing",
        //       text: formatMessage({ id: "connector.oauthCredentialsMissing" }),
        //       type: "error",
        //     });
        //   }
        // }
        throw e;
      }
    },
    completeOauthRequest: async (
      builderProjectId: string,
      queryParams: Record<string, unknown>
    ): Promise<CompleteOAuthResponse> => {
      const payload: CompleteConnectorBuilderProjectOauthRequest = {
        builderProjectId,
        redirectUrl: OAUTH_REDIRECT_URL,
        workspaceId,
        queryParams,
      };

      const ret = completeOAuth(payload);
      return ret;
    },
  };
}

const OAUTH_ERROR_ID = "connector.oauthError";

export function useRunOauthFlow({
  connector,
  onDone,
  onError,
}: {
  connector: ConnectorDefinitionSpecificationRead;
  onDone?: (values: CompleteOAuthResponseAuthPayload) => void;
  onError?: (error: string) => void;
}): {
  loading: boolean;
  done?: boolean;
  run: (oauthInputParams: Record<string, unknown>) => void;
} {
  const { selectedConnectorDefinition } = useConnectorForm();
  const { getConsentUrl, completeOauthRequest } = useConnectorAuth();
  const { registerNotification } = useNotificationService();
  const param = useRef<SourceOauthConsentRequest | DestinationOauthConsentRequest>();
  const connectorType = isSourceDefinitionSpecification(connector) ? "source" : "destination";
  const { trackOAuthSuccess, trackOAuthAttemp } = useAnalyticsTrackFunctions(connectorType);

  const [{ loading: loadingCompleteOauth, value }, completeOauth] = useAsyncFn(
    async (queryParams: Record<string, unknown>) => {
      const oauthStartedPayload = param.current;

      if (!oauthStartedPayload) {
        // unexpected call, no oauth flow was started
        onError?.("No OAuth flow was started");
        return false;
      }

      // Check for OAuth provider errors in the query params
      // OAuth providers like Google return error and error_description when auth fails
      if (queryParams.error) {
        const errorMessage =
          (queryParams.error_description as string) ||
          (queryParams.error as string) ||
          "OAuth authentication was denied or failed";
        registerNotification({
          id: OAUTH_ERROR_ID,
          text: <FormattedMessage id={OAUTH_ERROR_ID} values={{ message: errorMessage }} />,
          type: "error",
        });
        onError?.(errorMessage);
        param.current = undefined;
        return false;
      }

      let completeOauthResponse: CompleteOAuthResponse;
      try {
        completeOauthResponse = await completeOauthRequest(oauthStartedPayload, queryParams);
      } catch (e) {
        const errorMessage = e instanceof Error ? e.message : "Failed to complete OAuth";
        registerNotification({
          id: OAUTH_ERROR_ID,
          text: <FormattedMessage id={OAUTH_ERROR_ID} values={{ message: errorMessage }} />,
          type: "error",
        });
        onError?.(errorMessage);
        return false;
      }

      if (!completeOauthResponse.request_succeeded || !completeOauthResponse.auth_payload) {
        // user canceled or request failed
        const errorMessage = "OAuth request was not successful";
        onError?.(errorMessage);
        param.current = undefined;
        return false;
      }

      trackOAuthSuccess(selectedConnectorDefinition);
      onDone?.(completeOauthResponse.auth_payload);
      return true;
    },
    [connector, onDone, onError]
  );

  const [{ loading }, onStartOauth] = useAsyncFn(
    async (oauthInputParams: Record<string, unknown>) => {
      trackOAuthAttemp(selectedConnectorDefinition);
      const consentRequestInProgress = await getConsentUrl(connector, oauthInputParams);

      param.current = consentRequestInProgress.payload;

      // some oauth services (e.g. airflow) drop some of the query params we send as part of the consent url
      // so parse apart that url and re-apply the query params when completing the oauth flow
      const url = new URL(consentRequestInProgress.consentUrl);
      const consentUrlQueryParams = Object.fromEntries(url.searchParams);

      if (windowObjectReference && !windowObjectReference.closed) {
        // popup window is already open, so just focus it
        windowObjectReference.focus();
      } else {
        // popup window is not open, so open it and start listening on broadcast channel
        const popupWindow = openWindow(consentRequestInProgress.consentUrl);

        // NOTE: We cannot reliably monitor popup closure for cross-origin OAuth flows.
        // When the popup navigates to Google/other OAuth providers, the browser's
        // cross-origin security causes popupWindow.closed to return true even when
        // the popup is still open. Instead, we provide a cancel mechanism through
        // the onError callback that can be triggered by the user via UI.

        const bc = new BroadcastChannel<OAuthEvent>(OAUTH_BROADCAST_CHANNEL_NAME);

        bc.postMessage({ type: "takeover" });
        bc.onmessage = async (event) => {
          if (event.type === "cancel" && event.tabUuid !== tabUuid) {
            // cancel event is not meant for this tab, so ignore it
            return;
          }
          if (event.type === "completed") {
            const queryParams = {
              ...consentUrlQueryParams, // ensure we pass along params from the consent url
              ...event.query, // but any params provided here take priority and override
            };

            await completeOauth(queryParams);

            // Close the /auth_flow page after completing the oauth flow
            bc.postMessage({ type: "close" });
          }
          // OAuth flow is completed or taken over by another tab, so close the broadcast channel
          // and popup window if it is still open.
          await bc.close();
          popupWindow?.close();
        };
      }
    },
    [connector, onError]
  );

  // close the popup window and broadcast channel when unmounting
  useUnmount(() => {
    const cancelBc = new BroadcastChannel<OAuthEvent>(OAUTH_BROADCAST_CHANNEL_NAME);
    cancelBc.postMessage({ type: "cancel", tabUuid });
    cancelBc.close();
  });

  const onCloseWindow = useCallback(() => {
    windowObjectReference?.close();
  }, []);

  // close popup oauth window when we close the original tab
  useEvent("beforeunload", onCloseWindow);

  return {
    loading: loadingCompleteOauth || loading,
    done: value,
    run: onStartOauth,
  };
}

export function useRunOauthFlowBuilder({
  builderProjectId,
  onDone,
}: {
  builderProjectId: string;
  onDone?: (values: CompleteOAuthResponseAuthPayload) => void;
}): {
  loading: boolean;
  done?: boolean;
  run: (oauthInputParams: Record<string, unknown>) => void;
} {
  const { getConsentUrl, completeOauthRequest } = useConnectorAuthBuilder();
  const { registerNotification } = useNotificationService();
  const formatError = useFormatError();
  const param = useRef<BuilderProjectOauthConsentRequest>();
  // const { trackOAuthSuccess, trackOAuthAttemp } = useAnalyticsTrackFunctions(connectorType);

  const [{ loading: loadingCompleteOauth, value }, completeOauth] = useAsyncFn(
    async (queryParams: Record<string, unknown>) => {
      const oauthStartedPayload = param.current;

      if (!oauthStartedPayload) {
        // unexpected call, no oauth flow was started
        return false;
      }

      let completeOauthResponse: CompleteOAuthResponse;
      try {
        completeOauthResponse = await completeOauthRequest(builderProjectId, queryParams);
      } catch (e) {
        registerNotification({
          id: OAUTH_ERROR_ID,
          text: <FormattedMessage id={OAUTH_ERROR_ID} values={{ message: formatError(e) }} />,
          type: "error",
        });
        return false;
      }

      if (!completeOauthResponse.request_succeeded || !completeOauthResponse.auth_payload) {
        // user canceled
        param.current = undefined;
        return false;
      }

      // trackOAuthSuccess(connectorDefinition);
      onDone?.(completeOauthResponse.auth_payload);
      return true;
    },
    [builderProjectId, onDone]
  );

  const [{ loading, error }, onStartOauth] = useAsyncFn(
    async (oauthInputParams: Record<string, unknown>) => {
      // trackOAuthAttemp(connectorDefinition);
      const consentRequestInProgress = await getConsentUrl(builderProjectId, oauthInputParams);
      param.current = consentRequestInProgress.payload;

      // some oauth services (e.g. airflow) drop some of the query params we send as part of the consent url
      // so parse apart that url and re-apply the query params when completing the oauth flow
      const url = new URL(consentRequestInProgress.consentUrl);
      const consentUrlQueryParams = Object.fromEntries(url.searchParams);

      if (windowObjectReference && !windowObjectReference.closed) {
        // popup window is already open, so just focus it
        windowObjectReference.focus();
      } else {
        // popup window is not open, so open it and start listening on broadcast channel
        const popupWindow = openWindow(consentRequestInProgress.consentUrl);

        const bc = new BroadcastChannel<OAuthEvent>(OAUTH_BROADCAST_CHANNEL_NAME);
        bc.postMessage({ type: "takeover" });
        bc.onmessage = async (event) => {
          if (event.type === "cancel" && event.tabUuid !== tabUuid) {
            // cancel event is not meant for this tab, so ignore it
            return;
          }
          if (event.type === "completed") {
            const queryParams = {
              ...consentUrlQueryParams, // ensure we pass along params from the consent url
              ...event.query, // but any params provided here take priority and override
            };

            await completeOauth(queryParams);
          }
          // OAuth flow is completed or taken over by another tab, so close the broadcast channel
          // and popup window if it is still open.
          await bc.close();
          popupWindow?.close();
        };
      }
    },
    [builderProjectId]
  );

  useEffect(() => {
    if (error) {
      registerNotification({
        id: OAUTH_ERROR_ID,
        text: <FormattedMessage id={OAUTH_ERROR_ID} values={{ message: formatError(error) }} />,
        type: "error",
      });
    }
  }, [registerNotification, formatError, error]);

  // close the popup window and broadcast channel when unmounting
  useUnmount(() => {
    const cancelBc = new BroadcastChannel<OAuthEvent>(OAUTH_BROADCAST_CHANNEL_NAME);
    cancelBc.postMessage({ type: "cancel", tabUuid });
    cancelBc.close();
  });

  const onCloseWindow = useCallback(() => {
    windowObjectReference?.close();
  }, []);

  // close popup oauth window when we close the original tab
  useEvent("beforeunload", onCloseWindow);

  return {
    loading: loadingCompleteOauth || loading,
    done: value,
    run: onStartOauth,
  };
}
