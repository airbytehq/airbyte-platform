import { useCallback, useEffect, useRef } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useAsyncFn, useEffectOnce, useEvent } from "react-use";

import { useCompleteOAuth, useConsentUrls } from "core/api";
import { ConnectorDefinition, ConnectorDefinitionSpecification, ConnectorSpecification } from "core/domain/connector";
import { isSourceDefinitionSpecification } from "core/domain/connector/source";
import {
  CompleteOAuthResponse,
  CompleteOAuthResponseAuthPayload,
  DestinationOauthConsentRequest,
  SourceOauthConsentRequest,
} from "core/request/AirbyteClient";
import { isCommonRequestError } from "core/request/CommonRequestError";
import { useAnalyticsTrackFunctions } from "views/Connector/ConnectorForm/components/Sections/auth/useAnalyticsTrackFunctions";
import { useConnectorForm } from "views/Connector/ConnectorForm/connectorFormContext";

import { useAppMonitoringService } from "./AppMonitoringService";
import { useNotificationService } from "./Notification";
import { useCurrentWorkspace } from "./useWorkspace";
import { useQuery } from "../useQuery";

let windowObjectReference: Window | null = null; // global variable

const OAUTH_REDIRECT_URL = `${window.location.protocol}//${window.location.host}`;

function openWindow(url: string): void {
  if (windowObjectReference == null || windowObjectReference.closed) {
    /* if the pointer to the window object in memory does not exist
       or if such pointer exists but the window was closed */

    const strWindowFeatures = "toolbar=no,menubar=no,width=600,height=700,top=100,left=100";
    windowObjectReference = window.open(url, "name", strWindowFeatures);
    /* then create it. The new window will be created and
       will be brought on top of any other window. */
  } else {
    windowObjectReference.focus();
    /* else the window reference must exist and the window
       is not closed; therefore, we can bring it back on top of any other
       window with the focus() method. There would be no need to re-create
       the window or to reload the referenced resource. */
  }
}

export function useConnectorAuth(): {
  getConsentUrl: (
    connector: ConnectorDefinitionSpecification,
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
  const { trackError } = useAppMonitoringService();
  const { workspaceId } = useCurrentWorkspace();
  const { getDestinationConsentUrl, getSourceConsentUrl } = useConsentUrls();
  const { completeDestinationOAuth, completeSourceOAuth } = useCompleteOAuth();
  const notificationService = useNotificationService();
  const { connectorId } = useConnectorForm();

  return {
    getConsentUrl: async (
      connector: ConnectorDefinitionSpecification,
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
            redirectUrl: `${OAUTH_REDIRECT_URL}/auth_flow`,
            oAuthInputConfiguration,
            sourceId: connectorId,
          };
          const response = await getSourceConsentUrl(payload);

          return { consentUrl: response.consentUrl, payload };
        }
        const payload: DestinationOauthConsentRequest = {
          workspaceId,
          destinationDefinitionId: ConnectorSpecification.id(connector),
          redirectUrl: `${OAUTH_REDIRECT_URL}/auth_flow`,
          oAuthInputConfiguration,
          destinationId: connectorId,
        };
        const response = await getDestinationConsentUrl(payload);

        return { consentUrl: response.consentUrl, payload };
      } catch (e) {
        // If this API returns a 404 the OAuth credentials have not been added to the database.
        if (isCommonRequestError(e) && e.status === 404) {
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
      return "sourceDefinitionId" in payload ? completeSourceOAuth(payload) : completeDestinationOAuth(payload);
    },
  };
}

const OAUTH_ERROR_ID = "connector.oauthError";

export function useRunOauthFlow({
  connector,
  connectorDefinition,
  onDone,
}: {
  connector: ConnectorDefinitionSpecification;
  connectorDefinition?: ConnectorDefinition;
  onDone?: (values: CompleteOAuthResponseAuthPayload) => void;
}): {
  loading: boolean;
  done?: boolean;
  run: (oauthInputParams: Record<string, unknown>) => void;
} {
  const { getConsentUrl, completeOauthRequest } = useConnectorAuth();
  const { registerNotification } = useNotificationService();
  const param = useRef<SourceOauthConsentRequest | DestinationOauthConsentRequest>();
  const connectorType = isSourceDefinitionSpecification(connector) ? "source" : "destination";
  const { trackOAuthSuccess, trackOAuthAttemp } = useAnalyticsTrackFunctions(connectorType);
  const [{ loading }, onStartOauth] = useAsyncFn(
    async (oauthInputParams: Record<string, unknown>) => {
      trackOAuthAttemp(connectorDefinition);
      const consentRequestInProgress = await getConsentUrl(connector, oauthInputParams);

      param.current = consentRequestInProgress.payload;
      openWindow(consentRequestInProgress.consentUrl);
    },
    [connector]
  );

  const [{ loading: loadingCompleteOauth, value }, completeOauth] = useAsyncFn(
    async (queryParams: Record<string, unknown>) => {
      const oauthStartedPayload = param.current;

      if (!oauthStartedPayload) {
        // unexpected call, no oauth flow was started
        return false;
      }

      let completeOauthResponse: CompleteOAuthResponse;
      try {
        completeOauthResponse = await completeOauthRequest(oauthStartedPayload, queryParams);
      } catch (e) {
        registerNotification({
          id: OAUTH_ERROR_ID,
          text: <FormattedMessage id={OAUTH_ERROR_ID} values={{ message: e.message }} />,
          type: "error",
        });
        return false;
      }

      if (!completeOauthResponse.request_succeeded || !completeOauthResponse.auth_payload) {
        // user canceled
        param.current = undefined;
        return false;
      }

      trackOAuthSuccess(connectorDefinition);
      onDone?.(completeOauthResponse.auth_payload);
      return true;
    },
    [connector, onDone]
  );

  const onOathGranted = useCallback(
    async (event: MessageEvent) => {
      // TODO: check if more secure option is required
      if (event.data?.airbyte_type === "airbyte_oauth_callback" && event.origin === window.origin) {
        await completeOauth(event.data.query);
      }
    },
    [completeOauth]
  );

  const onCloseWindow = useCallback(() => {
    windowObjectReference?.close();
  }, []);

  useEffect(
    () => () => {
      // Close popup oauth window when unmounting
      onCloseWindow();
    },
    [onCloseWindow]
  );

  useEvent("message", onOathGranted);
  // Close popup oauth window when we close the original tab
  useEvent("beforeunload", onCloseWindow);

  return {
    loading: loadingCompleteOauth || loading,
    done: value,
    run: onStartOauth,
  };
}

export function useResolveNavigate(): void {
  const query = useQuery();

  useEffectOnce(() => {
    // we add "airbyte_type" into the window so that we can catch events only from this window back in `onOathGranted`
    window.opener?.postMessage({ airbyte_type: "airbyte_oauth_callback", query });
    window.close();
  });
}
