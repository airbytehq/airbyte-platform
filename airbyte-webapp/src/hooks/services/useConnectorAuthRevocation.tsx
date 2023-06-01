import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useMutation } from "react-query";

import { useConfig } from "config";
import { SourceAuthService } from "core/domain/connector/SourceAuthService";
import { RevokeSourceOauthTokensRequest } from "core/request/AirbyteClient";
import { useConnectorForm } from "views/Connector/ConnectorForm/connectorFormContext";

import { useNotificationService } from "./Notification";
import { useCurrentWorkspace } from "./useWorkspace";
import { ConnectorDefinitionSpecification, ConnectorSpecification } from "../../core/domain/connector";
import { useDefaultRequestMiddlewares } from "../../services/useDefaultRequestMiddlewares";

export function useConnectorAuthRevocation(): {
  revokeAuthTokens: (connector: ConnectorDefinitionSpecification) => Promise<void>;
} {
  const { workspaceId } = useCurrentWorkspace();
  const { connectorId } = useConnectorForm();
  const { apiUrl } = useConfig();

  const requestAuthMiddleware = useDefaultRequestMiddlewares();

  const sourceAuthService = useMemo(
    () => new SourceAuthService(apiUrl, requestAuthMiddleware),
    [apiUrl, requestAuthMiddleware]
  );

  return {
    revokeAuthTokens: async (connector: ConnectorDefinitionSpecification): Promise<void> => {
      return sourceAuthService.revokeOauthTokens({
        sourceDefinitionId: ConnectorSpecification.id(connector),
        sourceId: connectorId,
        workspaceId,
      } as RevokeSourceOauthTokensRequest);
    },
  };
}

const OAUTH_REVOCATION_ERROR_ID = "connector.oauthRevocationError";

export function useRunOauthRevocation({
  connector,
  onDone,
}: {
  connector: ConnectorDefinitionSpecification;
  onDone?: () => void;
}): {
  loading: boolean;
  done?: boolean;
  run: () => void;
} {
  const { revokeAuthTokens } = useConnectorAuthRevocation();
  const { registerNotification } = useNotificationService();

  const { status, mutate } = useMutation(() => revokeAuthTokens(connector), {
    onError: (e: Error) => {
      registerNotification({
        id: OAUTH_REVOCATION_ERROR_ID,
        text: <FormattedMessage id={OAUTH_REVOCATION_ERROR_ID} values={{ message: e.message }} />,
        type: "error",
      });
    },
    onSuccess: onDone,
  });

  return {
    loading: status === "loading",
    done: status === "success",
    run: mutate,
  };
}
