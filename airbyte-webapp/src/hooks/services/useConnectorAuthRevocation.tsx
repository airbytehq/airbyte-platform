import { useMutation } from "@tanstack/react-query";
import { FormattedMessage } from "react-intl";

import { useRevokeSourceOAuthToken } from "core/api";

import { useNotificationService } from "./Notification";
import { useCurrentWorkspace } from "./useWorkspace";
import { ConnectorDefinitionSpecificationRead, ConnectorSpecification } from "../../core/domain/connector";

export function useConnectorAuthRevocation(sourceId: string): {
  revokeAuthTokens: (connector: ConnectorDefinitionSpecificationRead) => Promise<void>;
} {
  const { workspaceId } = useCurrentWorkspace();
  const revokeSourceOAuthToken = useRevokeSourceOAuthToken();

  return {
    revokeAuthTokens: async (connector: ConnectorDefinitionSpecificationRead): Promise<void> => {
      return revokeSourceOAuthToken({
        sourceDefinitionId: ConnectorSpecification.id(connector),
        sourceId,
        workspaceId,
      });
    },
  };
}

const OAUTH_REVOCATION_ERROR_ID = "connector.oauthRevocationError";

export function useRunOauthRevocation({
  sourceId,
  connector,
  onDone,
}: {
  sourceId: string;
  connector: ConnectorDefinitionSpecificationRead;
  onDone?: () => void;
}): {
  loading: boolean;
  done?: boolean;
  run: () => void;
} {
  const { revokeAuthTokens } = useConnectorAuthRevocation(sourceId);
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
