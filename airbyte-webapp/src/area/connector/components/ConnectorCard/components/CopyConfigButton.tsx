import React, { useEffect, useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { maskSecrets } from "area/connector/utils/maskSecrets";
import { useCurrentWorkspace } from "core/api";
import { Connector } from "core/domain/connector";
import { useNotificationService } from "core/services/Notification";
import { copyToClipboard } from "core/utils/clipboard";
import { trackError } from "core/utils/datadog";

import styles from "./CopyConfigButton.module.scss";
import { useConnectorForm } from "../../ConnectorForm/connectorFormContext";
import { ConnectorFormValues } from "../../ConnectorForm/types";
import { useRegisterConfigJson } from "../CopyConfigContext";

const COPIED_MESSAGE_DURATION_MS = 3000;

/** The button is disabled without `getConfigJson`. */
export const CopyConfigButton: React.FC<{ getConfigJson?: () => string }> = ({ getConfigJson }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const [copiedAt, setCopiedAt] = useState<number>();

  useEffect(() => {
    if (copiedAt === undefined) {
      return;
    }
    const timeout = setTimeout(() => setCopiedAt(undefined), COPIED_MESSAGE_DURATION_MS);
    return () => clearTimeout(timeout);
  }, [copiedAt]);

  const onCopy = async () => {
    if (!getConfigJson) {
      return;
    }
    try {
      await copyToClipboard(getConfigJson());
      setCopiedAt(Date.now());
    } catch (e) {
      trackError(e, { id: "connectorForm.copyConfig" });
      registerNotification({
        id: "connectorForm.copyConfig.failure",
        type: "error",
        text: formatMessage({ id: "connectorForm.copyConfig.failure" }),
      });
    }
  };

  return (
    <Tooltip
      control={
        <Button
          className={styles.button}
          variant="secondary"
          icon="copy"
          iconSize="sm"
          aria-label={formatMessage({ id: "connectorForm.copyConfig" })}
          onClick={onCopy}
          disabled={!getConfigJson}
          type="button"
        />
      }
      placement="bottom"
      cursor={getConfigJson ? undefined : "not-allowed"}
    >
      <FormattedMessage id={copiedAt === undefined ? "connectorForm.copyConfig" : "connectorForm.copyConfig.success"} />
    </Tooltip>
  );
};

/** Returns undefined until the specification loads, because masking the secrets needs the schema. */
const useFormConfigJson = () => {
  const { getValues } = useFormContext<ConnectorFormValues>();
  const { castValues, selectedConnectorDefinition, selectedConnectorDefinitionSpecification } = useConnectorForm();
  const { workspaceId } = useCurrentWorkspace();
  const schema = selectedConnectorDefinitionSpecification?.connectionSpecification as
    | Record<string, unknown>
    | undefined;

  return useMemo(() => {
    if (!schema) {
      return undefined;
    }
    return () => {
      const { name, connectionConfiguration } = castValues(getValues());
      return JSON.stringify(
        {
          name,
          workspaceId,
          definitionId: Connector.id(selectedConnectorDefinition),
          configuration: maskSecrets(connectionConfiguration as Record<string, unknown>, schema),
        },
        null,
        2
      );
    };
  }, [castValues, getValues, schema, selectedConnectorDefinition, workspaceId]);
};

/** Renders inside the connector form and copies its current values, including unsaved edits. */
export const FormCopyConfigButton: React.FC = () => {
  const getConfigJson = useFormConfigJson();
  return <CopyConfigButton getConfigJson={getConfigJson} />;
};

/** Renders inside the connector form and gives its current values to `CopyConfigProvider`. */
export const ShareFormConfigJson: React.FC = () => {
  const getConfigJson = useFormConfigJson();
  useRegisterConfigJson(getConfigJson);
  return null;
};
