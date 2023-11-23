import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { ValidationError } from "yup";

import { Heading } from "components/ui/Heading";
import { Spinner } from "components/ui/Spinner";

import { ConnectorConfig } from "core/api/types/ConnectorBuilderClient";
import { Spec } from "core/api/types/ConnectorManifest";
import { jsonSchemaToFormBlock } from "core/form/schemaToFormBlock";
import { buildYupFormForJsonSchema } from "core/form/schemaToYup";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderFormManagementState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import addStreamScreenshotDark from "./add-stream-screenshot-dark.png";
import addStreamScreenshotLight from "./add-stream-screenshot-light.png";
import { ConfigMenu } from "./ConfigMenu";
import { StreamSelector } from "./StreamSelector";
import { StreamTester } from "./StreamTester";
import styles from "./StreamTestingPanel.module.scss";
import { useBuilderWatch } from "../types";

const EMPTY_SCHEMA = {};

function useTestInputJsonErrors(testInputJson: ConnectorConfig | undefined, spec?: Spec): number {
  const { formatMessage } = useIntl();

  return useMemo(() => {
    try {
      const jsonSchema = spec && spec.connection_specification ? spec.connection_specification : EMPTY_SCHEMA;
      const formFields = jsonSchemaToFormBlock(jsonSchema);
      const validationSchema = buildYupFormForJsonSchema(jsonSchema, formFields, formatMessage);
      validationSchema.validateSync(testInputJson, { abortEarly: false });
      return 0;
    } catch (e) {
      if (ValidationError.isError(e)) {
        return e.errors.length;
      }
      return 1;
    }
  }, [spec, formatMessage, testInputJson]);
}

export const StreamTestingPanel: React.FC<unknown> = () => {
  const { isTestInputOpen, setTestInputOpen } = useConnectorBuilderFormManagementState();
  const { jsonManifest, yamlEditorIsMounted } = useConnectorBuilderFormState();
  const { testInputJson } = useConnectorBuilderTestRead();
  const mode = useBuilderWatch("mode");
  const { theme } = useAirbyteTheme();

  const testInputJsonErrors = useTestInputJsonErrors(testInputJson, jsonManifest.spec);

  if (!yamlEditorIsMounted) {
    return (
      <div className={styles.loadingSpinner}>
        <Spinner />
      </div>
    );
  }

  const hasStreams = jsonManifest.streams?.length > 0;

  return (
    <div className={styles.container}>
      <ConfigMenu testInputJsonErrors={testInputJsonErrors} isOpen={isTestInputOpen} setIsOpen={setTestInputOpen} />
      {hasStreams || mode === "yaml" ? (
        <>
          <StreamSelector className={styles.streamSelector} />
          <StreamTester hasTestInputJsonErrors={testInputJsonErrors > 0} setTestInputOpen={setTestInputOpen} />
        </>
      ) : (
        <div className={styles.addStreamMessage}>
          <img
            className={styles.logo}
            alt=""
            src={theme === "airbyteThemeLight" ? addStreamScreenshotLight : addStreamScreenshotDark}
            width={320}
          />
          <Heading as="h2" className={styles.addStreamHeading}>
            <FormattedMessage id="connectorBuilder.noStreamsMessage" />
          </Heading>
        </div>
      )}
    </div>
  );
};
