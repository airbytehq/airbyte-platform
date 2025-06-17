import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Spinner } from "components/ui/Spinner";

import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderFormManagementState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import addStreamScreenshotDark from "./add-stream-screenshot-dark.png";
import addStreamScreenshotLight from "./add-stream-screenshot-light.png";
import { AdvancedTestSettings } from "./AdvancedTestSettings";
import { StreamSelector } from "./StreamSelector";
import { StreamTester } from "./StreamTester";
import styles from "./StreamTestingPanel.module.scss";
import { useTestingValuesErrors } from "./TestingValuesMenu";
import { useBuilderWatch } from "../useBuilderWatch";

export const StreamTestingPanel: React.FC<unknown> = () => {
  const { isTestReadSettingsOpen, setTestReadSettingsOpen, setTestingValuesInputOpen } =
    useConnectorBuilderFormManagementState();
  const { yamlEditorIsMounted } = useConnectorBuilderFormState();
  const manifest = useBuilderWatch("manifest");
  const mode = useBuilderWatch("mode");
  const { theme } = useAirbyteTheme();
  const testingValuesErrors = useTestingValuesErrors();

  if (!yamlEditorIsMounted) {
    return (
      <div className={styles.loadingSpinner}>
        <Spinner />
      </div>
    );
  }

  const hasStreams =
    (manifest.streams && manifest.streams.length > 0) ||
    (manifest.dynamic_streams && manifest.dynamic_streams.length > 0);

  return (
    <div className={styles.container}>
      {hasStreams || mode === "yaml" ? (
        <>
          <FlexContainer justifyContent="space-between" gap="sm" className={styles.testingValues}>
            <StreamSelector />
            <AdvancedTestSettings
              className={styles.advancedSettings}
              isOpen={isTestReadSettingsOpen}
              setIsOpen={setTestReadSettingsOpen}
            />
          </FlexContainer>
          <StreamTester
            hasTestingValuesErrors={testingValuesErrors > 0}
            setTestingValuesInputOpen={setTestingValuesInputOpen}
          />
        </>
      ) : (
        <div className={styles.addStreamMessage}>
          <img
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
