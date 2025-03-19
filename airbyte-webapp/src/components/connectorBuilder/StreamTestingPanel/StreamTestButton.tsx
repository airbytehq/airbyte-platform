import classNames from "classnames";
import { useHotkeys } from "react-hotkeys-hook";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Tooltip } from "components/ui/Tooltip";

import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./StreamTestButton.module.scss";
import { HotkeyLabel, getCtrlOrCmdKey } from "../HotkeyLabel";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderWatch } from "../useBuilderWatch";

interface StreamTestButtonProps {
  queueStreamRead: () => void;
  hasTestingValuesErrors: boolean;
  setTestingValuesInputOpen: (open: boolean) => void;
  hasResolveErrors: boolean;
  isStreamTestQueued: boolean;
  isStreamTestRunning: boolean;
  className?: string;
}

export const StreamTestButton: React.FC<StreamTestButtonProps> = ({
  queueStreamRead,
  hasTestingValuesErrors,
  setTestingValuesInputOpen,
  hasResolveErrors,
  isStreamTestQueued,
  isStreamTestRunning,
  className,
}) => {
  const { yamlIsValid } = useConnectorBuilderFormState();
  const mode = useBuilderWatch("mode");
  const testStreamIndex = useBuilderWatch("testStreamIndex");
  const { hasErrors, validateAndTouch } = useBuilderErrors();
  const relevantViews: BuilderView[] = ["global", "inputs", testStreamIndex];

  useHotkeys(
    ["ctrl+enter", "meta+enter"],
    () => {
      executeTestRead();
    },
    { enableOnFormTags: ["input", "textarea", "select"] }
  );

  const isLoading = isStreamTestQueued || isStreamTestRunning;

  let buttonDisabled = false;
  let showWarningIcon = false;
  let tooltipContent = isLoading ? (
    <FormattedMessage id="connectorBuilder.testRead.running" />
  ) : (
    <FlexContainer direction="column" gap="md" alignItems="center">
      <FormattedMessage id="connectorBuilder.testRead" />
      <HotkeyLabel keys={[getCtrlOrCmdKey(), "Enter"]} />
    </FlexContainer>
  );

  if (mode === "yaml" && !yamlIsValid) {
    buttonDisabled = true;
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.invalidYamlTest" />;
  }

  if ((mode === "ui" && hasErrors(relevantViews)) || (mode === "yaml" && hasTestingValuesErrors)) {
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.configErrorsTest" />;
  } else if (hasResolveErrors) {
    // only disable the button on stream list errors if there are no user-fixable errors
    buttonDisabled = true;
  }

  const executeTestRead = () => {
    if (mode === "yaml" && hasTestingValuesErrors) {
      setTestingValuesInputOpen(true);
      return;
    }
    if (mode === "yaml") {
      queueStreamRead();
      return;
    }

    validateAndTouch(queueStreamRead, relevantViews);
  };

  const testButton = (
    <Button
      className={classNames(styles.testButton, className)}
      size="sm"
      onClick={executeTestRead}
      disabled={buttonDisabled}
      type="button"
      data-testid="read-stream"
      icon={showWarningIcon ? "warningOutline" : "rotate"}
      iconSize="sm"
      isLoading={isLoading}
    >
      <FormattedMessage id="connectorBuilder.testButton" />
    </Button>
  );

  return tooltipContent !== undefined ? (
    <Tooltip control={testButton} containerClassName={styles.testButtonTooltipContainer}>
      {tooltipContent}
    </Tooltip>
  ) : (
    testButton
  );
};
