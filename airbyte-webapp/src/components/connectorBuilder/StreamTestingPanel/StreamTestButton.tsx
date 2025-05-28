import classNames from "classnames";
import { ComponentProps } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./StreamTestButton.module.scss";
import { HotkeyLabel, getCtrlOrCmdKey } from "../HotkeyLabel";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderWatch } from "../useBuilderWatch";

interface StreamTestButtonProps {
  queueStreamRead: () => void;
  cancelStreamRead: () => void;
  hasTestingValuesErrors: boolean;
  setTestingValuesInputOpen: (open: boolean) => void;
  hasResolveErrors: boolean;
  isStreamTestQueued: boolean;
  isStreamTestRunning: boolean;
  isStreamTestStale: boolean;
  className?: string;
  forceDisabled?: boolean;
  requestType: "sync" | "async";
  variant?: ComponentProps<typeof Button>["variant"];
}

export const StreamTestButton: React.FC<StreamTestButtonProps> = ({
  queueStreamRead,
  cancelStreamRead,
  hasTestingValuesErrors,
  setTestingValuesInputOpen,
  hasResolveErrors,
  isStreamTestQueued,
  isStreamTestRunning,
  isStreamTestStale,
  className,
  forceDisabled,
  requestType,
  variant,
}) => {
  const { yamlIsValid } = useConnectorBuilderFormState();
  const mode = useBuilderWatch("mode");
  const testStreamId = useBuilderWatch("testStreamId");
  const { hasErrors, validateAndTouch } = useBuilderErrors();
  const relevantViews: BuilderView[] = [{ type: "global" }, { type: "inputs" }, testStreamId];

  useHotkeys(
    ["ctrl+enter", "meta+enter"],
    () => {
      executeTestRead();
    },
    { enableOnFormTags: ["input", "textarea", "select"] }
  );

  const isLoading = isStreamTestQueued || isStreamTestRunning;

  function getTooltipContent() {
    if (testStreamId.type === "stream" || testStreamId.type === "generated_stream") {
      return isLoading ? (
        <FormattedMessage id="connectorBuilder.testRead.running" />
      ) : (
        <FlexContainer direction="column" gap="md" alignItems="center">
          <FormattedMessage id="connectorBuilder.testRead" />
          <HotkeyLabel keys={[getCtrlOrCmdKey(), "Enter"]} />
          {isStreamTestStale && <FormattedMessage id="connectorBuilder.testRead.stale" />}
        </FlexContainer>
      );
    }
    return isLoading ? (
      <FormattedMessage id="connectorBuilder.dynamicStreamPreview.running" />
    ) : (
      <FlexContainer direction="column" gap="md" alignItems="center">
        <FormattedMessage id="connectorBuilder.dynamicStreamPreview" />
        <HotkeyLabel keys={[getCtrlOrCmdKey(), "Enter"]} />
      </FlexContainer>
    );
  }

  let buttonDisabled = forceDisabled || false;
  let showWarningIcon = false;
  let tooltipContent = getTooltipContent();

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
      className={classNames(styles.testButton, className, { [styles.pulsate]: isStreamTestStale && !showWarningIcon })}
      size="sm"
      onClick={executeTestRead}
      disabled={buttonDisabled}
      type="button"
      data-testid="read-stream"
      icon={showWarningIcon ? "warningOutline" : "rotate"}
      iconSize="sm"
      isLoading={isLoading}
      variant={variant}
    >
      <FormattedMessage
        id={
          testStreamId.type === "stream" || testStreamId.type === "generated_stream"
            ? "connectorBuilder.testButton"
            : "connectorBuilder.testButtonDynamic"
        }
      />
    </Button>
  );

  const finalTooltipContent =
    requestType === "async" ? (
      <FlexContainer direction="column" alignItems="center">
        {tooltipContent ?? null}
        <Text italicized className={styles.longRequestWarning} size="sm">
          <FormattedMessage id="connectorBuilder.asyncStream.longRequestWarning" />
        </Text>
      </FlexContainer>
    ) : (
      tooltipContent
    );

  return (
    <FlexContainer>
      {finalTooltipContent !== undefined ? (
        <Tooltip control={testButton} containerClassName={styles.testButtonTooltipContainer}>
          {finalTooltipContent}
        </Tooltip>
      ) : (
        testButton
      )}
      <Button
        variant="secondary"
        size="sm"
        disabled={!isLoading}
        onClick={cancelStreamRead}
        data-testid="cancel-stream-read"
      >
        <FormattedMessage id="connectorBuilder.cancel" />
      </Button>
    </FlexContainer>
  );
};
