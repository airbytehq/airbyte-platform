import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./StreamTestButton.module.scss";
import { useBuilderWatch } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";

interface StreamTestButtonProps {
  readStream: () => void;
  hasTestingValuesErrors: boolean;
  setTestingValuesInputOpen: (open: boolean) => void;
  isResolving: boolean;
  hasResolveErrors: boolean;
}

export const StreamTestButton: React.FC<StreamTestButtonProps> = ({
  readStream,
  hasTestingValuesErrors,
  setTestingValuesInputOpen,
  isResolving,
  hasResolveErrors,
}) => {
  const { yamlIsValid } = useConnectorBuilderFormState();
  const mode = useBuilderWatch("mode");
  const testStreamIndex = useBuilderWatch("testStreamIndex");
  const { hasErrors, validateAndTouch } = useBuilderErrors();
  const relevantViews: BuilderView[] = ["global", "inputs", testStreamIndex];

  const handleClick = () => {
    if (hasTestingValuesErrors) {
      setTestingValuesInputOpen(true);
      return;
    }
    if (mode === "yaml") {
      readStream();
      return;
    }

    validateAndTouch(readStream, relevantViews);
  };

  let buttonDisabled = false;
  let showWarningIcon = false;
  let tooltipContent = undefined;

  if (isResolving) {
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.resolvingStreamList" />;
  }

  if (mode === "yaml" && !yamlIsValid) {
    buttonDisabled = true;
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.invalidYamlTest" />;
  }

  if ((mode === "ui" && hasErrors(relevantViews)) || hasTestingValuesErrors) {
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.configErrorsTest" />;
  } else if (hasResolveErrors) {
    // only disable the button on stream list errors if there are no user-fixable errors
    buttonDisabled = true;
  }

  const testButton = (
    <Button
      className={styles.testButton}
      size="sm"
      onClick={handleClick}
      disabled={buttonDisabled}
      type="button"
      data-testid="read-stream"
      icon={showWarningIcon ? "warningOutline" : "rotate"}
    >
      <Text className={styles.testButtonText} size="sm" bold>
        <FormattedMessage id="connectorBuilder.testButton" />
      </Text>
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
