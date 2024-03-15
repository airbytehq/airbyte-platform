import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./PublishButton.module.scss";
import { PublishModal } from "./PublishModal";
import { useBuilderWatch } from "./types";

interface PublishButtonProps {
  className?: string;
}

export const PublishButton: React.FC<PublishButtonProps> = ({ className }) => {
  const [isModalOpen, setModalOpen] = useState(false);
  const { currentProject, yamlIsValid, formValuesValid, permission, resolveErrorMessage } =
    useConnectorBuilderFormState();
  const mode = useBuilderWatch("mode");

  let buttonDisabled = permission === "readOnly";
  let showWarningIcon = false;
  let tooltipContent = undefined;

  if (mode === "yaml" && !yamlIsValid) {
    buttonDisabled = true;
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.invalidYamlPublish" />;
  }

  if (mode === "ui" && !formValuesValid) {
    showWarningIcon = true;
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.configErrorsPublish" />;
  }

  if (resolveErrorMessage) {
    buttonDisabled = true;
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.resolveErrorPublish" />;
  }

  const publishButton = (
    <Button
      full
      onClick={() => {
        if (!buttonDisabled) {
          setModalOpen(!isModalOpen);
        }
      }}
      disabled={buttonDisabled}
      data-testid="publish-button"
      {...(showWarningIcon && { type: "warningOutline" })}
      type="button"
    >
      <FormattedMessage
        id={currentProject.sourceDefinitionId ? "connectorBuilder.releaseNewVersion" : "connectorBuilder.publish"}
      />
    </Button>
  );

  return (
    <div className={className}>
      {tooltipContent !== undefined ? (
        <Tooltip
          containerClassName={styles.tooltipContainer}
          control={publishButton}
          placement={mode === "yaml" ? "left" : "top"}
        >
          {tooltipContent}
        </Tooltip>
      ) : (
        publishButton
      )}
      {isModalOpen && (
        <PublishModal
          onClose={() => {
            setModalOpen(false);
          }}
        />
      )}
    </div>
  );
};
