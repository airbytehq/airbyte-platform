import { faWarning } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { PublishModal } from "./PublishModal";

interface PublishButtonProps {
  className?: string;
}

export const PublishButton: React.FC<PublishButtonProps> = ({ className }) => {
  const [isModalOpen, setModalOpen] = useState(false);
  const { editorView, currentProject, yamlIsValid, formValuesValid } = useConnectorBuilderFormState();

  const { streamListErrorMessage } = useConnectorBuilderTestRead();

  let buttonDisabled = false;
  let showWarningIcon = false;
  let tooltipContent = undefined;

  if (editorView === "yaml" && !yamlIsValid) {
    buttonDisabled = true;
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.invalidYamlPublish" />;
  }

  if (editorView === "ui" && !formValuesValid) {
    showWarningIcon = true;
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.configErrorsPublish" />;
  }

  if (streamListErrorMessage) {
    buttonDisabled = true;
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.listErrorPublish" />;
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
      icon={showWarningIcon ? <FontAwesomeIcon icon={faWarning} /> : undefined}
    >
      <FormattedMessage
        id={currentProject.sourceDefinitionId ? "connectorBuilder.releaseNewVersion" : "connectorBuilder.publish"}
      />
    </Button>
  );

  return (
    <div className={className}>
      {tooltipContent !== undefined ? (
        <Tooltip control={publishButton} placement={editorView === "yaml" ? "left" : "top"}>
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
