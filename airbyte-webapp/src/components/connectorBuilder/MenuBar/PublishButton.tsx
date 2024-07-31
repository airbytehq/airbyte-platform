import { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./PublishButton.module.scss";
import { PublishModal } from "./PublishModal";
import { useBuilderWatch } from "../types";
import { useStreamTestMetadata } from "../useStreamTestMetadata";

interface PublishButtonProps {
  className?: string;
}

export const PublishButton: React.FC<PublishButtonProps> = ({ className }) => {
  const [isModalOpen, setModalOpen] = useState(false);
  const {
    currentProject,
    yamlIsValid,
    formValuesValid,
    permission,
    resolveErrorMessage,
    streamNames,
    isResolving,
    formValuesDirty,
  } = useConnectorBuilderFormState();
  const {
    streamRead: { isFetching: isReadingStream },
  } = useConnectorBuilderTestRead();
  const mode = useBuilderWatch("mode");

  let buttonDisabled = permission === "readOnly";
  let showWarningIcon = false;
  let tooltipContent = undefined;

  if (isResolving || formValuesDirty || isReadingStream) {
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.resolvingStreamList" />;
  }

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

  const { getStreamTestWarnings } = useStreamTestMetadata();
  const streamsWithWarnings = useMemo(() => {
    return streamNames.filter((streamName) => getStreamTestWarnings(streamName).length > 0);
  }, [getStreamTestWarnings, streamNames]);

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const publishButton = (
    <Button
      full
      onClick={() => {
        if (!buttonDisabled) {
          if (streamsWithWarnings.length > 0) {
            openConfirmationModal({
              title: "connectorBuilder.ignoreWarningsModal.title",
              text: "connectorBuilder.ignoreWarningsModal.text",
              confirmationText: "ignore warnings",
              submitButtonText: "connectorBuilder.ignoreWarningsModal.submit",
              additionalContent: (
                <>
                  <ul>
                    {streamsWithWarnings.map((streamName) => (
                      <li>{streamName}</li>
                    ))}
                  </ul>
                  <FormattedMessage id="connectorBuilder.ignoreWarningsModal.areYouSure" />
                </>
              ),
              onSubmit: () => {
                closeConfirmationModal();
                setModalOpen(true);
              },
            });
          } else {
            setModalOpen(true);
          }
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
      <Tooltip
        containerClassName={styles.tooltipContainer}
        control={publishButton}
        placement={mode === "yaml" ? "left" : "top"}
        disabled={!tooltipContent}
      >
        {tooltipContent}
      </Tooltip>
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
