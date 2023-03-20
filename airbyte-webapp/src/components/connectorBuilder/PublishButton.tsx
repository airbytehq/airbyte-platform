import { faWarning } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { useField } from "formik";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { ToastType } from "components/ui/Toast";
import { Tooltip } from "components/ui/Tooltip";

import { useNotificationService } from "hooks/services/Notification";
import { usePublishProject } from "services/connectorBuilder/ConnectorBuilderProjectsService";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { useBuilderErrors } from "./useBuilderErrors";

interface PublishButtonProps {
  className?: string;
}

const NOTIFICATION_ID = "connectorBuilder.publish";

export const PublishButton: React.FC<PublishButtonProps> = ({ className }) => {
  const { registerNotification } = useNotificationService();
  const { editorView, projectId, lastValidJsonManifest, yamlIsValid } = useConnectorBuilderFormState();
  const { mutateAsync: sendPublishRequest, isLoading } = usePublishProject();
  const { hasErrors, validateAndTouch } = useBuilderErrors();
  const [connectorNameField] = useField<string>("global.connectorName");

  const publish = async () => {
    try {
      const response = await sendPublishRequest({
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        manifest: lastValidJsonManifest!,
        name: connectorNameField.value,
        projectId,
      });
      registerNotification({
        id: NOTIFICATION_ID,
        text: <FormattedMessage id="connectorBuilder.publishedMessage" values={{ id: response.sourceDefinitionId }} />,
        type: ToastType.SUCCESS,
      });
    } catch (e) {
      registerNotification({
        id: NOTIFICATION_ID,
        text: <FormattedMessage id="form.error" values={{ message: e.message }} />,
        type: ToastType.ERROR,
      });
    }
  };

  const handleClick = () => {
    if (editorView === "yaml") {
      publish();
      return;
    }

    validateAndTouch(publish);
  };

  let buttonDisabled = false;
  let showWarningIcon = false;
  let tooltipContent = undefined;

  if (editorView === "yaml" && !yamlIsValid) {
    buttonDisabled = true;
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.invalidYamlPublish" />;
  }

  if (editorView === "ui" && hasErrors(true)) {
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.configErrorsPublish" />;
  }

  const publishButton = (
    <Button
      full
      onClick={handleClick}
      disabled={buttonDisabled || isLoading}
      isLoading={isLoading}
      icon={showWarningIcon ? <FontAwesomeIcon icon={faWarning} /> : undefined}
    >
      <FormattedMessage id="connectorBuilder.publish" />
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
    </div>
  );
};
