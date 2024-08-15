import { useCallback, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button, ButtonProps } from "components/ui/Button";
import { DropdownButton } from "components/ui/DropdownButton";
import { Icon } from "components/ui/Icon";
import { Tooltip } from "components/ui/Tooltip";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useExperiment } from "hooks/services/Experiment";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./PublishButton.module.scss";
import { PublishModal, PublishType } from "./PublishModal";
import { useBuilderWatch } from "../types";
import { useStreamTestMetadata } from "../useStreamTestMetadata";

interface PublishButtonProps {
  className?: string;
}

export const PublishButton: React.FC<PublishButtonProps> = ({ className }) => {
  const { yamlIsValid, formValuesValid, permission, resolveError, streamNames, isResolving, formValuesDirty } =
    useConnectorBuilderFormState();
  const {
    streamRead: { isFetching: isReadingStream },
  } = useConnectorBuilderTestRead();
  const analyticsService = useAnalyticsService();
  const [openModal, setOpenModal] = useState<PublishType | false>(false);
  const mode = useBuilderWatch("mode");

  let buttonDisabled = permission === "readOnly";
  let tooltipContent = undefined;

  if (isResolving || formValuesDirty || isReadingStream) {
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.resolvingStreamList" />;
  }

  if (mode === "yaml" && !yamlIsValid) {
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.invalidYamlPublish" />;
  }

  if (mode === "ui" && !formValuesValid) {
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.configErrorsPublish" />;
  }

  if (resolveError) {
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.resolveErrorPublish" />;
  }

  const { getStreamTestWarnings } = useStreamTestMetadata();
  const streamsWithWarnings = useMemo(() => {
    return streamNames.filter((streamName) => getStreamTestWarnings(streamName).length > 0);
  }, [getStreamTestWarnings, streamNames]);

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const openPublishModal = useCallback(
    (publishType: PublishType) => {
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
                  <li key={streamName}>{streamName}</li>
                ))}
              </ul>
              <FormattedMessage id="connectorBuilder.ignoreWarningsModal.areYouSure" />
            </>
          ),
          onSubmit: () => {
            closeConfirmationModal();
            setOpenModal(publishType);
          },
        });
      } else {
        setOpenModal(publishType);
      }
    },
    [closeConfirmationModal, openConfirmationModal, streamsWithWarnings]
  );

  const handleClick = () => {
    if (buttonDisabled) {
      return;
    }

    openPublishModal("workspace");
  };

  const buttonProps: ButtonProps = {
    full: true,
    onClick: handleClick,
    disabled: buttonDisabled,
    "data-testid": "publish-button",
    type: "button",
  };
  const { formatMessage } = useIntl();
  const isMarketplaceContributionFeatureEnabled = useExperiment("connectorBuilder.contributeToMarketplace", true);
  const isMarketplaceContributionActionDisabled = streamsWithWarnings.length > 0;
  const publishButton = isMarketplaceContributionFeatureEnabled ? (
    <DropdownButton
      {...buttonProps}
      dropdown={{
        options: [
          {
            icon: <Icon size="sm" type="import" />,
            displayName: formatMessage({ id: "connectorBuilder.publishModal.toWorkspace.label" }),
            value: "workspace",
          },
          {
            icon: <Icon size="sm" type="github" />,
            displayName: formatMessage({ id: "connectorBuilder.publishModal.toMarketplace.label" }),
            value: "marketplace",
            disabled: isMarketplaceContributionActionDisabled,
            tooltipContent: isMarketplaceContributionActionDisabled ? (
              <FormattedMessage id="connectorBuilder.publishModal.toMarketplace.disabledDescription" />
            ) : null,
          },
        ],
        textSize: "md",
        onSelect: (option) => {
          const publishType = option.value as PublishType;
          openPublishModal(publishType);
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.PUBLISH_DROPDOWN_SELECTED, {
            actionDescription: "An option in the Publish button dropdown menu was selected",
            selectedPublishType: publishType,
          });
        },
      }}
    >
      <FormattedMessage id="connectorBuilder.publish" />
    </DropdownButton>
  ) : (
    <Button {...buttonProps}>
      <FormattedMessage id="connectorBuilder.publish" />
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
      {openModal && (
        <PublishModal
          initialPublishType={openModal}
          onClose={() => {
            setOpenModal(false);
          }}
        />
      )}
    </div>
  );
};
