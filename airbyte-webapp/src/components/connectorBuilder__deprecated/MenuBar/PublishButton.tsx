import { useCallback, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ButtonProps } from "components/ui/Button";
import { DropdownButton } from "components/ui/DropdownButton";
import { Icon } from "components/ui/Icon";
import { Tooltip } from "components/ui/Tooltip";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import styles from "./PublishButton.module.scss";
import { PublishModal, PublishType } from "./PublishModal";
import { useBuilderWatch } from "../useBuilderWatch";
import { useStreamTestMetadata } from "../useStreamTestMetadata";

interface PublishButtonProps {
  className?: string;
}

export const PublishButton: React.FC<PublishButtonProps> = ({ className }) => {
  const {
    yamlIsValid,
    formValuesValid,
    permission,
    resolveErrorMessage,
    streamNames,
    isResolving,
    formValuesDirty,
    resolvedManifest,
  } = useConnectorBuilderFormState();
  const {
    streamRead: { isFetching: isReadingStream },
  } = useConnectorBuilderTestRead();
  const analyticsService = useAnalyticsService();
  const [openModal, setOpenModal] = useState<PublishType | false>(false);
  const mode = useBuilderWatch("mode");
  const dynamicStreams = useBuilderWatch("formValues.dynamicStreams");

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

  if (resolveErrorMessage) {
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.resolveErrorPublish" />;
  }

  if (resolvedManifest.streams?.length === 0 && resolvedManifest.dynamic_streams?.length === 0) {
    buttonDisabled = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.noStreamsPublish" />;
  }

  const { getStreamTestWarnings } = useStreamTestMetadata();
  const streamsWithWarnings = useMemo(() => {
    return streamNames
      .filter((_, index) => getStreamTestWarnings({ type: "stream", index }).length > 0)
      .map((streamName) => streamName);
  }, [getStreamTestWarnings, streamNames]);
  const dynamicStreamsWithWarnings = useMemo(() => {
    return dynamicStreams
      .filter((_, index) => getStreamTestWarnings({ type: "dynamic_stream", index }).length > 0)
      .map(({ dynamicStreamName }) => dynamicStreamName);
  }, [getStreamTestWarnings, dynamicStreams]);
  const namesWithWarnings = useMemo(() => {
    return [...dynamicStreamsWithWarnings, ...streamsWithWarnings];
  }, [streamsWithWarnings, dynamicStreamsWithWarnings]);

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const openPublishModal = useCallback(
    (publishType: PublishType) => {
      if (namesWithWarnings.length > 0) {
        openConfirmationModal({
          title: "connectorBuilder.ignoreWarningsModal.title",
          text: "connectorBuilder.ignoreWarningsModal.text",
          confirmationText: "ignore warnings",
          submitButtonText: "connectorBuilder.ignoreWarningsModal.submit",
          additionalContent: (
            <>
              <ul>
                {namesWithWarnings.map((streamName) => (
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
    [closeConfirmationModal, openConfirmationModal, namesWithWarnings]
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
  const isMarketplaceContributionActionDisabled = namesWithWarnings.length > 0;
  const publishButton = (
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
            displayName: formatMessage({ id: "connectorBuilder.publishModal.toAirbyte.label" }),
            value: "marketplace",
            disabled: isMarketplaceContributionActionDisabled,
            tooltipContent: isMarketplaceContributionActionDisabled ? (
              <FormattedMessage id="connectorBuilder.publishModal.toAirbyte.disabledDescription" />
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
