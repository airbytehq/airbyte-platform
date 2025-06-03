import React, { useCallback } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { SchemaFormControl } from "components/forms/SchemaForm/Controls/SchemaFormControl";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { InfoTooltip } from "components/ui/Tooltip";

import { DynamicDeclarativeStream } from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { BuilderView } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderConfigView } from "./BuilderConfigView";
import styles from "./DynamicStreamConfigView.module.scss";
import { StreamConfigView } from "./StreamConfigView";
import { getStreamFieldPath, StreamId } from "../types";

interface DynamicStreamConfigViewProps {
  streamId: StreamId;
}

export const DynamicStreamConfigView: React.FC<DynamicStreamConfigViewProps> = ({ streamId }) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const analyticsService = useAnalyticsService();
  const { setValue, getValues } = useFormContext();
  const { formatMessage } = useIntl();

  const dynamicStreamFieldPath = useCallback(
    (fieldPath?: string) => getStreamFieldPath(streamId, fieldPath),
    [streamId]
  );

  const handleDelete = useCallback(() => {
    openConfirmationModal({
      text: "connectorBuilder.deleteDynamicStreamModal.text",
      title: "connectorBuilder.deleteDynamicStreamModal.title",
      submitButtonText: "connectorBuilder.deleteDynamicStreamModal.submitButton",
      onSubmit: () => {
        const dynamicStreams: DynamicDeclarativeStream[] = getValues("manifest.dynamic_streams");
        const updatedStreams = dynamicStreams.filter((_, index) => index !== streamId.index);
        const streamToSelect = streamId.index >= updatedStreams.length ? updatedStreams.length - 1 : streamId.index;
        const viewToSelect: BuilderView =
          updatedStreams.length === 0 ? { type: "global" } : { type: "dynamic_stream", index: streamToSelect };
        setValue("manifest.dynamic_streams", updatedStreams);
        setValue("view", viewToSelect);
        closeConfirmationModal();
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DYNAMIC_STREAM_DELETE, {
          actionDescription: "Dynamic stream deleted",
          dynamic_stream_name: dynamicStreams[streamId.index].name,
        });
      },
    });
  }, [analyticsService, closeConfirmationModal, getValues, openConfirmationModal, setValue, streamId]);

  const templateHeaderRef = React.useRef<HTMLDivElement>(null);
  const scrollToTopOfTemplate = useCallback(() => {
    if (templateHeaderRef.current) {
      templateHeaderRef.current.scrollIntoView({ behavior: "auto" });
    }
  }, []);

  if (streamId.type !== "dynamic_stream") {
    return null;
  }

  return (
    <BuilderConfigView className={styles.fullHeight}>
      <FlexContainer direction="column" gap="2xl" className={styles.fullHeight}>
        <FlexContainer justifyContent="space-between" alignItems="center">
          <SchemaFormControl
            path={dynamicStreamFieldPath("name")}
            titleOverride={null}
            className={styles.streamNameInput}
            placeholder={formatMessage({ id: "connectorBuilder.streamTemplateName.placeholder" })}
          />
          <Button variant="danger" onClick={handleDelete}>
            <FormattedMessage id="connectorBuilder.deleteDynamicStreamModal.title" />
          </Button>
        </FlexContainer>

        <FlexContainer direction="column" gap="md">
          <FlexContainer gap="none">
            <Heading as="h2" size="sm">
              <FormattedMessage id="connectorBuilder.dynamicStream.resolver.header" />
            </Heading>
            <InfoTooltip placement="top">
              <FormattedMessage id="connectorBuilder.dynamicStream.resolver.tooltip" />
            </InfoTooltip>
          </FlexContainer>

          <Card>
            <SchemaFormControl
              path={dynamicStreamFieldPath("components_resolver")}
              nonAdvancedFields={NON_ADVANCED_RESOLVER_FIELDS}
            />
          </Card>
        </FlexContainer>

        <FlexContainer direction="column" gap="none" className={styles.fullHeight}>
          <FlexContainer gap="none" ref={templateHeaderRef}>
            <Heading as="h2" size="sm">
              <FormattedMessage id="connectorBuilder.dynamicStream.template.header" />
            </Heading>
            <InfoTooltip placement="top">
              <FormattedMessage id="connectorBuilder.dynamicStream.template.tooltip" />
            </InfoTooltip>
          </FlexContainer>
          <StreamConfigView streamId={streamId} scrollToTop={scrollToTopOfTemplate} />
        </FlexContainer>
      </FlexContainer>
    </BuilderConfigView>
  );
};

const NON_ADVANCED_RESOLVER_FIELDS = [
  "components_mapping",
  "retriever.requester.url_base",
  "retriever.requester.path",
  "retriever.requester.url",
  "retriever.requester.http_method",
  "retriever.requester.authenticator",
  "retriever.record_selector.extractor",
  "stream_config",
];
