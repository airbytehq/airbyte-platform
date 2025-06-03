import React, { useCallback } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { InfoTooltip } from "components/ui/Tooltip";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { BuilderView } from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import { BuilderCard } from "./BuilderCard";
import { BuilderConfigView } from "./BuilderConfigView";
import { BuilderField } from "./BuilderField";
import styles from "./DynamicStreamConfigView.module.scss";
import { getDescriptionByManifest, getLabelByManifest } from "./manifestHelpers";
import { StreamConfigView } from "./StreamConfigView";
import { manifestRecordSelectorToBuilder } from "../convertManifestToBuilderForm";
import { builderRecordSelectorToManifest, DynamicStreamPathFn, StreamId } from "../types";
import { useBuilderWatch } from "../useBuilderWatch";

interface DynamicStreamConfigViewProps {
  streamId: StreamId;
  scrollToTop: () => void;
}

export const DynamicStreamConfigView: React.FC<DynamicStreamConfigViewProps> = ({ streamId, scrollToTop }) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const analyticsService = useAnalyticsService();
  const { setValue } = useFormContext();
  const { formatMessage } = useIntl();
  const baseUrl = useBuilderWatch("formValues.global.urlBase");
  const dynamicStreamFieldPath: DynamicStreamPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.dynamicStreams.${streamId.index}.${fieldPath}` as const,
    [streamId.index]
  );

  const dynamicStreams = useBuilderWatch("formValues.dynamicStreams");

  const handleDelete = () => {
    openConfirmationModal({
      text: "connectorBuilder.deleteDynamicStreamModal.text",
      title: "connectorBuilder.deleteDynamicStreamModal.title",
      submitButtonText: "connectorBuilder.deleteDynamicStreamModal.submitButton",
      onSubmit: () => {
        const updatedStreams = dynamicStreams.filter((_, index) => index !== streamId.index);
        const streamToSelect = streamId.index >= updatedStreams.length ? updatedStreams.length - 1 : streamId.index;
        const viewToSelect: BuilderView =
          updatedStreams.length === 0 ? { type: "global" } : { type: "dynamic_stream", index: streamToSelect };
        setValue("formValues.dynamicStreams", updatedStreams);
        setValue("view", viewToSelect);
        closeConfirmationModal();
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DYNAMIC_STREAM_DELETE, {
          actionDescription: "Dynamic stream deleted",
          dynamic_stream_name: dynamicStreams[streamId.index].dynamicStreamName,
        });
      },
    });
  };

  if (streamId.type !== "dynamic_stream") {
    return null;
  }

  return (
    <BuilderConfigView className={styles.fullHeight}>
      <FlexContainer direction="column" gap="2xl" className={styles.fullHeight}>
        <FlexContainer justifyContent="space-between" alignItems="center">
          <BuilderField
            type="string"
            path={dynamicStreamFieldPath("dynamicStreamName")}
            containerClassName={styles.streamNameInput}
          />
          <Button variant="danger" onClick={handleDelete}>
            <FormattedMessage id="connectorBuilder.deleteDynamicStreamModal.title" />
          </Button>
        </FlexContainer>

        <FlexContainer direction="column" gap="md">
          <FlexContainer gap="none">
            <Heading as="h2" size="sm">
              <FormattedMessage id="connectorBuilder.dynamicStream.retriever.header" />
            </Heading>
            <InfoTooltip placement="top">
              <FormattedMessage id="connectorBuilder.dynamicStream.retriever.tooltip" />
            </InfoTooltip>
          </FlexContainer>

          <BuilderCard>
            <BuilderField
              type="jinja"
              path={dynamicStreamFieldPath("componentsResolver.retriever.requester.path")}
              manifestPath="HttpRequester.properties.path"
              preview={baseUrl ? (value) => `${baseUrl}${value}` : undefined}
            />
          </BuilderCard>
          <BuilderCard
            docLink={links.connectorBuilderRecordSelector}
            label={getLabelByManifest("RecordSelector")}
            tooltip={getDescriptionByManifest("RecordSelector")}
            inputsConfig={{
              toggleable: false,
              path: dynamicStreamFieldPath("componentsResolver.retriever.record_selector"),
              defaultValue: {
                fieldPath: [],
                normalizeToSchema: false,
              },
              yamlConfig: {
                builderToManifest: builderRecordSelectorToManifest,
                manifestToBuilder: manifestRecordSelectorToBuilder,
              },
            }}
          >
            <BuilderField
              type="array"
              path={dynamicStreamFieldPath("componentsResolver.retriever.record_selector.extractor.field_path")}
              manifestPath="DpathExtractor.properties.field_path"
              optional
            />
            <BuilderField
              type="jinja"
              path={dynamicStreamFieldPath("componentsResolver.retriever.record_selector.record_filter.condition")}
              label={getLabelByManifest("RecordFilter")}
              manifestPath="RecordFilter.properties.condition"
              pattern={formatMessage({ id: "connectorBuilder.condition.pattern" })}
              optional
            />
          </BuilderCard>
        </FlexContainer>

        <FlexContainer direction="column" gap="none" className={styles.fullHeight}>
          <FlexContainer gap="none">
            <Heading as="h2" size="sm">
              <FormattedMessage id="connectorBuilder.dynamicStream.template.header" />
            </Heading>
            <InfoTooltip placement="top">
              <FormattedMessage id="connectorBuilder.dynamicStream.template.tooltip" />
            </InfoTooltip>
          </FlexContainer>
          <StreamConfigView streamId={streamId} scrollToTop={scrollToTop} />
        </FlexContainer>
      </FlexContainer>
    </BuilderConfigView>
  );
};
