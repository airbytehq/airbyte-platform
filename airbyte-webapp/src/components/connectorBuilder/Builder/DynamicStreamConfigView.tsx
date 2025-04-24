import React, { useCallback } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { BuilderView } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderCard } from "./BuilderCard";
import { BuilderConfigView } from "./BuilderConfigView";
import { BuilderField } from "./BuilderField";
import { getDescriptionByManifest, getLabelByManifest } from "./manifestHelpers";
import styles from "./StreamConfigView.module.scss";
import { manifestRecordSelectorToBuilder } from "../convertManifestToBuilderForm";
import { builderRecordSelectorToManifest, DynamicStreamPathFn } from "../types";
import { useBuilderWatch } from "../useBuilderWatch";

interface DynamicStreamConfigViewProps {
  streamNum: number;
}

export const DynamicStreamConfigView: React.FC<DynamicStreamConfigViewProps> = ({ streamNum }) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const analyticsService = useAnalyticsService();
  const { setValue } = useFormContext();
  const { formatMessage } = useIntl();
  const baseUrl = useBuilderWatch("formValues.global.urlBase");
  const dynamicStreamFieldPath: DynamicStreamPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.dynamicStreams.${streamNum}.${fieldPath}` as const,
    [streamNum]
  );

  const dynamicStreams = useBuilderWatch("formValues.dynamicStreams");

  const handleDelete = () => {
    openConfirmationModal({
      text: "connectorBuilder.deleteDynamicStreamModal.text",
      title: "connectorBuilder.deleteDynamicStreamModal.title",
      submitButtonText: "connectorBuilder.deleteDynamicStreamModal.submitButton",
      onSubmit: () => {
        const updatedStreams = dynamicStreams.filter((_, index) => index !== streamNum);
        const streamToSelect = streamNum >= updatedStreams.length ? updatedStreams.length - 1 : streamNum;
        const viewToSelect: BuilderView =
          updatedStreams.length === 0 ? { type: "global" } : { type: "dynamic_stream", index: streamToSelect };
        setValue("formValues.dynamicStreams", updatedStreams);
        setValue("view", viewToSelect);
        closeConfirmationModal();
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DYNAMIC_STREAM_DELETE, {
          actionDescription: "Dynamic stream deleted",
          dynamic_stream_name: dynamicStreams[streamNum].dynamicStreamName,
        });
      },
    });
  };

  return (
    <BuilderConfigView>
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
    </BuilderConfigView>
  );
};
