import { useIntl } from "react-intl";

import { AssistButton } from "components/connectorBuilder/Builder/Assist/AssistButton";

import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { getDescriptionByManifest, getLabelByManifest } from "./manifestHelpers";
import { manifestRecordSelectorToBuilder } from "../convertManifestToBuilderForm";
import { StreamPathFn, builderRecordSelectorToManifest } from "../types";

interface RecordSelectorSectionProps {
  streamFieldPath: StreamPathFn;
  currentStreamIndex: number;
}

export const RecordSelectorSection: React.FC<RecordSelectorSectionProps> = ({
  streamFieldPath,
  currentStreamIndex,
}) => {
  const { formatMessage } = useIntl();
  const label = getLabelByManifest("RecordSelector");

  return (
    <BuilderCard
      docLink={links.connectorBuilderRecordSelector}
      label={label}
      tooltip={getDescriptionByManifest("RecordSelector")}
      labelAction={<AssistButton assistKey="record_selector" streamNum={currentStreamIndex} />}
      inputsConfig={{
        toggleable: true,
        path: streamFieldPath("recordSelector"),
        defaultValue: {
          fieldPath: [],
          normalizeToSchema: false,
        },
        yamlConfig: {
          builderToManifest: builderRecordSelectorToManifest,
          manifestToBuilder: manifestRecordSelectorToBuilder,
        },
      }}
      copyConfig={{
        path: "recordSelector",
        currentStreamIndex,
        componentName: label,
      }}
    >
      <BuilderField
        type="array"
        path={streamFieldPath("recordSelector.fieldPath")}
        manifestPath="DpathExtractor.properties.field_path"
        optional
      />
      <BuilderField
        type="jinja"
        path={streamFieldPath("recordSelector.filterCondition")}
        label={getLabelByManifest("RecordFilter")}
        manifestPath="RecordFilter.properties.condition"
        pattern={formatMessage({ id: "connectorBuilder.condition.pattern" })}
        optional
      />
      <BuilderField
        type="boolean"
        path={streamFieldPath("recordSelector.normalizeToSchema")}
        label={formatMessage({ id: "connectorBuilder.recordSelector.normalizeToSchema.label" })}
        tooltip={formatMessage({ id: "connectorBuilder.recordSelector.normalizeToSchema.tooltip" })}
      />
    </BuilderCard>
  );
};
