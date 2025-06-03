import React from "react";
import { useIntl } from "react-intl";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";

import { CsvDecoderType, GzipDecoderType, JsonlDecoderType, JsonDecoderType } from "core/api/types/ConnectorManifest";

import { BuilderField } from "./BuilderField";
import { BUILDER_DECODER_TYPES, BuilderDecoderConfig, DECODER_CONFIGS } from "../types";
import { useBuilderWatch } from "../useBuilderWatch";

export interface DecoderTypeConfig {
  title: string;
  fields: Array<{
    key: string;
    type: "string"; // Extend this to support additional types as needed
    label: string;
    tooltip: string;
    manifestPath?: string;
    placeholder?: string;
    optional?: boolean;
  }>;
}

interface DecoderConfigProps {
  decoderType: (typeof BUILDER_DECODER_TYPES)[number];
  decoderFieldPath: (fieldPath: string) => string;
}

export const DecoderConfig: React.FC<DecoderConfigProps> = ({ decoderType, decoderFieldPath }) => {
  const { formatMessage } = useIntl();
  const config = DECODER_CONFIGS[decoderType];
  const nestedDecoderType = useBuilderWatch(decoderFieldPath("decoder.type")) as BuilderDecoderConfig["type"];

  if (!config) {
    return null;
  }

  return (
    <GroupControls label={<ControlLabels label={formatMessage({ id: config.title })} />}>
      {config.fields.map((field) => {
        const { key, label, tooltip, ...fieldProps } = field;

        if (key === "decoder") {
          return (
            <>
              <BuilderField
                type="enum"
                label={formatMessage({
                  id: "connectorBuilder.decoder.nestedDecoder.label",
                })}
                tooltip={formatMessage({
                  id: "connectorBuilder.decoder.nestedDecoder.tooltip",
                })}
                path={decoderFieldPath("decoder.type")}
                options={["JSON", "JSON Lines", "CSV", "gzip"]}
                manifestPath="SimpleRetriever.properties.decoder.properties.decoder"
                manifestOptionPaths={[
                  JsonDecoderType.JsonDecoder,
                  JsonlDecoderType.JsonlDecoder,
                  CsvDecoderType.CsvDecoder,
                  GzipDecoderType.GzipDecoder,
                ]}
              />
              {nestedDecoderType && (
                <DecoderConfig
                  decoderType={nestedDecoderType}
                  decoderFieldPath={(fieldPath: string) => `${decoderFieldPath("decoder")}.${fieldPath}`}
                />
              )}
            </>
          );
        }

        return (
          <BuilderField
            key={key}
            {...fieldProps}
            path={decoderFieldPath(key)}
            label={formatMessage({ id: label })}
            tooltip={formatMessage({ id: tooltip })}
          />
        );
      })}
    </GroupControls>
  );
};
