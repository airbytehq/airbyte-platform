import React from "react";
import { useIntl } from "react-intl";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";

import { BuilderField } from "./BuilderField";
import { BUILDER_DECODER_TYPES, DECODER_CONFIGS } from "../types";

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
  streamFieldPath: (fieldPath: string) => string;
}

export const DecoderConfig: React.FC<DecoderConfigProps> = ({ decoderType, streamFieldPath }) => {
  const { formatMessage } = useIntl();
  const config = DECODER_CONFIGS[decoderType];

  if (!config) {
    return null;
  }

  return (
    <GroupControls label={<ControlLabels label={formatMessage({ id: config.title })} />}>
      {config.fields.map((field) => {
        const { key, label, tooltip, ...fieldProps } = field;

        return (
          <BuilderField
            key={key}
            {...fieldProps}
            path={streamFieldPath(`decoder.${key}`)}
            label={formatMessage({ id: label })}
            tooltip={formatMessage({ id: tooltip })}
          />
        );
      })}
    </GroupControls>
  );
};
