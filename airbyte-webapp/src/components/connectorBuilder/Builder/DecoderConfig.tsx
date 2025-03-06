import React, { PropsWithChildren } from "react";
import { useIntl } from "react-intl";

import { BuilderCardProps } from "./BuilderCard";
import { BuilderFieldProps } from "./BuilderField";
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
  currentStreamIndex: number;
  BuilderCard: React.ComponentType<PropsWithChildren<BuilderCardProps>>;
  BuilderField: React.ComponentType<BuilderFieldProps>;
}

export const DecoderConfig: React.FC<DecoderConfigProps> = ({
  decoderType,
  streamFieldPath,
  currentStreamIndex,
  BuilderCard,
  BuilderField,
}) => {
  const { formatMessage } = useIntl();
  const config = DECODER_CONFIGS[decoderType];

  if (!config) {
    return null;
  }

  return (
    <BuilderCard
      label={formatMessage({ id: config.title })}
      copyConfig={{
        path: "decoder.config",
        currentStreamIndex,
        componentName: formatMessage({ id: config.title }),
      }}
    >
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
    </BuilderCard>
  );
};
