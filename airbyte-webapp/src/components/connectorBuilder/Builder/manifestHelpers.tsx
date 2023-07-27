import get from "lodash/get";
import { ReactNode } from "react";
import { FormattedMessage } from "react-intl";
import ReactMarkdown from "react-markdown";

import { LabelInfo } from "components/Label";

import { links } from "core/utils/links";

import declarativeComponentSchema from "../../../../build/declarative_component_schema.yaml";

export interface ManifestDescriptor {
  title?: string;
  description?: string;
  examples?: string[];
  interpolation_context?: string[];
  enum?: string[];
}

function getDescriptor(manifestPath: string): ManifestDescriptor | undefined {
  return get(declarativeComponentSchema, `definitions.${manifestPath}`);
}

export function getLabelByManifest(manifestPath: string) {
  return getDescriptor(manifestPath)?.title || manifestPath;
}

export function getOptionsByManifest(manifestPath: string) {
  const options = getDescriptor(manifestPath)?.enum;
  if (!options) {
    throw new Error(`cant get options from manifest: ${manifestPath}`);
  }
  return options;
}

export function getDescriptionByManifest(manifestPath: string) {
  return getDescriptor(manifestPath)?.description;
}

export function getLabelAndTooltip(
  label: string | undefined,
  tooltip: React.ReactNode | undefined,
  manifestPath: string | undefined,
  path: string,
  omitExamples = false,
  omitInterpolationContext = false,
  manifestOptionPaths?: string[]
): { label: string; tooltip: React.ReactNode | undefined } {
  const manifestDescriptor = manifestPath ? getDescriptor(manifestPath) : undefined;
  const finalLabel = label || manifestDescriptor?.title || path;
  let finalDescription: ReactNode = manifestDescriptor?.description ? (
    <ReactMarkdown linkTarget="_blank">{manifestDescriptor?.description}</ReactMarkdown>
  ) : undefined;
  if (!omitInterpolationContext && manifestDescriptor?.interpolation_context) {
    finalDescription = (
      <>
        {finalDescription}
        <br />
        <FormattedMessage id="connectorBuilder.interpolationHeading" />:{" "}
        <ul>
          {manifestDescriptor.interpolation_context.map((context, i) => (
            <li key={i}>
              <a href={`${links.interpolationVariableDocs}#/variables/${context}`} target="_blank" rel="noreferrer">
                {context}
              </a>
            </li>
          ))}
        </ul>
        <FormattedMessage
          id="connectorBuilder.interpolationMacros"
          values={{
            a: (node: React.ReactNode) => (
              <a href={links.interpolationMacroDocs} target="_blank" rel="noreferrer">
                {node}
              </a>
            ),
          }}
        />
      </>
    );
  }
  const options = manifestOptionPaths?.flatMap((optionPath) => {
    const optionDescriptor: ManifestDescriptor | undefined = get(
      declarativeComponentSchema,
      `definitions.${optionPath}`
    );
    if (!optionDescriptor?.title) {
      return [];
    }
    return [
      {
        title: optionDescriptor.title,
        description: optionDescriptor.description,
      },
    ];
  });
  return {
    label: finalLabel,
    tooltip:
      tooltip || finalDescription || options ? (
        <LabelInfo
          label={finalLabel}
          examples={!omitExamples ? manifestDescriptor?.examples : undefined}
          description={tooltip || finalDescription}
          options={options}
        />
      ) : null,
  };
}
