import get from "lodash/get";
import ReactMarkdown from "react-markdown";

import { LabelInfo } from "components/Label";

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

export function getInterpolationVariablesByManifest(manifestPath: string) {
  return getDescriptor(manifestPath)?.interpolation_context ?? undefined;
}

export function getLabelAndTooltip(manifestPath: string): {
  label: string | undefined;
  tooltip: React.ReactNode | undefined;
} {
  const manifestDescriptor = manifestPath ? getDescriptor(manifestPath) : undefined;
  const label = manifestDescriptor?.title;
  return {
    label,
    tooltip: manifestDescriptor?.description ? (
      <LabelInfo
        label={label}
        examples={manifestDescriptor?.examples}
        description={<ReactMarkdown linkTarget="_blank">{manifestDescriptor?.description}</ReactMarkdown>}
      />
    ) : undefined,
  };
}

export interface InterpolationVariable {
  title: string;
  description: string;
  examples: string[] | object[];
}

type InterpolationFunction = InterpolationVariable & {
  arguments: Record<string, string>;
  return_type: string;
};

export type InterpolationValue =
  | (InterpolationVariable & {
      type: "variable";
    })
  | (InterpolationFunction & {
      type: "macro" | "filter";
    });

export interface InterpolationValues {
  variables: InterpolationVariable[];
  macros: InterpolationFunction[];
  filters: InterpolationFunction[];
}
export const getInterpolationValues = (): InterpolationValue[] => {
  const { variables, macros, filters } = get(declarativeComponentSchema, `interpolation`) as InterpolationValues;
  return [
    ...variables.map((variable) => ({ ...variable, type: "variable" as const })),
    ...macros.map((macro) => ({ ...macro, type: "macro" as const })),
    ...filters.map((filter) => ({ ...filter, type: "filter" as const })),
  ];
};
