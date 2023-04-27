import { useField } from "formik";
import { useIntl } from "react-intl";

import { ControlLabels } from "components/LabeledControl";

import { links } from "utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { getDescriptionByManifest, getLabelByManifest } from "./manifestHelpers";
import { BuilderStream } from "../types";

interface PartitionSectionProps {
  streamFieldPath: (fieldPath: string) => string;
  currentStreamIndex: number;
}

export const TransformationSection: React.FC<PartitionSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();
  const [field, , helpers] = useField<BuilderStream["transformations"]>(streamFieldPath("transformations"));

  const handleToggle = (newToggleValue: boolean) => {
    if (newToggleValue) {
      helpers.setValue([
        {
          type: "remove",
          path: [],
        },
      ]);
    } else {
      helpers.setValue(undefined);
    }
  };

  const getTransformationOptions = (buildPath: (path: string) => string): OneOfOption[] => [
    {
      label: "Remove field",
      typeValue: "remove",
      default: {
        path: [],
      },
      children: (
        <BuilderField type="array" path={buildPath("path")} label="Path" tooltip="Path to the field to remove" />
      ),
    },
    {
      label: "Add field",
      typeValue: "add",
      default: {
        value: "",
        path: [],
      },
      children: (
        <>
          <BuilderField type="array" path={buildPath("path")} manifestPath="AddedFieldDefinition.properties.path" />
          <BuilderFieldWithInputs
            type="string"
            path={buildPath("value")}
            manifestPath="AddedFieldDefinition.properties.value"
          />
        </>
      ),
    },
  ];
  const toggledOn = field.value !== undefined;

  return (
    <BuilderCard
      docLink={links.connectorBuilderTransformations}
      label={
        <ControlLabels
          label={getLabelByManifest("DeclarativeStream.properties.transformations")}
          infoTooltipContent={getDescriptionByManifest("DeclarativeStream.properties.transformations")}
        />
      }
      toggleConfig={{
        toggledOn,
        onToggle: handleToggle,
      }}
      copyConfig={{
        path: "transformations",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromTransformationTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToTransformationTitle" }),
      }}
    >
      <BuilderList
        addButtonLabel={formatMessage({ id: "connectorBuilder.addNewTransformation" })}
        basePath={streamFieldPath("transformations")}
        emptyItem={{
          type: "remove",
          path: [],
        }}
      >
        {({ buildPath }) => (
          <BuilderOneOf
            path={buildPath("")}
            label="Transformation"
            tooltip="Add or remove a field"
            options={getTransformationOptions(buildPath)}
          />
        )}
      </BuilderList>
    </BuilderCard>
  );
};
