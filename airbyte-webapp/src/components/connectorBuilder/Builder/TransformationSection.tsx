import { useField } from "formik";
import { useIntl } from "react-intl";

import { ControlLabels } from "components/LabeledControl";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
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
      helpers.setValue([]);
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
          <BuilderField type="array" path={buildPath("path")} label="Path" tooltip="Path to the field to add" />
          <BuilderFieldWithInputs
            type="string"
            path={buildPath("value")}
            label="Value"
            tooltip="Value of the new field (use {{ record.existing_field }} syntax to reference to other fields in the same record"
          />
        </>
      ),
    },
  ];
  const toggledOn = field.value !== undefined;

  return (
    <BuilderCard
      toggleConfig={{
        label: (
          <ControlLabels
            label="Transformations"
            infoTooltipContent="Transform records before sending them to the destination by removing or changing fields."
          />
        ),
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
