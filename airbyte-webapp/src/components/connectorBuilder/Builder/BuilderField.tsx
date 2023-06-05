import isEqual from "lodash/isEqual";
import toPath from "lodash/toPath";
import { ReactNode, useEffect, useRef } from "react";
import { useController } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { LabeledSwitch } from "components/LabeledSwitch";
import { CodeEditor } from "components/ui/CodeEditor";
import { ComboBox, Option } from "components/ui/ComboBox";
import DatePicker from "components/ui/DatePicker";
import { DropDown } from "components/ui/DropDown";
import { Input } from "components/ui/Input";
import { TagInput } from "components/ui/TagInput";
import { Text } from "components/ui/Text";
import { TextArea } from "components/ui/TextArea";
import { InfoTooltip } from "components/ui/Tooltip/InfoTooltip";

import { FORM_PATTERN_ERROR } from "core/form/types";
import { useConnectorBuilderFormManagementState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./BuilderField.module.scss";
import { getLabelAndTooltip } from "./manifestHelpers";

interface EnumFieldProps {
  options: string[];
  value: string;
  setValue: (value: string) => void;
  error: boolean;
}

interface ArrayFieldProps {
  name: string;
  value: string[];
  setValue: (value: string[]) => void;
  error: boolean;
  itemType?: string;
}

interface BaseFieldProps {
  // path to the location in the Connector Manifest schema which should be set by this component
  path: string;
  label?: string;
  manifestPath?: string;
  tooltip?: React.ReactNode;
  readOnly?: boolean;
  optional?: boolean;
  pattern?: string;
  adornment?: ReactNode;
  className?: string;
}

export type BuilderFieldProps = BaseFieldProps &
  (
    | {
        type: "string" | "number" | "integer";
        onChange?: (newValue: string) => void;
        onBlur?: (value: string) => void;
        disabled?: boolean;
      }
    | { type: "date" | "date-time"; onChange?: (newValue: string) => void }
    | { type: "boolean"; onChange?: (newValue: boolean) => void }
    | { type: "array"; onChange?: (newValue: string[]) => void; itemType?: string }
    | { type: "textarea"; onChange?: (newValue: string[]) => void }
    | { type: "jsoneditor"; onChange?: (newValue: string[]) => void }
    | { type: "enum"; onChange?: (newValue: string) => void; options: string[] }
    | { type: "combobox"; onChange?: (newValue: string) => void; options: Option[] }
  );

const EnumField: React.FC<EnumFieldProps> = ({ options, value, setValue, error, ...props }) => {
  return (
    <DropDown
      {...props}
      options={options.map((option) => {
        return { label: option, value: option };
      })}
      onChange={(selected) => selected && setValue(selected.value)}
      value={value}
      error={error}
    />
  );
};

const ArrayField: React.FC<ArrayFieldProps> = ({ name, value, setValue, error, itemType }) => {
  return (
    <TagInput name={name} fieldValue={value} onChange={(value) => setValue(value)} itemType={itemType} error={error} />
  );
};

// check whether paths are equal, normalizing [] and . notation
function arePathsEqual(path1: string, path2: string) {
  return isEqual(toPath(path1), toPath(path2));
}

const handleScrollToField = (
  ref: React.RefObject<HTMLDivElement>,
  path: string,
  scrollToField: string | undefined,
  setScrollToField: (value: string | undefined) => void
) => {
  if (ref.current && scrollToField && arePathsEqual(path, scrollToField)) {
    ref.current.scrollIntoView({ block: "center" });
    setScrollToField(undefined);
  }
};

export const BuilderField: React.FC<BuilderFieldProps> = ({
  path,
  optional = false,
  readOnly,
  pattern,
  adornment,
  manifestPath,
  ...props
}) => {
  const { field, fieldState } = useController({ name: path });
  const hasError = !!fieldState.error;

  const { label, tooltip } = getLabelAndTooltip(props.label, props.tooltip, manifestPath, path);
  const { scrollToField, setScrollToField } = useConnectorBuilderFormManagementState();

  const elementRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    // Call handler in here to make sure it handles new scrollToField value from the context
    handleScrollToField(elementRef, path, scrollToField, setScrollToField);
  }, [path, scrollToField, setScrollToField]);

  if (props.type === "boolean") {
    return (
      <LabeledSwitch
        {...field}
        ref={(ref) => {
          elementRef.current = ref;
          // Call handler in here to make sure it handles new refs
          handleScrollToField(elementRef, path, scrollToField, setScrollToField);
        }}
        checked={field.value as boolean}
        label={
          <>
            {label} {tooltip && <InfoTooltip placement="top-start">{tooltip}</InfoTooltip>}
          </>
        }
      />
    );
  }

  const setValue = (newValue: unknown) => {
    props.onChange?.(newValue as string & string[]);
    field.onChange(newValue);
  };

  return (
    <ControlLabels
      className={styles.container}
      label={label}
      infoTooltipContent={tooltip}
      optional={optional}
      ref={(ref) => {
        elementRef.current = ref;
        handleScrollToField(elementRef, path, scrollToField, setScrollToField);
      }}
    >
      {(props.type === "number" || props.type === "string" || props.type === "integer") && (
        <Input
          {...field}
          onChange={(e) => {
            setValue(e.target.value);
          }}
          className={props.className}
          type={props.type}
          value={(field.value as string | number | undefined) ?? ""}
          error={hasError}
          readOnly={readOnly}
          adornment={adornment}
          disabled={props.disabled}
          onBlur={(e) => {
            field.onBlur();
            props.onBlur?.(e.target.value);
          }}
        />
      )}
      {(props.type === "date" || props.type === "date-time") && (
        <DatePicker
          error={hasError}
          withTime={props.type === "date-time"}
          onChange={setValue}
          value={(field.value as string) ?? ""}
          onBlur={field.onBlur}
        />
      )}
      {props.type === "textarea" && (
        <TextArea
          {...field}
          onChange={(e) => {
            setValue(e.target.value);
          }}
          className={props.className}
          value={(field.value as string) ?? ""}
          error={hasError}
          readOnly={readOnly}
          onBlur={field.onBlur}
        />
      )}
      {props.type === "jsoneditor" && (
        <CodeEditor
          height="300px"
          key={path}
          value={field.value || ""}
          language="json"
          theme="airbyte-light"
          onChange={(val: string | undefined) => {
            setValue(val);
          }}
        />
      )}
      {props.type === "array" && (
        <div data-testid={`tag-input-${path}`}>
          <ArrayField
            name={path}
            value={(field.value as string[] | undefined) ?? []}
            itemType={props.itemType}
            setValue={setValue}
            error={hasError}
          />
        </div>
      )}
      {props.type === "enum" && (
        <EnumField
          options={props.options}
          value={field.value as string}
          setValue={setValue}
          error={hasError}
          data-testid={path}
        />
      )}
      {props.type === "combobox" && (
        <ComboBox
          options={props.options}
          value={field.value as string}
          onChange={setValue}
          error={hasError}
          adornment={adornment}
          data-testid={path}
          fieldInputProps={field}
          onBlur={(e) => {
            if (e.relatedTarget?.id.includes("headlessui-combobox-option")) {
              return;
            }
            field.onBlur();
          }}
          filterOptions={false}
        />
      )}
      {hasError && (
        <Text className={styles.error}>
          <FormattedMessage
            id={fieldState.error?.message}
            values={
              fieldState.error?.message === FORM_PATTERN_ERROR && pattern ? { pattern: String(pattern) } : undefined
            }
          />
        </Text>
      )}
    </ControlLabels>
  );
};
