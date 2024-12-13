import classNames from "classnames";
import { ReactNode, useEffect, useRef } from "react";
import { useController } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { LabeledSwitch } from "components/LabeledSwitch";
import { CodeEditor } from "components/ui/CodeEditor";
import { ComboBox, OptionsConfig, MultiComboBox, Option } from "components/ui/ComboBox";
import DatePicker from "components/ui/DatePicker";
import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";
import { TagInput } from "components/ui/TagInput";
import { Text } from "components/ui/Text";
import { TextArea } from "components/ui/TextArea";
import { Tooltip } from "components/ui/Tooltip";

import { FORM_PATTERN_ERROR } from "core/form/types";
import { useConnectorBuilderFormManagementState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./BuilderField.module.scss";
import { JinjaInput } from "./JinjaInput";
import { getLabelAndTooltip } from "./manifestHelpers";
import { useWatchWithPreview } from "../preview";

interface EnumFieldProps {
  options: string[] | Array<{ label: string; value: string }>;
  value: string;
  setValue: (value: string) => void;
  error: boolean;
  disabled?: boolean;
}

interface ArrayFieldProps {
  name: string;
  value: string[];
  setValue: (value: string[]) => void;
  error: boolean;
  itemType?: string;
  directionalStyle?: boolean;
  uniqueValues?: boolean;
  disabled?: boolean;
}

interface BaseFieldProps {
  // path to the location in the Connector Manifest schema which should be set by this component
  path: string;
  label?: string;
  manifestPath?: string;
  manifestOptionPaths?: string[];
  tooltip?: React.ReactNode;
  readOnly?: boolean;
  optional?: boolean;
  pattern?: string;
  adornment?: ReactNode;
  preview?: (formValue: string) => ReactNode;
  labelAction?: ReactNode;
  className?: string;
  disabled?: boolean;
}

export type BuilderFieldProps = BaseFieldProps &
  (
    | {
        type: "jinja";
        onChange?: (newValue: string) => void;
        onBlur?: (value: string) => void;
        bubbleUpUndoRedo?: boolean;
      }
    | {
        type: "string" | "number" | "integer";
        onChange?: (newValue: string) => void;
        onBlur?: (value: string) => void;
        step?: number;
        min?: number;
        placeholder?: string;
      }
    | { type: "date" | "date-time"; onChange?: (newValue: string) => void }
    | { type: "boolean"; onChange?: (newValue: boolean) => void; disabledTooltip?: string }
    | {
        type: "array";
        onChange?: (newValue: string[]) => void;
        itemType?: string;
        directionalStyle?: boolean;
        uniqueValues?: boolean;
      }
    | { type: "textarea"; onChange?: (newValue: string[]) => void }
    | { type: "jsoneditor"; onChange?: (newValue: string[]) => void }
    | {
        type: "enum";
        onChange?: (newValue: string) => void;
        options: string[] | Array<{ label: string; value: string }>;
      }
    | { type: "combobox"; onChange?: (newValue: string) => void; options: Option[]; optionsConfig?: OptionsConfig }
    | { type: "multicombobox"; onChange?: (newValue: string[]) => void; options: Option[] }
  );

const EnumField: React.FC<EnumFieldProps> = ({ options, value, setValue, error, disabled, ...props }) => {
  return (
    <ListBox
      {...props}
      options={
        typeof options[0] === "string"
          ? (options as string[]).map((option) => {
              return { label: option, value: option };
            })
          : (options as Array<{ label: string; value: string }>)
      }
      onSelect={(selected) => selected && setValue(selected)}
      selectedValue={value}
      hasError={error}
      isDisabled={disabled}
    />
  );
};

const ArrayField: React.FC<ArrayFieldProps> = ({
  name,
  value,
  setValue,
  error,
  itemType,
  directionalStyle,
  uniqueValues,
  disabled,
}) => {
  return (
    <TagInput
      name={name}
      fieldValue={value}
      onChange={(value) => setValue(value)}
      itemType={itemType}
      error={error}
      directionalStyle={directionalStyle}
      uniqueValues={uniqueValues}
      disabled={disabled}
    />
  );
};

const InnerBuilderField: React.FC<BuilderFieldProps> = ({
  path,
  optional = false,
  readOnly,
  pattern,
  adornment,
  preview,
  manifestPath,
  manifestOptionPaths,
  labelAction,
  ...props
}) => {
  const { field, fieldState } = useController({ name: path });
  // Must use useWatch instead of field.value from useController because the latter is not updated
  // when setValue is called on a parent path in a way that changes the value of this field.
  const { fieldValue, isPreview } = useWatchWithPreview({ name: path });

  const isPreviewRef = useRef(isPreview);
  isPreviewRef.current = isPreview;

  const hasError = !!fieldState.error;

  const { label, tooltip } = getLabelAndTooltip(
    props.label,
    props.tooltip,
    manifestPath,
    path,
    false,
    manifestOptionPaths
  );

  const { handleScrollToField } = useConnectorBuilderFormManagementState();
  const elementRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    // Call handler in here to make sure it handles new scrollToField value from the context
    handleScrollToField(elementRef, path);
  }, [handleScrollToField, path]);

  if (props.type === "boolean") {
    const switchId = `switch-${path}`;
    const labeledSwitch = (
      <LabeledSwitch
        {...field}
        id={switchId}
        ref={elementRef}
        checked={fieldValue as boolean}
        label={
          <ControlLabels
            className={styles.switchLabel}
            label={label}
            infoTooltipContent={tooltip}
            optional={optional}
            htmlFor={switchId}
            labelAction={labelAction}
          />
        }
        disabled={props.disabled}
      />
    );

    if (props.disabled && props.disabledTooltip) {
      return (
        <Tooltip control={labeledSwitch} placement="bottom-start">
          {props.disabledTooltip}
        </Tooltip>
      );
    }
    return labeledSwitch;
  }

  const setValue = (newValue: unknown) => {
    props.onChange?.(newValue as string & string[]);
    field.onChange(newValue);
  };

  const isDisabled = props.disabled || isPreview;

  return (
    <ControlLabels
      className={styles.container}
      label={label}
      labelAction={labelAction}
      infoTooltipContent={tooltip}
      optional={optional}
      ref={elementRef}
    >
      {props.type === "jinja" && (
        <JinjaInput
          key={path}
          name={field.name}
          value={fieldValue || ""}
          onChange={(newValue) => {
            // Monaco editor triggers onChange whenever the value is changed, whether by the user or by
            // changing the value passed to the "value" prop above.
            // Because we show preview values by changing the value prop, but we don't want to actually
            // commit that preview value back to the form, we don't want to call setValue in that case.
            // So, we use a ref to track the current value of isPreview (because onChange gets called
            // on the old instance of the component which has the old value of isPreview), and if the
            // current value is true, then we don't commit the change back to the form.
            if (isPreviewRef.current) {
              return;
            }
            setValue(newValue);
          }}
          onBlur={(value) => {
            field.onBlur();
            props.onBlur?.(value);
          }}
          disabled={isDisabled}
          manifestPath={manifestPath}
          error={hasError}
          bubbleUpUndoRedo={props.bubbleUpUndoRedo}
        />
      )}
      {(props.type === "string" || props.type === "number" || props.type === "integer") && (
        <Input
          {...field}
          onChange={(e) => {
            setValue(e.target.value);
          }}
          placeholder={props.placeholder}
          className={props.className}
          type={props.type}
          value={(fieldValue as string | number | undefined) ?? ""}
          error={hasError}
          readOnly={readOnly}
          adornment={adornment}
          disabled={isDisabled}
          step={props.step}
          min={props.min}
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
          value={(fieldValue as string) ?? ""}
          onBlur={field.onBlur}
          disabled={isDisabled}
        />
      )}
      {props.type === "textarea" && (
        <TextArea
          {...field}
          onChange={(e) => {
            setValue(e.target.value);
          }}
          className={props.className}
          value={(fieldValue as string) ?? ""}
          error={hasError}
          readOnly={readOnly}
          onBlur={field.onBlur}
          disabled={isDisabled}
        />
      )}
      {props.type === "jsoneditor" && (
        <div className={classNames(props.className, styles.jsonEditor)}>
          <CodeEditor
            key={path}
            value={fieldValue || ""}
            language="json"
            onChange={(val: string | undefined) => {
              if (isPreviewRef.current) {
                return;
              }
              setValue(val);
            }}
            disabled={isDisabled}
            bubbleUpUndoRedo
          />
        </div>
      )}
      {props.type === "array" && (
        <div data-testid={`tag-input-${path}`}>
          <ArrayField
            name={path}
            value={(fieldValue as string[] | undefined) ?? []}
            itemType={props.itemType}
            setValue={setValue}
            error={hasError}
            directionalStyle={props.directionalStyle ?? true}
            uniqueValues={props.uniqueValues}
            disabled={isDisabled}
          />
        </div>
      )}
      {props.type === "enum" && (
        <EnumField
          options={props.options}
          value={fieldValue as string}
          setValue={setValue}
          error={hasError}
          data-testid={path}
          disabled={isDisabled}
        />
      )}
      {props.type === "combobox" && (
        <ComboBox
          options={props.options}
          value={fieldValue as string}
          onChange={(value) => setValue(value ?? "")}
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
          disabled={isDisabled}
          allowCustomValue
          optionsConfig={props.optionsConfig}
        />
      )}
      {props.type === "multicombobox" && (
        <MultiComboBox
          name={path}
          options={props.options}
          value={fieldValue as string[]}
          onChange={setValue}
          error={hasError}
          data-testid={path}
          fieldInputProps={field}
          disabled={isDisabled}
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
      {preview && !hasError && <div className={styles.inputPreview}>{preview(fieldValue)}</div>}
    </ControlLabels>
  );
};

export const BuilderField: React.FC<BuilderFieldProps> = (props) => <InnerBuilderField {...props} key={props.path} />;
