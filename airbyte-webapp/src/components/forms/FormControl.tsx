import classNames from "classnames";
import isString from "lodash/isString";
import uniqueId from "lodash/uniqueId";
import React, { HTMLInputTypeAttribute, ReactNode, useState } from "react";
import { Path, get, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { DatePickerProps } from "components/ui/DatePicker/DatePicker";
import { FlexContainer } from "components/ui/Flex";
import { InputProps } from "components/ui/Input";
import { ListBoxProps, Option } from "components/ui/ListBox";
import { SwitchProps } from "components/ui/Switch/Switch";
import { TagInputProps } from "components/ui/TagInput";
import { Text } from "components/ui/Text";
import { TextAreaProps } from "components/ui/TextArea";
import { InfoTooltip } from "components/ui/Tooltip";

import { NON_I18N_ERROR_TYPE } from "core/utils/form";

import { ArrayWrapper } from "./ArrayWrapper";
import { DatepickerWrapper } from "./DatepickerWrapper";
import { FormValues } from "./Form";
import styles from "./FormControl.module.scss";
import { InputWrapper } from "./InputWrapper";
import { SelectWrapper } from "./SelectWrapper";
import { SwitchWrapper } from "./SwitchWrapper";
import { TextAreaWrapper } from "./TextAreaWrapper";

type ControlProps<T extends FormValues> =
  | SelectControlProps<T>
  | InputControlProps<T>
  | TextAreaControlProps<T>
  | DatepickerControlProps<T>
  | SwitchControlProps<T>
  | ArrayControlProps<T>;

interface ControlBaseProps<T extends FormValues> {
  /**
   * fieldType determines what form element is rendered. Depending on the chosen fieldType, additional props may be optional or required.
   */
  fieldType: "input" | "textarea" | "date" | "dropdown" | "switch" | "array";
  /**
   * The field name must match any provided default value or validation schema.
   */
  name: Path<T>;
  /**
   * A label that is displayed above the form control
   */
  label?: string;
  /**
   * A tooltip that appears next to the form label
   */
  labelTooltip?: ReactNode;
  /**
   * An optional description that appears under the label
   */
  description?: string | ReactNode;
  /**
   * Optional content to render on the right side of the label
   */
  header?: ReactNode;
  hasError?: boolean;
  controlId?: string;
  inline?: boolean;
  optional?: boolean;
  disabled?: boolean;
  /**
   * A custom className that is applied to the form control container
   */
  containerControlClassName?: string;
  /**
   * Optional text displayed below the input, but only when there is no error to display
   */
  footer?: string;
  /**
   * If true, the error message will only be shown if the field has been touched.
   * Otherwise, the error will be shown regardless of whether the field has been touched.
   */
  onlyShowErrorIfTouched?: boolean;
  /**
   * Whether to reserve space for the error message, avoiding layout shifts when the error message appears. By default
   * we want this to be true to avoid layout shifts.
   */
  reserveSpaceForError?: boolean;
}

/**
 * These properties are only relevant at the control level. They can therefore be omitted before passing along to the underlying form input.
 */
export type OmittableProperties = "fieldType" | "label" | "labelTooltip" | "description" | "inline";

export interface InputControlProps<T extends FormValues> extends ControlBaseProps<T>, Omit<InputProps, "name"> {
  fieldType: "input";
  type?: HTMLInputTypeAttribute;
}

export interface TextAreaControlProps<T extends FormValues> extends ControlBaseProps<T>, Omit<TextAreaProps, "name"> {
  fieldType: "textarea";
  type?: HTMLTextAreaElement;
}

export interface DatepickerControlProps<T extends FormValues>
  extends ControlBaseProps<T>,
    Omit<DatePickerProps, "name" | "value" | "onChange"> {
  fieldType: "date";
  /**
   * The desired format for the date string:
   * - **date**       *YYYY-MM-DD* (default)
   * - **date-time**  *YYYY-MM-DDTHH:mm:ssZ*
   */
  format?: "date" | "date-time";
}

export interface SelectControlProps<T extends FormValues>
  extends ControlBaseProps<T>,
    Omit<ListBoxProps<string>, "onSelect" | "selectedValue">,
    Partial<Pick<ListBoxProps<string>, "onSelect">> {
  fieldType: "dropdown";
  options: Array<Option<string>>;
}

export interface SwitchControlProps<T extends FormValues> extends ControlBaseProps<T>, Omit<SwitchProps, "name"> {
  fieldType: "switch";
}

export interface ArrayControlProps<T extends FormValues>
  extends ControlBaseProps<T>,
    Omit<TagInputProps, "name" | "fieldValue" | "onChange"> {
  fieldType: "array";
  itemType?: "string" | "number" | "integer";
}

export const FormControl = <T extends FormValues>({
  label,
  labelTooltip,
  description,
  header,
  inline = false,
  optional = false,
  containerControlClassName,
  footer,
  onlyShowErrorIfTouched,
  reserveSpaceForError = true,
  ...props
}: ControlProps<T>) => {
  // only retrieve new form state if form state of current field has changed
  const { errors, touchedFields } = useFormState<T>({ name: props.name });
  const error = !!get(errors, props.name) && (onlyShowErrorIfTouched ? !!get(touchedFields, props.name) : true);
  const [controlId] = useState(props.id ?? `input-control-${uniqueId()}`);

  // Properties to pass to the underlying control
  const controlProps = {
    ...props,
    hasError: error,
    controlId,
  };

  function renderControl() {
    if (controlProps.fieldType === "input") {
      // After narrowing controlProps, we need to strip controlProps.fieldType as it's no longer needed
      const { fieldType, ...withoutFieldType } = controlProps;
      return <InputWrapper {...withoutFieldType} />;
    }

    if (controlProps.fieldType === "textarea") {
      const { fieldType, ...withoutFieldType } = controlProps;
      return <TextAreaWrapper {...withoutFieldType} />;
    }

    if (controlProps.fieldType === "date") {
      const { fieldType, ...withoutFieldType } = controlProps;
      return <DatepickerWrapper {...withoutFieldType} />;
    }

    if (controlProps.fieldType === "dropdown") {
      const { fieldType, ...withoutFieldType } = controlProps;
      return <SelectWrapper {...withoutFieldType} />;
    }

    if (controlProps.fieldType === "switch") {
      const { fieldType, ...withoutFieldType } = controlProps;
      return <SwitchWrapper {...withoutFieldType} />;
    }

    if (controlProps.fieldType === "array") {
      const { fieldType, ...withoutFieldType } = controlProps;
      return <ArrayWrapper {...withoutFieldType} />;
    }

    throw new Error(`No matching form input found for type: ${props.fieldType}`);
  }

  const displayFooter = !!error || !!footer;

  return (
    <div
      className={classNames(
        styles.control,
        { [styles["control--inline"]]: inline, [styles["control--reserveSpaceForError"]]: reserveSpaceForError },
        containerControlClassName
      )}
      data-field-path={props.name}
    >
      {label && (
        <FormLabel
          description={description}
          label={label}
          labelTooltip={labelTooltip}
          htmlFor={controlId}
          optional={optional}
          header={header}
        />
      )}
      <div className={styles.control__field}>{renderControl()}</div>
      {displayFooter && (
        <FormControlFooter doesShiftLayout={!reserveSpaceForError}>
          <FormControlErrorMessage<FormValues> name={props.name} />
          {!error && footer && <FormControlFooterInfo>{footer}</FormControlFooterInfo>}
        </FormControlFooter>
      )}
    </div>
  );
};

interface FormLabelProps {
  description?: string | ReactNode;
  label: string;
  labelTooltip?: ReactNode;
  htmlFor: string;
  inline?: boolean;
  optional?: boolean;
  header?: ReactNode;
}

export const FormLabel: React.FC<FormLabelProps> = ({
  description,
  label,
  labelTooltip,
  htmlFor,
  optional,
  header,
}) => {
  return (
    <label className={styles.control__label} htmlFor={htmlFor}>
      <FlexContainer alignItems="center" gap="md">
        <Text size="lg" className={styles.control__labelText} bold>
          {label}
          {labelTooltip && <InfoTooltip placement="top-start">{labelTooltip}</InfoTooltip>}
          {optional && (
            <Text className={styles.control__optional} as="span">
              <FormattedMessage id="form.optional" />
            </Text>
          )}
        </Text>
        {header}
      </FlexContainer>
      {description &&
        (isString(description) ? <Text className={styles.control__description}>{description}</Text> : description)}
    </label>
  );
};

interface FormControlFooterProps {
  doesShiftLayout?: boolean;
}

export const FormControlFooter: React.FC<React.PropsWithChildren<FormControlFooterProps>> = ({
  children,
  doesShiftLayout,
}) => {
  return (
    <div
      className={classNames(styles.control__footer, { [styles["control__footer--doesShiftLayout"]]: doesShiftLayout })}
    >
      {children}
    </div>
  );
};

export const FormControlFooterInfo: React.FC<React.PropsWithChildren> = ({ children }) => {
  return (
    <Text color="grey400" size="xs" className={styles.control__footerText}>
      {children}
    </Text>
  );
};

export const FormControlFooterError: React.FC<React.PropsWithChildren> = ({ children }) => {
  return (
    <Text color="red" size="xs" className={styles.control__footerText}>
      {children}
    </Text>
  );
};

interface FormControlErrorProps<TFormValues> {
  // An override for the message. If it isn't provided, the error from the RHF schema validation will be shown.
  message?: React.ReactNode;
  name: Path<TFormValues>;
}

export const FormControlErrorMessage = <TFormValues extends FormValues>({
  message,
  name,
}: FormControlErrorProps<TFormValues>) => {
  const { formatMessage } = useIntl();
  const { errors } = useFormState<TFormValues>({ name });
  const error = get(errors, name);
  if (!error) {
    return null;
  }

  return (
    <Text color="red" size="xs" className={styles.control__footerText}>
      {!message &&
        // NON_I18N_ERROR_TYPE is a custom error type that is used to display a non-i18n error message.
        // "validate" type means the error came from the react-hook-form validate() method.
        (error.type === NON_I18N_ERROR_TYPE || error.type === "validate"
          ? error.message
          : formatMessage({ id: error.message ?? "form.empty.error" }))}
      {message && message}
    </Text>
  );
};
