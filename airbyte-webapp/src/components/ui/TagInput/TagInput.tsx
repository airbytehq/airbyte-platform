import classNames from "classnames";
import isEqual from "lodash/isEqual";
import uniqueId from "lodash/uniqueId";
import React, { KeyboardEventHandler, useCallback, useMemo, useState } from "react";
import {
  ActionMeta,
  GroupBase,
  MultiValue,
  OnChangeValue,
  StylesConfig,
  components,
  InputProps,
  InputActionMeta,
} from "react-select";
import CreatableSelect from "react-select/creatable";

import styles from "./TagInput.module.scss";

const customStyles = (directional?: boolean, disabled?: boolean): StylesConfig<Tag, true, GroupBase<Tag>> => ({
  multiValue: (provided) => ({
    ...provided,
    maxWidth: "100%",
    display: "flex",
    background: `${styles.backgroundColor}`,
    color: `${styles.fontColor}`,
    borderRadius: `${directional ? styles.borderRadiusDirectional : styles.borderRadius}`,
    paddingLeft: `${styles.paddingLeft}`,
    border: `${styles.valueBorder}`,
    opacity: `${disabled ? styles.disabledOpacity : undefined}`,
  }),
  multiValueLabel: (provided) => ({
    ...provided,
    color: `${styles.fontColor}`,
    fontWeight: 500,
    fontSize: styles.fontSize,
  }),
  multiValueRemove: (provided) => ({
    ...provided,
    borderRadius: `${directional ? styles.borderRadiusDirectional : styles.borderRadius}`,
    cursor: "pointer",
  }),
  clearIndicator: (provided) => ({
    ...provided,
    cursor: "pointer",
  }),
  control: (provided, state) => {
    const isInvalid = state.selectProps["aria-invalid"];
    const regularBorderColor = isInvalid ? styles.errorBorderColor : styles.regularBorderColor;
    const hoveredBorderColor = isInvalid ? styles.errorHoveredBorderColor : styles.hoveredBorderColor;
    return {
      ...provided,
      backgroundColor: "inherit",
      boxShadow: "none",
      borderColor: state.isFocused ? styles.focusedBorderColor : regularBorderColor,
      ":hover": {
        cursor: "text",
        borderColor: state.isFocused ? styles.focusedBorderColor : hoveredBorderColor,
      },
      fontSize: styles.fontSize,
    };
  },
  input: (provided) => ({
    ...provided,
    color: styles.fontColor,
  }),
});

interface Tag {
  readonly label: string;
  readonly value: string;
}

export type TagInputProps = {
  className?: string;
  name: string;
  fieldValue: Array<string | number>;
  onBlur?: () => void;
  onFocus?: () => void;
  error?: boolean;
  disabled?: boolean;
  id?: string;
  directionalStyle?: boolean;
  uniqueValues?: boolean;
} & (
  | {
      itemType?: "string";
      onChange: (value: string[]) => void;
    }
  | {
      itemType?: "number" | "integer";
      onChange: (value: number[]) => void;
    }
);

const generateTagFromValue = (inputValue: string | number): Tag => ({
  label: String(inputValue),
  value: uniqueId(`tag_value_`),
});

const generateValueFromTag = (tag: Tag, itemType?: string): string | number => {
  if (itemType === "integer") {
    return parseInt(tag.label, 10);
  } else if (itemType === "number") {
    return parseFloat(tag.label);
  }
  return tag.label;
};

const delimiters = [",", ";"];

export const TagInput = React.forwardRef(
  (
    {
      className,
      onChange: onChangeProp,
      fieldValue,
      name,
      disabled,
      id,
      error,
      itemType = "string",
      onBlur,
      onFocus,
      directionalStyle,
      uniqueValues,
    }: TagInputProps,
    _ref: React.Ref<HTMLDivElement> // ignore the ref, because TagInput is not a real <input> so it can't handle things like setSelectionRange
  ) => {
    const onChange = useCallback(
      (value: Array<string | number>) => {
        if (itemType === "string") {
          (onChangeProp as (value: string[]) => void)(value as string[]);
        } else if (itemType === "number" || itemType === "integer") {
          (onChangeProp as (value: number[]) => void)(value as number[]);
        }
      },
      [itemType, onChangeProp]
    );
    const [draftValue, setDraftValue] = useState("");
    const draftExists = draftValue.length > 0;

    // possibly deduplicates values based on the uniqueValues prop
    const valuesToCommit = useCallback(
      (preexistingValues: Array<string | number>, newValues: Array<string | number> = []) => {
        const allValues = [...preexistingValues, ...newValues];
        return uniqueValues ? Array.from(new Set(allValues)) : allValues;
      },
      [uniqueValues]
    );

    const tags = useMemo(() => {
      const nonDraftValues =
        draftExists && fieldValue.length > 0 ? fieldValue.slice(0, fieldValue.length - 1) : fieldValue;
      // we deduplicate based on props.uniqueValues when values get converted to tags
      return valuesToCommit(nonDraftValues).map(generateTagFromValue);
    }, [draftExists, fieldValue, valuesToCommit]);

    // handles various ways of deleting a value
    const handleDelete = (_value: OnChangeValue<Tag, true>, actionMeta: ActionMeta<Tag>) => {
      let updatedTags: MultiValue<Tag> = tags;

      /**
       * remove-value: user clicked x or used backspace/delete to remove tag
       * clear: user clicked big x to clear all tags
       * pop-value: user clicked backspace to remove tag
       */
      if (actionMeta.action === "remove-value") {
        updatedTags = updatedTags.filter((tag) => tag.value !== actionMeta.removedValue.value);
      } else if (actionMeta.action === "clear") {
        updatedTags = [];
      } else if (actionMeta.action === "pop-value") {
        updatedTags = updatedTags.slice(0, updatedTags.length - 1);
      }

      const updatedValues = updatedTags.map((tag) => generateValueFromTag(tag, itemType));
      if (draftExists) {
        const parsedDraft = parseInputValue(draftValue);
        if (parsedDraft !== undefined) {
          onChange([...updatedValues, parsedDraft]);
        } else {
          onChange(updatedValues);
        }
      } else {
        onChange(updatedValues);
      }
    };

    function parseInputValue(input: string): string | number | undefined {
      const trimmedInput = input.trim();
      if (itemType !== "number" && itemType !== "integer") {
        return trimmedInput;
      }

      const parsedInput = itemType === "integer" ? Number.parseInt(trimmedInput, 10) : Number.parseFloat(trimmedInput);

      if (Number.isNaN(parsedInput)) {
        return undefined;
      }

      return parsedInput;
    }

    // handle when a user types OR pastes in the input
    const handleInputChange = (newDraftValue: string, actionMeta: InputActionMeta) => {
      // only handle events where the input is changed; ignore blur events as they are handled separately
      if (actionMeta.action !== "input-change") {
        return;
      }

      const fieldValueMinusLast = fieldValue.slice(0, fieldValue.length - 1);
      const delimiter = delimiters.find((del) => newDraftValue.includes(del));

      if (delimiter) {
        const newValues = newDraftValue
          .split(delimiter)
          .map(parseInputValue)
          .filter((val): val is string | number => val !== undefined && val !== "");

        if (newValues.length === 0) {
          return;
        }

        setDraftValue("");

        if (draftExists) {
          // the user has been typing and entered a delimiter character
          onChange(valuesToCommit([...fieldValueMinusLast, ...newValues]));
        } else {
          // the user pastes in text with a delimiter character with no draft item in progress
          onChange(valuesToCommit([...fieldValue, ...newValues]));
        }
      } else {
        // Special case: Empty input or deleting the last character
        if (newDraftValue === "") {
          setDraftValue("");
          if (draftExists) {
            // User deleted the last character of a draft value
            onChange(valuesToCommit(fieldValueMinusLast));
          }
          return;
        }

        const parsedValue = parseInputValue(newDraftValue);
        if (parsedValue === undefined) {
          return;
        }

        setDraftValue(newDraftValue);

        if (draftExists) {
          if (newDraftValue.length === 0) {
            // the user has just deleted the last character of a draft value
            onChange(valuesToCommit(fieldValueMinusLast));
          } else {
            // the user is continuing to update a draft value
            onChange([...fieldValueMinusLast, parsedValue]);
          }
        } else if (parsedValue !== undefined) {
          // the user has typed or pasted the first character(s) of a draft value
          onChange([...fieldValue, parsedValue]);
        }
      }
    };

    // handle when user presses keyboard keys in the input
    const handleKeyDown: KeyboardEventHandler<HTMLDivElement> = (event) => {
      if (!draftValue || !draftValue.length) {
        return;
      }
      switch (event.key) {
        case "Enter":
        case "Tab":
          event.preventDefault();
          setDraftValue("");
      }
    };

    /**
     * Clear the draft value state when leaving the control, causing the draft value to be shown as a tag.
     * This needs to be implemented outside of the onBlur prop of react-select because it's not default behavior.
     */
    const onBlurControl = () => {
      setDraftValue("");
      // When `uniqueValues` is set, this ensures that invalid duplicate values are actually
      // removed from the underlying fieldValue when the user exits the field, not just
      // removed from view. Without it, drafting a duplicate value and then clicking outside
      // the field will retain the potentially invalid value *even though* it will be removed
      // from the user's view if `uniqueValues` is set.
      if (!isEqual(valuesToCommit(fieldValue), fieldValue)) {
        onChange(valuesToCommit(fieldValue));
      }
      onBlur?.();
    };

    const overwrittenComponents = useMemo(
      () => ({
        DropdownIndicator: null,
        Input: (props: InputProps<Tag, true, GroupBase<Tag>>) => (
          <components.Input {...props} data-testid={`tag-input-${name}`} />
        ),
      }),
      [name]
    );

    return (
      <div
        data-testid="tag-input"
        onBlur={onBlurControl}
        className={classNames(className, { [styles.disabled]: disabled })}
      >
        <CreatableSelect
          inputId={id}
          name={name}
          components={overwrittenComponents}
          inputValue={draftValue}
          placeholder=""
          aria-invalid={Boolean(error)}
          isClearable
          isMulti
          onBlur={() => handleDelete}
          menuIsOpen={false}
          onChange={handleDelete}
          onInputChange={handleInputChange}
          onKeyDown={handleKeyDown}
          value={tags}
          onFocus={onFocus}
          isDisabled={disabled}
          styles={customStyles(directionalStyle, disabled)}
        />
      </div>
    );
  }
);
TagInput.displayName = "TagInput";
