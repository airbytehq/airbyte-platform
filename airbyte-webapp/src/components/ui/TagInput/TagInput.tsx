import classNames from "classnames";
import uniqueId from "lodash/uniqueId";
import { KeyboardEventHandler, useMemo, useState } from "react";
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
    const regularBorderColor = isInvalid ? styles.errorBorderColor : "transparent";
    const hoveredBorderColor = isInvalid ? styles.errorHoveredBorderColor : styles.hoveredBorderColor;
    return {
      ...provided,
      backgroundColor: styles.inputBackgroundColor,
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

interface TagInputProps {
  name: string;
  fieldValue: string[];
  itemType?: string;
  onChange: (value: string[]) => void;
  onBlur?: () => void;
  onFocus?: () => void;
  error?: boolean;
  disabled?: boolean;
  id?: string;
  directionalStyle?: boolean;
}

const generateTagFromString = (inputValue: string): Tag => ({
  label: inputValue,
  value: uniqueId(`tag_value_`),
});

const generateStringFromTag = (tag: Tag): string => tag.label;

const delimiters = [",", ";"];

export const TagInput: React.FC<TagInputProps> = ({
  onChange,
  fieldValue,
  name,
  disabled,
  id,
  error,
  itemType,
  onBlur,
  onFocus,
  directionalStyle,
}) => {
  const [draftValue, setDraftValue] = useState("");
  const draftExists = draftValue.length > 0;

  const tags = useMemo(() => {
    const nonDraftValues =
      draftExists && fieldValue.length > 0 ? fieldValue.slice(0, fieldValue.length - 1) : fieldValue;
    return nonDraftValues.map(generateTagFromString);
  }, [draftExists, fieldValue]);

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

    const updatedTagsAsStrings = updatedTags.map((tag) => generateStringFromTag(tag));
    if (draftExists) {
      onChange([...updatedTagsAsStrings, draftValue]);
    } else {
      onChange(updatedTagsAsStrings);
    }
  };

  function normalizeInput(input: string) {
    if (itemType !== "number" && itemType !== "integer") {
      return input.trim();
    }
    const parsedInput = itemType === "integer" ? Number.parseInt(input) : Number.parseFloat(input);
    if (Number.isNaN(parsedInput)) {
      return undefined;
    }
    return parsedInput.toString();
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
      const newTagStrings = newDraftValue
        .split(delimiter)
        .map(normalizeInput)
        .filter((tag): tag is string => Boolean(tag));
      if (newTagStrings.length === 0) {
        return;
      }

      setDraftValue("");

      if (draftExists) {
        onChange([...fieldValueMinusLast, ...newTagStrings]);
      } else {
        onChange([...fieldValue, ...newTagStrings]);
      }
    } else {
      const normalizedDraft = normalizeInput(newDraftValue);
      if (normalizedDraft === undefined) {
        return;
      }

      setDraftValue(newDraftValue);

      if (draftExists) {
        if (newDraftValue.length === 0) {
          onChange([...fieldValueMinusLast]);
        } else {
          onChange([...fieldValueMinusLast, normalizedDraft]);
        }
      } else {
        onChange([...fieldValue, normalizedDraft]);
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
    <div data-testid="tag-input" onBlur={onBlurControl} className={classNames({ [styles.disabled]: disabled })}>
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
};
