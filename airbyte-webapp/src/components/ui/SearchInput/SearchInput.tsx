import React from "react";
import { useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { Input, InputProps } from "components/ui/Input";

import styles from "./SearchInput.module.scss";
import { Icon } from "../Icon";

interface SearchInputProps extends Omit<InputProps, "onChange"> {
  placeholder?: string;
  value: string;
  onChange: (newValue: string) => void;
  inline?: boolean;
  containerClassName?: InputProps["containerClassName"];
  debounceTimeout?: number;
}

export const SearchInput = React.forwardRef<HTMLInputElement, SearchInputProps>(
  ({ value, onChange, placeholder, inline = false, debounceTimeout = 0, ...restProps }, ref) => {
    const [inputValue, setInputValue] = React.useState(value);

    useDebounce(
      () => {
        value !== inputValue && onChange(inputValue);
      },
      debounceTimeout,
      [inputValue, debounceTimeout, onChange, value]
    );
    const { formatMessage } = useIntl();

    return (
      // Our <Input> component contains an <input>, but eslint is not smart enough to find it
      // eslint-disable-next-line jsx-a11y/label-has-associated-control
      <label className={styles.searchInput}>
        <div className={styles.searchInput__iconWrapper}>
          <Icon type="lens" color="action" />
        </div>
        <Input
          ref={ref}
          type="search"
          role="search"
          className={styles.searchInput__input}
          placeholder={placeholder ?? formatMessage({ id: "form.search.placeholder" })}
          value={inputValue}
          onChange={(e) => {
            setInputValue(e.target.value);
          }}
          light
          inline={inline}
          {...restProps}
        />
      </label>
    );
  }
);
SearchInput.displayName = "SearchInput";
