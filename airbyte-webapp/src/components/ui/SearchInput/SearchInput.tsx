import React from "react";
import { useIntl } from "react-intl";

import { Input, InputProps } from "components/ui/Input";

import styles from "./SearchInput.module.scss";
import { Icon } from "../Icon";

interface SearchInputProps extends InputProps {
  placeholder?: string;
  value: string;
  onChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  inline?: boolean;
  containerClassName?: InputProps["containerClassName"];
}

export const SearchInput = React.forwardRef<HTMLInputElement, SearchInputProps>(
  ({ value, onChange, placeholder, inline = false, ...restProps }, ref) => {
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
          className={styles.searchInput__input}
          placeholder={placeholder ?? formatMessage({ id: "form.search.placeholder" })}
          value={value}
          onChange={onChange}
          light
          inline={inline}
          {...restProps}
        />
      </label>
    );
  }
);
SearchInput.displayName = "SearchInput";
