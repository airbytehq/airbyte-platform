import { useIntl } from "react-intl";

import { Input, InputProps } from "components/ui/Input";

import styles from "./SearchInput.module.scss";
import { Icon } from "../Icon";

interface SearchInputProps {
  placeholder?: string;
  value: string;
  onChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  inline?: boolean;
  containerClassName?: InputProps["containerClassName"];
}

export const SearchInput: React.FC<SearchInputProps> = ({
  value,
  onChange,
  placeholder,
  inline = false,
  containerClassName,
}) => {
  const { formatMessage } = useIntl();

  return (
    // Our <Input> component contains an <input>, but eslint is not smart enough to find it
    // eslint-disable-next-line jsx-a11y/label-has-associated-control
    <label className={styles.searchInput}>
      <div className={styles.searchInput__iconWrapper}>
        <Icon type="lens" color="action" />
      </div>
      <Input
        type="search"
        className={styles.searchInput__input}
        containerClassName={containerClassName}
        placeholder={placeholder ?? formatMessage({ id: "form.search.placeholder" })}
        value={value}
        onChange={onChange}
        light
        inline={inline}
      />
    </label>
  );
};
