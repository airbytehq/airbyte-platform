import { useCallback, useState } from "react";
import { useIntl } from "react-intl";

import { SearchInput } from "components/ui/SearchInput";

import { useStreamsListContext } from "./StreamsListContext";

interface StreamSearchFilteringProps {
  className?: string;
}

export const StreamSearchFiltering: React.FC<StreamSearchFilteringProps> = ({ className }) => {
  const [value, _setValue] = useState("");
  const { setSearchTerm } = useStreamsListContext();
  const { formatMessage } = useIntl();

  const setValue = useCallback(
    (value: string) => {
      _setValue(value);
      setSearchTerm(value);
    },
    [setSearchTerm]
  );

  return (
    <div className={className}>
      <SearchInput
        placeholder={formatMessage({
          id: "form.search.placeholder",
        })}
        value={value}
        onChange={(e) => setValue(e.target.value)}
      />
    </div>
  );
};
