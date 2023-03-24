import { useIntl } from "react-intl";

import { Input } from "components/ui/Input";

import { useStreamsListContext } from "./StreamsListContext";

interface StreamSearchFilteringProps {
  className?: string;
}

export const StreamSearchFiltering: React.FC<StreamSearchFilteringProps> = ({ className }) => {
  const { setSearchTerm } = useStreamsListContext();
  const { formatMessage } = useIntl();

  return (
    <div className={className}>
      <Input
        placeholder={formatMessage({
          id: `form.nameSearch`,
        })}
        onChange={(e) => setSearchTerm(e.target.value)}
      />
    </div>
  );
};
