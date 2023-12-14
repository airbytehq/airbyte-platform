import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

export const NoDataMessage: React.FC = () => (
  <div>
    <Text size="lg" align="center">
      <FormattedMessage id="connection.overview.graph.noData" />
    </Text>
  </div>
);
