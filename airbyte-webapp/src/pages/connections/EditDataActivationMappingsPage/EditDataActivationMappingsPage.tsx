import { FormattedMessage } from "react-intl";

import { Heading } from "components/ui/Heading";

export const EditDataActivationMappingsPage = () => {
  return (
    <Heading as="h1">
      <FormattedMessage id="connections.mappings.title" />
      {/** TODO: add content to this page https://github.com/airbytehq/airbyte-internal-issues/issues/13201 */}
    </Heading>
  );
};
