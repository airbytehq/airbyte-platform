import { useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { ConnectionConfigurationPreview } from "./ConnectionConfigurationPreview";
import { DestinationStreamPrefixNameFormField } from "./DestinationStreamPrefixNameFormField";
import { NamespaceDefinitionFormField } from "./NamespaceDefinitionFormField";
import { NonBreakingChangesPreferenceFormField } from "./NonBreakingChangesPreferenceFormField";
import { ScheduleFormField } from "./ScheduleFormField/ScheduleFormField";

export const ConnectionConfigurationCard = () => {
  const { formatMessage } = useIntl();
  const { mode } = useConnectionFormService();
  const isEditMode = mode === "edit";

  return (
    <Card
      title={formatMessage({ id: "form.configuration" })}
      collapsible={isEditMode}
      defaultCollapsedState={isEditMode}
      collapsedPreviewInfo={<ConnectionConfigurationPreview />}
      dataTestId="configuration"
    >
      <FlexContainer direction="column" gap="lg">
        <ScheduleFormField />
        <NamespaceDefinitionFormField />
        <DestinationStreamPrefixNameFormField />
        <NonBreakingChangesPreferenceFormField />
      </FlexContainer>
    </Card>
  );
};
