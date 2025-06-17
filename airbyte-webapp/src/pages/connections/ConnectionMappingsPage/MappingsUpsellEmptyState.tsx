import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";
import { UpsellCard } from "components/ui/UpsellCard/UpsellCard";

import { FeatureItem, useFeature } from "core/services/features";
import { links } from "core/utils/links";

import mappings_screenshot from "./mappings_screenshot.png";

export const MappingsUpsellEmptyState = () => {
  const enterpriseUpsell = useFeature(FeatureItem.EnterpriseUpsell);
  const cloudForTeamsUpsell = useFeature(FeatureItem.CloudForTeamsUpsell);

  const description = (
    <FlexContainer direction="column" gap="lg">
      <Text>
        <FormattedMessage id="connections.mappings.emptyState.upsellBody" />
      </Text>
      <Text>
        <FormattedMessage
          id="connections.mappings.emptyState.upsellFooter"
          values={{ product: enterpriseUpsell ? "enterprise" : "teams" }}
        />
      </Text>
    </FlexContainer>
  );

  const cta = (
    <ExternalLink variant="buttonPrimary" href={links.featureTalkToSales.replace("{feature}", "mappings")}>
      <Box p="xs">
        <FlexContainer alignItems="center">
          <Icon type="lock" />
          <FormattedMessage id="connections.mappings.emptyState.upsellButton" />
        </FlexContainer>
      </Box>
    </ExternalLink>
  );

  return (
    <UpsellCard
      branding={enterpriseUpsell ? "enterprise" : cloudForTeamsUpsell ? "teams" : undefined}
      header={<FormattedMessage id="connections.mappings.emptyState.upsellTitle" />}
      description={description}
      cta={cta}
      image={<img width={295} src={mappings_screenshot} alt="Mappings tab screenshot" />}
      data-testid="mappings-upsell-empty-state"
    />
  );
};
