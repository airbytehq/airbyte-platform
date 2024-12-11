import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { HighlightCard } from "components/ui/Card/HighlightCard";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { FeatureItem, useFeature } from "core/services/features";
import { links } from "core/utils/links";
import { BrandingBadge } from "views/layout/SideBar/AirbyteHomeLink";

import mappings_screenshot from "./mappings_screenshot.png";
export const MappingsUpsellEmptyState = () => {
  const enterpriseUpsell = useFeature(FeatureItem.EnterpriseUpsell);
  const cloudForTeamsUpsell = useFeature(FeatureItem.CloudForTeamsUpsell);

  return (
    <HighlightCard>
      <FlexContainer direction="row" justifyContent="space-between">
        <FlexContainer direction="column" gap="lg">
          {enterpriseUpsell && <BrandingBadge product="enterprise" />}
          {cloudForTeamsUpsell && <BrandingBadge product="cloudForTeams" />}
          <Heading as="h3" size="md">
            <FormattedMessage id="connections.mappings.emptyState.upsellTitle" />
          </Heading>
          <Text>
            <FormattedMessage id="connections.mappings.emptyState.upsellBody" />
          </Text>
          <Text>
            <FormattedMessage
              id="connections.mappings.emptyState.upsellFooter"
              values={{ product: enterpriseUpsell ? "enterprise" : "teams" }}
            />
          </Text>
          <ExternalLink variant="buttonPrimary" href={links.featureTalkToSales.replace("{feature}", "mappings")}>
            <Box p="xs">
              <FlexContainer alignItems="center">
                <Icon type="lock" />
                <FormattedMessage id="connections.mappings.emptyState.upsellButton" />
              </FlexContainer>
            </Box>
          </ExternalLink>
        </FlexContainer>
        <FlexContainer alignItems="center" justifyContent="center">
          <img width={295} src={mappings_screenshot} alt="Blurred connector form" />
        </FlexContainer>
      </FlexContainer>
    </HighlightCard>
  );
};
