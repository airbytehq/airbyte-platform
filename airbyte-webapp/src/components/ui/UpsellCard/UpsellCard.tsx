import { BrandingBadge } from "components/ui/BrandingBadge";
import { HighlightCard } from "components/ui/Card/HighlightCard";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

interface UpsellCardProps {
  header: React.ReactNode;
  description: React.ReactNode;
  cta: React.ReactNode;
  image: React.ReactNode;
  "data-testid"?: string;
  branding?: "enterprise" | "teams";
}

export const UpsellCard: React.FC<UpsellCardProps> = ({
  header,
  description,
  cta,
  image,
  branding,
  "data-testid": dataTestId,
}) => (
  <HighlightCard data-testid={dataTestId}>
    <FlexContainer direction="row" justifyContent="space-between">
      <FlexContainer direction="column" gap="lg">
        {branding === "enterprise" && <BrandingBadge product="enterprise" />}
        {branding === "teams" && <BrandingBadge product="cloudForTeams" />}
        <Heading as="h3" size="md">
          {header}
        </Heading>
        <Text>{description}</Text>
        {cta}
      </FlexContainer>
      <FlexContainer alignItems="center" justifyContent="center">
        {image}
      </FlexContainer>
    </FlexContainer>
  </HighlightCard>
);
