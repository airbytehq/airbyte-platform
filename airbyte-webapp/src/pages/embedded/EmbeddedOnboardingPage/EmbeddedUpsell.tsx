import { useMutation } from "@tanstack/react-query";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/HeadTitle";
import AirbyteLogo from "components/illustrations/airbyte-logo.svg?react";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink, Link } from "components/ui/Link";
import { Text } from "components/ui/Text";
import { UpsellCard } from "components/ui/UpsellCard/UpsellCard";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useAuthService } from "core/services/auth";
import { links } from "core/utils/links";

import widget_screenshot from "./embedded_widget.png";
import styles from "./EmbeddedUpsell.module.scss";

export const EmbeddedUpsell: React.FC = () => {
  useTrackPage(PageTrackingCodes.EMBEDDED_UPSELL);
  const { logout } = useAuthService();
  const { isLoading: isLogoutLoading, mutateAsync: handleLogout } = useMutation(() => logout?.() ?? Promise.resolve());

  return (
    <>
      <HeadTitle titles={[{ id: "settings.embedded" }]} />
      <FlexContainer alignItems="center" justifyContent="space-between" className={styles.header}>
        <AirbyteLogo className={styles.logo} />

        {logout && (
          <Button variant="secondary" onClick={() => handleLogout()} isLoading={isLogoutLoading}>
            <FormattedMessage id="settings.accountSettings.logoutText" />
          </Button>
        )}
      </FlexContainer>
      <Box py="2xl">
        <FlexContainer direction="column" gap="xl" className={styles.upsellContainer}>
          <Heading as="h1">
            <FormattedMessage id="embedded.onoarding.upsell.pageTitle" />
          </Heading>
          <Box py="lg">
            <UpsellCard
              header={<FormattedMessage id="embedded.onboarding.upsell.header" />}
              description={
                <Text>
                  <FormattedMessage id="embedded.onboarding.upsell.description" />
                </Text>
              }
              cta={
                <ExternalLink variant="buttonPrimary" href={links.sonarTalktoSales}>
                  <Box p="xs">
                    <FlexContainer alignItems="center">
                      <Icon type="lock" />
                      <FormattedMessage id="embedded.onboarding.talkToSales" />
                    </FlexContainer>
                  </Box>
                </ExternalLink>
              }
              image={<img width={101} src={widget_screenshot} alt="Airbyte Embedded widget screenshot" />}
              data-testid="embedded-upsell-empty-state"
            />
          </Box>
          <Text color="grey" size="md">
            <FormattedMessage
              id="embedded.onboarding.upsell.orReturnToAirbyteCloud"
              values={{
                lnk: (node: React.ReactNode) => <Link to="/">{node}</Link>,
              }}
            />
          </Text>
        </FlexContainer>
      </Box>
    </>
  );
};
