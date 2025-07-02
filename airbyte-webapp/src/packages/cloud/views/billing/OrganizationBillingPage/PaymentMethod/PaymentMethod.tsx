import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DataLoadingError } from "components/ui/DataLoadingError";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useGetPaymentInformation } from "core/api";
import { PaymentMethodRead } from "core/api/types/AirbyteClient";

import styles from "./PaymentMethod.module.scss";
import { useRedirectToCustomerPortal } from "../../../../area/billing/utils/useRedirectToCustomerPortal";
import { CreditCardLogo } from "../CreditCardLogo";
import LinkLogo from "../logos/link.svg?react";
import { UpdateButton } from "../UpdateButton";

interface CurrentPaymentMethodProps {
  paymentMethod: PaymentMethodRead;
}

const CurrentPaymentMethod: React.FC<CurrentPaymentMethodProps> = ({ paymentMethod }) => {
  if (paymentMethod.type === "card") {
    return (
      <FlexContainer alignItems="center" justifyContent="space-between" wrap="wrap">
        <FlexContainer alignItems="center">
          <CreditCardLogo brand={paymentMethod.cardBrand} />
          <Text size="lg" bold>
            <FormattedMessage
              id="settings.organization.billing.paymentMethod.preview"
              values={{ lastFour: paymentMethod.cardLastDigits }}
            />
          </Text>
        </FlexContainer>

        <Text>
          <FormattedMessage
            id="settings.organization.billing.paymentMethod.expires"
            values={{
              month: paymentMethod.cardExpireMonth,
              year: paymentMethod.cardExpireYear,
            }}
          />
        </Text>
      </FlexContainer>
    );
  } else if (paymentMethod.type === "link") {
    return <LinkLogo className={styles.paymentMethod__link} />;
  }

  return (
    <Text color="grey600">
      <FlexContainer alignItems="center" gap="sm">
        <Icon type="check" />
        <FormattedMessage id="settings.organization.billing.paymentMethodUnknown" />
      </FlexContainer>
    </Text>
  );
};

export const PaymentMethod = () => {
  const { redirecting, goToCustomerPortal } = useRedirectToCustomerPortal("payment_method");
  const organizationId = useCurrentOrganizationId();
  const {
    data: paymentInformation,
    isLoading: paymentInformationLoading,
    isError: paymentInformationError,
  } = useGetPaymentInformation(organizationId);

  return (
    <>
      <FlexContainer justifyContent="space-between">
        <Heading as="h2" size="sm">
          <FormattedMessage id="settings.organization.billing.paymentMethod" />
        </Heading>

        {paymentInformation?.customer?.defaultPaymentMethod && (
          <UpdateButton isLoading={redirecting} onClick={goToCustomerPortal}>
            <FormattedMessage id="settings.organization.billing.update" />
          </UpdateButton>
        )}
      </FlexContainer>
      <Box pt="xl">
        {!!paymentInformation ? (
          <>
            {paymentInformation.customer?.defaultPaymentMethod && (
              <CurrentPaymentMethod paymentMethod={paymentInformation.customer.defaultPaymentMethod} />
            )}
            {!paymentInformation.customer?.defaultPaymentMethod && (
              <Button isLoading={redirecting} onClick={goToCustomerPortal}>
                <FormattedMessage id="settings.organization.billing.paymentMethod.add" />
              </Button>
            )}
          </>
        ) : (
          <>
            {paymentInformationLoading && <LoadingSkeleton />}
            {paymentInformationError && (
              <DataLoadingError>
                <FormattedMessage id="settings.organization.billing.paymentMethodError" />
              </DataLoadingError>
            )}
          </>
        )}
      </Box>
    </>
  );
};
