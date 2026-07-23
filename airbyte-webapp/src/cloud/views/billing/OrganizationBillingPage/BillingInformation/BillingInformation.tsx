import * as Flags from "country-flag-icons/react/3x2";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { DataLoadingError } from "components/ui/DataLoadingError";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useGetPaymentInformation } from "core/api";

import styles from "./BillingInformation.module.scss";
import { useRedirectToCustomerPortal } from "../../../../area/billing/utils/useRedirectToCustomerPortal";
import { UpdateButton } from "../UpdateButton";

type CountryCodes = keyof typeof Flags;

const formatCityStatePostalCode = (city?: string, state?: string, postalCode?: string, country?: string) => {
  switch (country) {
    case "DE":
      return [postalCode, city].filter(Boolean).join(" ");
    case "US":
    default:
      return [city, state, postalCode].filter(Boolean).join(" ");
  }
};

export const BillingInformation = () => {
  const organizationId = useCurrentOrganizationId();
  const { redirecting, goToCustomerPortal } = useRedirectToCustomerPortal("portal");
  const {
    data: paymentInformation,
    isLoading: paymentInformationLoading,
    isError: paymentInformationError,
  } = useGetPaymentInformation(organizationId);

  const address = paymentInformation?.customer?.customerAddress;

  const addressLineOne = address?.line1;
  const addressLineTwo = formatCityStatePostalCode(
    address?.city,
    address?.state,
    address?.postalCode,
    address?.country
  );

  const FlagComponent = address?.country ? Flags[address.country.toUpperCase() as CountryCodes] : null;
  const anyAddressPresent = addressLineOne || addressLineTwo;

  return (
    <>
      <FlexContainer justifyContent="space-between">
        <Heading as="h2" size="sm">
          <FormattedMessage id="settings.organization.billing.billingInformation" />
        </Heading>

        {paymentInformation && (
          <UpdateButton isLoading={redirecting} onClick={goToCustomerPortal}>
            <FormattedMessage id="settings.organization.billing.update" />
          </UpdateButton>
        )}
      </FlexContainer>

      <Box pt="xl">
        {paymentInformation?.customer ? (
          <FlexContainer direction="column" gap="lg">
            <Text>{paymentInformation.customer.email}</Text>
            {anyAddressPresent && (
              <FlexContainer direction="column" gap="xs">
                {addressLineOne && <Text>{addressLineOne}</Text>}
                {addressLineTwo && (
                  <Text>
                    {addressLineTwo}
                    {FlagComponent && <FlagComponent className={styles.countryFlag} title={address?.country} />}
                  </Text>
                )}
              </FlexContainer>
            )}
          </FlexContainer>
        ) : (
          <>
            {paymentInformationLoading && (
              <FlexContainer direction="column" gap="sm">
                <LoadingSkeleton />
                <LoadingSkeleton />
              </FlexContainer>
            )}
            {paymentInformationError && (
              <DataLoadingError>
                <FormattedMessage id="settings.organization.billing.billingInformationError" />
              </DataLoadingError>
            )}
          </>
        )}
      </Box>
    </>
  );
};
