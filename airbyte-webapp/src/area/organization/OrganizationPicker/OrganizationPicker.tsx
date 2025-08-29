import { Popover, PopoverButton, PopoverPanel, useClose } from "@headlessui/react";
import classNames from "classnames";
import { useState } from "react";
import { flushSync } from "react-dom";
import { FormattedMessage, useIntl } from "react-intl";
// eslint-disable-next-line no-restricted-imports
import { Link } from "react-router-dom";

import AirbyteLogoIcon from "components/illustrations/airbyte-logo-icon.svg?react";
import { Box } from "components/ui/Box";
import { BrandingBadge, useGetProductBranding } from "components/ui/BrandingBadge";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useListOrganizationsByUser, useListOrganizationsByUserInfinite } from "core/api";
import { useCurrentUser } from "core/services/auth";
import { RoutePaths } from "pages/routePaths";

import styles from "./OrganizationPicker.module.scss";
import { useCurrentOrganizationId } from "../utils";

export const useHasMultipleOrganizations = () => {
  const user = useCurrentUser();
  const { organizations } = useListOrganizationsByUser({
    userId: user.userId,
    pagination: { pageSize: 2, rowOffset: 0 },
  });
  return organizations.length > 1;
};

export const OrganizationPicker = () => {
  const currentOrganizationInfo = useCurrentOrganizationInfo();
  const product = useGetProductBranding();
  const hasMultipleOrganizations = useHasMultipleOrganizations();

  if (!hasMultipleOrganizations) {
    return (
      <FlexContainer className={styles.organizationPicker__singleOrgWrapper} alignItems="center">
        <AirbyteLogoIcon className={styles.organizationPicker__logo} />
        <FlexContainer direction="column" gap="sm" className={styles.organizationPicker__currentOrg}>
          <Text size="lg" bold className={styles.organizationPicker__orgName}>
            {currentOrganizationInfo.organizationName}
          </Text>
          <BrandingBadge product={product} />
        </FlexContainer>
      </FlexContainer>
    );
  }

  return (
    <Popover className={styles.organizationPicker}>
      <PopoverButton className={styles.organizationPicker__button}>
        <AirbyteLogoIcon className={styles.organizationPicker__logo} />
        <FlexContainer direction="column" gap="sm" className={styles.organizationPicker__currentOrg}>
          <Text size="lg" bold className={styles.organizationPicker__orgName}>
            {currentOrganizationInfo.organizationName}
          </Text>
          <BrandingBadge product={product} />
        </FlexContainer>

        <Icon type="chevronUpDown" size="xs" color="disabled" className={styles.organizationPicker__icon} />
      </PopoverButton>
      <PopoverPanel anchor={{ to: "bottom start", offset: 5 }} className={styles.organizationPicker__panel}>
        <OrganizationPickerPanelContent />
      </PopoverPanel>
    </Popover>
  );
};

const OrganizationPickerPanelContent = () => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();
  const currentOrganizationId = useCurrentOrganizationId();
  const [searchValue, setSearchValue] = useState("");
  const closePopover = useClose();

  const { data: organizationPages, isLoading } = useListOrganizationsByUserInfinite({
    nameContains: searchValue,
    userId: user.userId,
  });

  const infiniteOrganizations = organizationPages?.pages.flatMap((page) => page.organizations) ?? [];

  return (
    <>
      <Box p="md">
        <SearchInput
          value={searchValue}
          onChange={setSearchValue}
          placeholder={formatMessage({ id: "sidebar.searchOrganizations" })}
          debounceTimeout={150}
        />
      </Box>
      {!isLoading && infiniteOrganizations.length === 0 && (
        <Box px="md" pb="md">
          <Text color="grey" align="center">
            <FormattedMessage id="sidebar.noOrganizationsFound" />
          </Text>
        </Box>
      )}
      {isLoading && (
        <Box px="md" pb="md">
          <FlexContainer direction="column">
            <LoadingSkeleton />
            <LoadingSkeleton />
            <LoadingSkeleton />
          </FlexContainer>
        </Box>
      )}
      {!isLoading && infiniteOrganizations.length > 0 && (
        <div className={styles.organizationPicker__options}>
          {infiniteOrganizations.map((org) => (
            <Link
              key={org.organizationId}
              onClick={() => flushSync(() => closePopover())}
              to={`/${RoutePaths.Organization}/${org.organizationId}`}
              className={classNames(styles.organizationPicker__option, {
                [styles["organizationPicker__option--selected"]]: org.organizationId === currentOrganizationId,
              })}
            >
              <Text bold={org.organizationId === currentOrganizationId}>{org.organizationName}</Text>
            </Link>
          ))}
        </div>
      )}
    </>
  );
};
