import { Popover, PopoverButton, PopoverPanel, useClose } from "@headlessui/react";
import classNames from "classnames";
import { useRef, useState } from "react";
import { flushSync } from "react-dom";
import { FormattedMessage, useIntl } from "react-intl";
// eslint-disable-next-line no-restricted-imports
import { Link } from "react-router-dom";
import { Components, Virtuoso, VirtuosoHandle, ItemContent } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { BrandingBadge, useGetProductBranding } from "components/ui/BrandingBadge";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import AirbyteLogoIcon from "components/ui/illustrations/airbyte-logo-icon.svg?react";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useListOrganizationsByUser, useListOrganizationsByUserInfinite } from "core/api";
import { OrganizationInfoRead } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";
import { RoutePaths } from "pages/routePaths";

import styles from "./OrganizationPicker.module.scss";

export const useHasMultipleOrganizations = () => {
  const user = useCurrentUser();
  const { organizations } = useListOrganizationsByUser({
    userId: user.userId,
    pagination: { pageSize: 2, rowOffset: 0 },
  });
  return organizations.length > 1;
};

type OrganizationNameAndId = Pick<OrganizationInfoRead, "organizationId" | "organizationName">;

export const OrganizationPicker = () => {
  const currentOrganizationInfo = useCurrentOrganizationInfo();
  const product = useGetProductBranding();
  const hasMultipleOrganizations = useHasMultipleOrganizations();
  const { formatMessage } = useIntl();

  const currentOrgName =
    currentOrganizationInfo.organizationName.length > 0
      ? currentOrganizationInfo.organizationName
      : formatMessage({ id: "organization.emptyName" });

  if (!hasMultipleOrganizations) {
    return (
      <FlexContainer className={styles.organizationPicker__singleOrgWrapper} alignItems="center" title={currentOrgName}>
        <AirbyteLogoIcon className={styles.organizationPicker__logo} />
        <FlexContainer direction="column" gap="sm" className={styles.organizationPicker__currentOrg}>
          <Text
            size="lg"
            bold
            className={styles.organizationPicker__orgName}
            italicized={!currentOrganizationInfo.organizationName}
          >
            {currentOrgName}
          </Text>
          <BrandingBadge product={product} />
        </FlexContainer>
      </FlexContainer>
    );
  }

  return (
    <Popover className={styles.organizationPicker}>
      <PopoverButton className={styles.organizationPicker__button} title={currentOrgName}>
        <AirbyteLogoIcon className={styles.organizationPicker__logo} />
        <FlexContainer direction="column" gap="sm" className={styles.organizationPicker__currentOrg}>
          <Text
            size="lg"
            bold
            className={styles.organizationPicker__orgName}
            italicized={!currentOrganizationInfo.organizationName}
          >
            {currentOrgName}
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

interface OrganizationPickerContext {
  isFetchingNextPage: boolean;
  closePopover: () => void;
  currentOrganizationId: string;
}

const OrganizationPickerPanelContent = () => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();
  const currentOrganization = useCurrentOrganizationInfo();
  const [searchValue, setSearchValue] = useState("");
  const closePopover = useClose();

  const {
    data: organizationPages,
    isLoading,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
  } = useListOrganizationsByUserInfinite({
    nameContains: searchValue,
    userId: user.userId,
  });

  const organizationsList = organizationPages?.pages.flatMap((page) => page.organizations) ?? [];

  const [totalListHeight, setTotalListHeight] = useState(0);
  // Single line rows in the picker are approximately 36px tall. This is used as an approximate fallback value in the
  // first render when totalListHeight is unknown.
  const listHeight = Math.min(Math.max(36 * organizationsList.length, totalListHeight), 300);

  const virtuosoRef = useRef<VirtuosoHandle | null>(null);

  return (
    <>
      <Box p="md">
        <SearchInput
          value={searchValue}
          onChange={(value) => {
            setSearchValue(value);
            setTotalListHeight(0);
            virtuosoRef.current?.scrollTo({ top: 0 });
          }}
          placeholder={formatMessage({ id: "sidebar.searchOrganizations" })}
          debounceTimeout={150}
        />
      </Box>
      {!isLoading && organizationsList.length === 0 && (
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
      {!isLoading && organizationsList.length > 0 && (
        <div className={styles.organizationPicker__options}>
          <Virtuoso<OrganizationNameAndId, OrganizationPickerContext>
            style={{
              height: listHeight,
              width: "100%",
            }}
            data={organizationsList}
            context={{
              isFetchingNextPage,
              closePopover,
              currentOrganizationId: currentOrganization.organizationId,
            }}
            endReached={() => {
              if (hasNextPage && !isFetchingNextPage) {
                fetchNextPage();
              }
            }}
            totalListHeightChanged={setTotalListHeight}
            components={{
              Footer,
            }}
            itemContent={OrganizationLink}
          />
        </div>
      )}
    </>
  );
};

const OrganizationLink: ItemContent<OrganizationNameAndId, OrganizationPickerContext> = (
  _index,
  organization,
  context
) => {
  return (
    <Link
      key={organization.organizationId}
      onClick={() => flushSync(() => context.closePopover())}
      to={`/${RoutePaths.Organization}/${organization.organizationId}`}
      className={classNames(styles.organizationPicker__option, {
        [styles["organizationPicker__option--selected"]]: organization.organizationId === context.currentOrganizationId,
      })}
    >
      <Text
        bold={organization.organizationId === context.currentOrganizationId}
        italicized={!organization.organizationName}
      >
        {organization.organizationName.length > 0 ? (
          organization.organizationName
        ) : (
          <FormattedMessage id="organization.emptyName" />
        )}
      </Text>
    </Link>
  );
};

const Footer: Components<OrganizationNameAndId, OrganizationPickerContext>["Footer"] = ({ context }) => {
  if (!context?.isFetchingNextPage) {
    return null;
  }
  return (
    <Box px="md" pb="md">
      <FlexContainer direction="column" gap="md">
        <LoadingSkeleton />
        <LoadingSkeleton />
        <LoadingSkeleton />
      </FlexContainer>
    </Box>
  );
};
