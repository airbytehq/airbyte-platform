import { createColumnHelper } from "@tanstack/react-table";
import { useMemo } from "react";
import { FormattedDate, FormattedMessage } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";
import { MaskedText } from "components/ui/Text/MaskedText";

import { useListApplications } from "core/api";
import { ApplicationRead } from "core/api/types/AirbyteClient";
import { useAuthService } from "core/services/auth";
import { links } from "core/utils/links";

import styles from "./ApplicationSettingsView.module.scss";
import { CreateApplicationControl } from "./CreateApplicationControl";
import { DeleteApplicationControl } from "./DeleteApplicationControl";
import { GenerateTokenControl } from "./GenerateTokenControl";

export const ApplicationSettingsView = () => {
  const { applicationSupport } = useAuthService();
  const { applications } = useListApplications();
  const columnHelper = useMemo(() => createColumnHelper<ApplicationRead>(), []);

  const columns = useMemo(() => {
    return [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="settings.applications.table.name" />,
        cell: (props) => {
          return <Text>{props.cell.getValue()}</Text>;
        },
      }),
      columnHelper.accessor("clientId", {
        header: () => <FormattedMessage id="settings.applications.table.clientId" />,
        cell: (props) => {
          return (
            <Text color="grey400" className={styles.clientDetailsCell}>
              {props.cell.getValue()}
            </Text>
          );
        },
        enableSorting: false,
        meta: { thClassName: styles.clientDetailsColumn },
      }),
      columnHelper.accessor("clientSecret", {
        header: () => <FormattedMessage id="settings.applications.table.clientSecret" />,
        cell: (props) => {
          return (
            <MaskedText color="grey400" className={styles.clientDetailsCell}>
              {props.cell.getValue()}
            </MaskedText>
          );
        },
        enableSorting: false,
        meta: { thClassName: styles.clientDetailsColumn },
      }),
      columnHelper.accessor("createdAt", {
        header: () => <FormattedMessage id="settings.applications.table.createdAt" />,
        cell: (props) => (
          <Text color="grey400">
            <FormattedDate value={props.row.original.createdAt * 1000} dateStyle="medium" timeStyle="short" />
          </Text>
        ),
        sortingFn: "basic",
      }),

      columnHelper.display({
        id: "actions",
        cell: (props) => {
          return (
            <FlexContainer justifyContent="flex-end">
              <GenerateTokenControl
                clientId={props.row.original.clientId}
                clientSecret={props.row.original.clientSecret}
              />
              {applicationSupport === "multiple" && (
                <DeleteApplicationControl
                  applicationId={props.row.original.id}
                  applicationName={props.row.original.name}
                />
              )}
            </FlexContainer>
          );
        },
        meta: { thClassName: styles.actionsColumn },
      }),
    ];
  }, [columnHelper, applicationSupport]);

  return (
    <>
      <FlexContainer direction="row" justifyContent="space-between" alignItems="center">
        <FlexItem>
          <Heading as="h1">
            <FormattedMessage id="settings.applications" />
          </Heading>
          <Box pt="sm">
            <Text color="grey" size="sm">
              <FormattedMessage id="settings.applications.helptext" />
              {applicationSupport === "multiple" && (
                <FormattedMessage id="settings.applications.helptext.permissions" />
              )}
              <ExternalLink href={links.apiAccess}>
                <FormattedMessage id="ui.learnMore" />
              </ExternalLink>
            </Text>
          </Box>
        </FlexItem>
        {applicationSupport === "multiple" && <CreateApplicationControl />}
      </FlexContainer>
      <Box py="lg">
        {applications.length ? (
          <Table columns={columns} data={applications} />
        ) : (
          <Box p="xl" m="xl">
            <EmptyState text={<FormattedMessage id="settings.applications.table.empty" />} icon="grid" />
          </Box>
        )}
      </Box>
    </>
  );
};
