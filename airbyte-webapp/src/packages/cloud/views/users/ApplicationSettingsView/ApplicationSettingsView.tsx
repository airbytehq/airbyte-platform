import { createColumnHelper } from "@tanstack/react-table";
import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";
import { MaskedText } from "components/ui/Text/MaskedText";
import { InfoTooltip } from "components/ui/Tooltip";

import { useListApplications } from "core/api";
import { ApplicationRead } from "core/api/types/AirbyteClient";

import styles from "./ApplicationSettingsView.module.scss";
import { CreateApplicationControl } from "./CreateApplicationControl";
import { DeleteApplicationControl } from "./DeleteApplicationControl";
import { GenerateTokenControl } from "./GenerateTokenControl";
export const ApplicationSettingsView = () => {
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
          <Text color="grey400">{dayjs.unix(props.row.original.createdAt).format("MMM DD, YYYY h:mmA")}</Text>
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
              <DeleteApplicationControl
                applicationId={props.row.original.id}
                applicationName={props.row.original.name}
              />
            </FlexContainer>
          );
        },
        meta: { thClassName: styles.actionsColumn },
      }),
    ];
  }, [columnHelper]);

  return (
    <Card
      title={
        <FlexContainer direction="row" justifyContent="space-between">
          <FlexContainer gap="none">
            <Heading as="h2">
              <FormattedMessage id="settings.applications" />
            </Heading>
            <InfoTooltip>
              <Text inverseColor align="center">
                <FormattedMessage
                  id="settings.applications.tooltip"
                  values={{
                    // todo: add link to docs once available
                    learnMoreLink: (children) => <ExternalLink href="">{children}</ExternalLink>,
                  }}
                />
              </Text>
            </InfoTooltip>
          </FlexContainer>
          <CreateApplicationControl />
        </FlexContainer>
      }
    >
      <Box py="lg" px="xl">
        {applications.length ? (
          <Table columns={columns} data={applications} variant="light" />
        ) : (
          <Box p="lg">
            <Text color="grey400" italicized>
              <FormattedMessage id="settings.applications.table.empty" />
            </Text>
          </Box>
        )}
      </Box>
    </Card>
  );
};
