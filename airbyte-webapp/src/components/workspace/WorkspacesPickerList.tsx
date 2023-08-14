import { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useUpdateEffect } from "react-use";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useListWorkspacesAsync } from "core/api";
import { RoutePaths } from "pages/routePaths";

import styles from "./WorkspacesPickerList.module.scss";

export const WorkspacesPickerList: React.FC<{ closePicker: () => void }> = ({ closePicker }) => {
  const [workspaceFilter, setWorkspaceFilter] = useState("");
  const { data: workspaceList, isLoading } = useListWorkspacesAsync();
  const location = useLocation();

  useUpdateEffect(() => {
    closePicker();
  }, [closePicker, location.pathname, location.search]);

  const filteredWorkspaces = useMemo(() => {
    return (
      workspaceList?.workspaces.filter((workspace) => {
        return workspace.name.toLowerCase().includes(workspaceFilter.toLowerCase());
      }) ?? []
    );
  }, [workspaceFilter, workspaceList?.workspaces]);

  return isLoading ? (
    <Box p="lg">
      <LoadingSpinner />
    </Box>
  ) : (
    <div>
      <div className={styles.workspaceSearch}>
        <SearchInput value={workspaceFilter} onChange={(e) => setWorkspaceFilter(e.target.value)} inline />
      </div>
      {!filteredWorkspaces.length ? (
        <Box p="md">
          <FormattedMessage id="workspaces.noWorkspaces" />
        </Box>
      ) : (
        <div className={styles.workspacesPickerList__scrollbox}>
          <ul>
            {filteredWorkspaces.map((workspace) => {
              // const roleId = workspace.permissions ? `role.${permissions.toLowerCase()}` : ""; for when we have this on the workspace object

              return (
                <li key={workspace.slug}>
                  <Link variant="primary" to={`/${RoutePaths.Workspaces}/${workspace.workspaceId}`}>
                    <Box py="md" px="md" className={styles.workspacesPickerList__item}>
                      <FlexContainer direction="column" justifyContent="center" gap="sm">
                        <Text align="left" color="blue" bold size="md">
                          {workspace.name}
                        </Text>
                        {/* irrelevant until we get roles and orgs */}
                        {false && (
                          <FlexContainer alignItems="baseline" justifyContent="flex-start">
                            {/* when we get organizations on the workspace object */}
                            {false && (
                              <Text size="sm" color="grey">
                                {/* {orgName} */}
                              </Text>
                            )}
                            {/* for when we get permissions on the user/workspace */}
                            {false && (
                              <Box py="xs" px="md" mb="sm" className={styles.workspacesPickerList__orgPill}>
                                <Text size="sm" color="green600">
                                  {/* <FormattedMessage
                                    id="user.roleLabel"
                                    values={{ role: <FormattedMessage id={roleId} /> }}
                                  /> */}
                                </Text>
                              </Box>
                            )}
                          </FlexContainer>
                        )}
                      </FlexContainer>
                    </Box>
                  </Link>
                </li>
              );
            })}
          </ul>
        </div>
      )}

      <Box py="lg">
        <Link variant="primary" to={`/${RoutePaths.Workspaces}`}>
          <Text color="blue" size="md" bold align="center">
            <FormattedMessage id="workspaces.seeAll" />
          </Text>
        </Link>
      </Box>
    </div>
  );
};
