import { Fragment, useMemo } from "react";
import { useFieldArray, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { FormControl, FormControlErrorMessage } from "components/ui/forms/FormControl";
import { Option } from "components/ui/ListBox";
import { RemoveButton } from "components/ui/RemoveButton/RemoveButton";
import { Text } from "components/ui/Text";

import { useListWorkspacesInOrganization } from "core/api";
import { PublicPermissionType } from "core/api/types/AirbyteClient";
import { permissionStringDictionary } from "pages/SettingsPage/pages/AccessManagementPage/components/util";

import styles from "./EditGroupPermissionsModal.module.scss";
import { isPublicPermissionType } from "./groupPermissionsOperations";
import { GroupPermissionsFormValues, WORKSPACE_PERMISSION_TYPES } from "./groupPermissionsSchema";

interface WorkspacePermissionsSectionProps {
  organizationId: string;
}

export const WorkspacePermissionsSection: React.FC<WorkspacePermissionsSectionProps> = ({ organizationId }) => {
  const { formatMessage } = useIntl();
  const { fields, append, remove } = useFieldArray<GroupPermissionsFormValues, "workspacePermissions">({
    name: "workspacePermissions",
  });

  const workspaceRoleOptions: Array<Option<string>> = useMemo(
    () =>
      WORKSPACE_PERMISSION_TYPES.map((permissionType) => ({
        value: permissionType,
        label: formatMessage({ id: permissionStringDictionary[permissionType].role }),
      })),
    [formatMessage]
  );
  // No `pagination` argument: `listWorkspacesInOrganization` only paginates when `pagination` is
  // non-null, so omitting it returns every workspace in one page. Passing it would silently cap
  // this picker at 10 (`RegionsTable` omits it for the same reason).
  const { data: workspacesData } = useListWorkspacesInOrganization({ organizationId });
  const workspaces = workspacesData?.pages[0]?.workspaces ?? [];

  const rows = useWatch<GroupPermissionsFormValues, "workspacePermissions">({ name: "workspacePermissions" }) ?? [];

  const hasRows = fields.length > 0;

  return (
    <div className={styles.section}>
      <div className={styles.section__header}>
        <Text size="lg" bold>
          <FormattedMessage id="settings.organization.groups.editPermissions.workspacePermissions" />
        </Text>
      </div>
      <div className={styles.section__body}>
        {hasRows && (
          <FlexContainer className={styles.columnHeaders}>
            <FlexItem grow className={styles.row__field}>
              <Text size="sm" color="grey">
                <FormattedMessage id="settings.organization.groups.editPermissions.workspace" />
              </Text>
            </FlexItem>
            <FlexItem grow className={styles.row__field}>
              <Text size="sm" color="grey">
                <FormattedMessage id="settings.organization.groups.editPermissions.workspaceRole" />
              </Text>
            </FlexItem>
          </FlexContainer>
        )}
        {fields.map((field, index) => {
          const disabled = !isPublicPermissionType(field.permissionType);
          const currentWorkspaceId = rows[index]?.workspaceId;
          // D5: exclude workspaces already chosen in the other rows, never this row's own value.
          const selectedElsewhere = new Set(
            rows.filter((_, otherIndex) => otherIndex !== index).map((row) => row.workspaceId)
          );
          const workspaceOptions: Array<Option<string>> = workspaces
            .filter(
              (workspace) =>
                workspace.workspaceId === currentWorkspaceId || !selectedElsewhere.has(workspace.workspaceId)
            )
            .map((workspace) => ({ value: workspace.workspaceId, label: workspace.name }));

          const rowPath = `workspacePermissions.${index}`;
          return (
            <Fragment key={field.id}>
              <FlexContainer alignItems="flex-start" className={styles.row}>
                <FlexItem grow className={styles.row__field}>
                  <FormControl<GroupPermissionsFormValues>
                    fieldType="dropdown"
                    name={`workspacePermissions.${index}.workspaceId`}
                    options={workspaceOptions}
                    disabled={disabled}
                    // The row renders its own error message below, so the 30px `FormControl`
                    // reserves for one would be dead space above the add button.
                    reserveSpaceForError={false}
                    data-testid={`workspacePermissions.${index}.workspaceId`}
                  />
                </FlexItem>
                <FlexItem grow className={styles.row__field}>
                  <FormControl<GroupPermissionsFormValues>
                    fieldType="dropdown"
                    name={`workspacePermissions.${index}.permissionType`}
                    options={workspaceRoleOptions}
                    disabled={disabled}
                    reserveSpaceForError={false}
                    data-testid={`workspacePermissions.${index}.permissionType`}
                  />
                </FlexItem>
                <RemoveButton
                  className={styles.row__removeButton}
                  disabled={disabled}
                  onClick={() => remove(index)}
                  aria-label={formatMessage({ id: "settings.organization.groups.editPermissions.removePermission" })}
                />
              </FlexContainer>
              <FormControlErrorMessage<GroupPermissionsFormValues> name={rowPath as never} />
            </Fragment>
          );
        })}
        <FlexContainer>
          <Button
            type="button"
            variant="secondary"
            icon="plus"
            onClick={() => append({ permissionType: PublicPermissionType.workspace_reader, workspaceId: undefined })}
          >
            <FormattedMessage id="settings.organization.groups.editPermissions.addPermission" />
          </Button>
        </FlexContainer>
      </div>
    </div>
  );
};
