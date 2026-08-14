import { Fragment, useMemo } from "react";
import { useFieldArray } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { FormControl, FormControlErrorMessage } from "components/ui/forms/FormControl";
import { Option } from "components/ui/ListBox";
import { RemoveButton } from "components/ui/RemoveButton/RemoveButton";
import { Text } from "components/ui/Text";

import { PublicPermissionType } from "core/api/types/AirbyteClient";
import { permissionStringDictionary } from "pages/SettingsPage/pages/AccessManagementPage/components/util";

import styles from "./EditGroupPermissionsModal.module.scss";
import { isPublicPermissionType } from "./groupPermissionsOperations";
import { GroupPermissionsFormValues, ORGANIZATION_PERMISSION_TYPES } from "./groupPermissionsSchema";

export const OrganizationPermissionSection: React.FC = () => {
  const { formatMessage } = useIntl();
  const { fields, append, remove } = useFieldArray<GroupPermissionsFormValues, "organizationPermission">({
    name: "organizationPermission",
  });

  const organizationRoleOptions: Array<Option<string>> = useMemo(
    () =>
      ORGANIZATION_PERMISSION_TYPES.map((permissionType) => ({
        value: permissionType,
        label: formatMessage({ id: permissionStringDictionary[permissionType].role }),
      })),
    [formatMessage]
  );

  const hasRow = fields.length > 0;

  return (
    <div className={styles.section}>
      <div className={styles.section__header}>
        <Text size="lg" bold>
          <FormattedMessage id="settings.organization.groups.editPermissions.organizationPermission" />
        </Text>
      </div>
      <div className={styles.section__body}>
        {hasRow && (
          <div className={styles.columnHeaders}>
            <Text size="sm" color="grey">
              <FormattedMessage id="settings.organization.groups.editPermissions.organizationRole" />
            </Text>
          </div>
        )}
        {fields.map((field, index) => {
          const disabled = !isPublicPermissionType(field.permissionType);
          const rowPath = `organizationPermission.${index}`;
          return (
            <Fragment key={field.id}>
              <div className={styles.row}>
                <FlexItem grow className={styles.row__field}>
                  <FormControl<GroupPermissionsFormValues>
                    fieldType="dropdown"
                    name={`organizationPermission.${index}.permissionType`}
                    options={organizationRoleOptions}
                    disabled={disabled}
                    // The row renders its own error message below, so the 30px `FormControl`
                    // reserves for one would be dead space above the add button.
                    reserveSpaceForError={false}
                    data-testid={`organizationPermission.${index}.permissionType`}
                  />
                </FlexItem>
                <RemoveButton
                  className={styles.row__removeButton}
                  disabled={disabled}
                  onClick={() => remove(index)}
                  aria-label={formatMessage({ id: "settings.organization.groups.editPermissions.removePermission" })}
                />
              </div>
              <FormControlErrorMessage<GroupPermissionsFormValues> name={rowPath as never} />
            </Fragment>
          );
        })}
        <FlexContainer>
          <Button
            type="button"
            variant="secondary"
            icon="plus"
            disabled={hasRow}
            onClick={() => append({ permissionType: PublicPermissionType.organization_member })}
          >
            <FormattedMessage id="settings.organization.groups.editPermissions.addPermission" />
          </Button>
        </FlexContainer>
      </div>
    </div>
  );
};
