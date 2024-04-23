import { useDeferredValue, useMemo, useState } from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation } from "react-router-dom";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Form } from "components/forms";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { ModalFooter } from "components/ui/Modal";
import { SearchInput } from "components/ui/SearchInput";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  useCreateUserInvitation,
  useCurrentOrganizationInfo,
  useListUsersInOrganization,
  useListWorkspaceAccessUsers,
} from "core/api";
import { PermissionType, WorkspaceUserAccessInfoRead } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";

import { AddUserModalBody } from "./AddUserModalBody";

export interface AddUserFormValues {
  email: string;
  permission: PermissionType;
}

const ValidationSchema: SchemaOf<AddUserFormValues> = yup.object().shape({
  email: yup.string().email("form.email.error").required(),
  permission: yup.mixed().oneOf(Object.values(PermissionType)).required(),
});

const SubmissionButton: React.FC = () => {
  const { isSubmitting, isValid } = useFormState();

  return (
    <Button type="submit" disabled={!isValid} isLoading={isSubmitting}>
      <FormattedMessage id="userInvitations.create.modal.addNew" />
    </Button>
  );
};

export const AddUserModal: React.FC<{ onSubmit: () => void }> = ({ onSubmit }) => {
  const { formatMessage } = useIntl();
  const workspaceId = useCurrentWorkspaceId();
  const organizationInfo = useCurrentOrganizationInfo();
  const canListUsersInOrganization = useIntent("ListOrganizationMembers", {
    organizationId: organizationInfo?.organizationId,
  });
  const { users } = useListUsersInOrganization(
    canListUsersInOrganization ? organizationInfo?.organizationId : undefined
  );
  const [searchValue, setSearchValue] = useState("");
  const deferredSearchValue = useDeferredValue(searchValue);
  const [selectedRow, setSelectedRow] = useState<string | null>(null);
  const { mutateAsync: createInvitation } = useCreateUserInvitation();
  const { usersWithAccess } = useListWorkspaceAccessUsers(workspaceId);
  const canInviteExternalUsers = useFeature(FeatureItem.ExternalInvitations);
  const analyticsService = useAnalyticsService();
  const location = useLocation();
  const invitedFrom = useMemo(() => {
    return location.pathname.includes("source")
      ? "source"
      : location.pathname.includes("destination")
      ? "destination"
      : "user.settings";
  }, [location.pathname]);

  const isValidEmail = useMemo(() => {
    // yup considers an empty string a valid email address so we need to check both
    return deferredSearchValue.length > 0 && yup.string().email().isValidSync(deferredSearchValue);
  }, [deferredSearchValue]);

  const onInviteSubmit = async (values: AddUserFormValues) => {
    await createInvitation({
      invitedEmail: values.email,
      permissionType: values.permission,
      scopeType: "workspace",
      scopeId: workspaceId,
    });

    analyticsService.track(Namespace.USER, Action.INVITE, {
      invited_from: invitedFrom,
    });

    onSubmit();
  };

  /*      Before the user begins typing an email address, the list of users should only be users
          who can be added to the workspace (organization users who aren't org_admin + don't have a workspace permission).  
      
          When they begin typing, we filter a list that is a superset of workspaceAccessUsers + organization users.  We want to prefer the workspaceAccessUsers
          object for a given user (if present) because it contains all relevant permissions for the user.  
          
          Then, we enrich that from the list of organization_members who don't have a permission to this workspace.
      */
  const userMap = new Map();

  usersWithAccess
    .filter((user) => {
      return deferredSearchValue.length > 0
        ? true // include all workspaceAccessUsers if there _is_ a search value
        : !user.workspacePermission && !(user.organizationPermission?.permissionType === "organization_admin"); // otherwise, show only those who can be "upgraded" by creating a permission
    })
    .forEach((user) => {
      userMap.set(user.userId, {
        userId: user.userId,
        userName: user.userName,
        userEmail: user.userEmail,
        organizationPermission: user.organizationPermission,
        workspacePermission: user.workspacePermission,
      });
    });

  users.forEach((user) => {
    if (
      user.permissionType === "organization_member" && // they are an organization_member
      !usersWithAccess.some((u) => u.userId === user.userId) // they don't have a workspace permission (they may not be listed)
    ) {
      userMap.set(user.userId, {
        userId: user.userId,
        userName: user.name,
        userEmail: user.email,
        organizationPermission: {
          permissionId: user.permissionId,
          permissionType: user.permissionType,
          organizationId: user.organizationId,
          userId: user.userId,
        },
      });
    }
  });

  const usersToFilter: WorkspaceUserAccessInfoRead[] = Array.from(userMap.values());

  const usersToList = usersToFilter.filter((user) => {
    return (
      user.userName?.toLowerCase().includes(deferredSearchValue.toLowerCase()) ||
      user.userEmail?.toLowerCase().includes(deferredSearchValue.toLowerCase())
    );
  });

  // Only allow external invitations on cloud + if there is no matching email address in the organization/workspace already (since we can't invite an existing user)
  const showInviteNewUser =
    canInviteExternalUsers && !usersToList.some((user) => user.userEmail === deferredSearchValue) && isValidEmail;

  return (
    <Form<AddUserFormValues>
      schema={ValidationSchema}
      defaultValues={{ email: "", permission: PermissionType.workspace_admin }}
      onSubmit={onInviteSubmit}
    >
      <Box p="md">
        <SearchInput
          value={searchValue}
          onChange={(e) => setSearchValue(e.target.value)}
          placeholder={formatMessage({ id: "userInvitations.create.modal.search" })}
        />
      </Box>
      <AddUserModalBody
        usersToList={usersToList}
        showInviteNewUser={showInviteNewUser}
        selectedRow={selectedRow}
        setSelectedRow={setSelectedRow}
        deferredSearchValue={deferredSearchValue}
        canInviteExternalUsers={canInviteExternalUsers}
      />
      <ModalFooter>
        <SubmissionButton />
      </ModalFooter>
    </Form>
  );
};
