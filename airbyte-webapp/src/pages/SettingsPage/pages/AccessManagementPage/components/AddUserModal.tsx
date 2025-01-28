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

import { useCreateUserInvitation, useCurrentWorkspace } from "core/api";
import { PermissionType, ScopeType } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";

import { AddUserModalBody } from "./AddUserModalBody";
import { useListUsersToAdd } from "./useListUsersToAdd";

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

export const AddUserModal: React.FC<{ onSubmit: () => void; scope: ScopeType }> = ({ onSubmit, scope }) => {
  const { formatMessage } = useIntl();
  const { workspaceId, organizationId } = useCurrentWorkspace();

  const [searchValue, setSearchValue] = useState("");
  const deferredSearchValue = useDeferredValue(searchValue);
  const [selectedRow, setSelectedRow] = useState<string | null>(null);
  const { mutateAsync: createInvitation } = useCreateUserInvitation();

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
      scopeType: scope,
      scopeId: scope === "workspace" ? workspaceId : organizationId,
    });

    analyticsService.track(Namespace.USER, Action.INVITE, {
      invited_from: invitedFrom,
    });

    onSubmit();
  };

  const usersToList = useListUsersToAdd(scope, deferredSearchValue);

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
        scope={scope}
      />
      <ModalFooter>
        <SubmissionButton />
      </ModalFooter>
    </Form>
  );
};
