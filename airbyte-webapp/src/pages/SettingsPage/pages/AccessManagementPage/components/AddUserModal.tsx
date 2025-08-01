import { useDeferredValue, useMemo, useState } from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation } from "react-router-dom";
import { z } from "zod";

import { Form } from "components/forms";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { ModalFooter } from "components/ui/Modal";
import { SearchInput } from "components/ui/SearchInput";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreateUserInvitation } from "core/api";
import { PermissionType, ScopeType } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";

import { AddUserModalBody } from "./AddUserModalBody";
import { useListUsersToAdd } from "./useListUsersToAdd";
import { getInitialPermissionType } from "./util";

const ValidationSchema = z.object({
  email: z.string().trim().email("form.email.error"),
  permission: z.nativeEnum(PermissionType),
});

export type AddUserFormValues = z.infer<typeof ValidationSchema>;

interface AddUserModalInitialValues {
  searchValue: string;
  email: string;
  permission: PermissionType;
  selectedRow: string | null;
}

const SubmissionButton: React.FC = () => {
  const { isSubmitting, isValid } = useFormState();

  return (
    <Button type="submit" disabled={!isValid} isLoading={isSubmitting}>
      <FormattedMessage id="userInvitations.create.modal.addNew" />
    </Button>
  );
};

export const AddUserModal: React.FC<{
  onSubmit: () => void;
  scope: ScopeType;
  initialValues?: AddUserModalInitialValues;
}> = ({ onSubmit, scope, initialValues }) => {
  const canInviteExternalUsers = useFeature(FeatureItem.ExternalInvitations);

  const { formatMessage } = useIntl();
  const workspaceId = useCurrentWorkspaceId();
  const organizationId = useCurrentOrganizationId();

  const [searchValue, setSearchValue] = useState(initialValues?.searchValue ?? "");
  const deferredSearchValue = useDeferredValue(searchValue);
  const [selectedRow, setSelectedRow] = useState<string | null>(initialValues?.selectedRow ?? null);
  const { mutateAsync: createInvitation } = useCreateUserInvitation();

  const analyticsService = useAnalyticsService();
  const location = useLocation();
  const invitedFrom = useMemo(() => {
    return location.pathname.includes("source")
      ? "source"
      : location.pathname.includes("destination")
      ? "destination"
      : "user.settings";
  }, [location.pathname]);

  const isValidEmail = useMemo(
    () => z.string().nonempty().email().safeParse(deferredSearchValue).success,
    [deferredSearchValue]
  );

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
      zodSchema={ValidationSchema}
      defaultValues={{
        email: initialValues?.email ?? "",
        permission: initialValues?.permission ?? getInitialPermissionType(scope),
      }}
      onSubmit={onInviteSubmit}
    >
      <Box p="md">
        <SearchInput
          value={searchValue}
          onChange={setSearchValue}
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
