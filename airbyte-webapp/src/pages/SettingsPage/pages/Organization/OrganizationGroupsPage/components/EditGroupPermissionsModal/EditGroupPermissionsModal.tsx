import { useQueryClient } from "@tanstack/react-query";
import { useMemo, useRef } from "react";
import { useFormState, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Form, FormSubmissionHandler } from "components/ui/forms/Form";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { groupKeys, useCreateGroupPermission, useDeleteGroupPermission, useListGroupPermissions } from "core/api";
import { GroupPermissionRead, GroupRead, PublicPermissionType } from "core/api/types/AirbyteClient";

import { applyOperationResults, computeOperations, executeOperations } from "./groupPermissionsOperations";
import { GroupPermissionsFormValues, groupPermissionsFormSchema, PermissionRowValue } from "./groupPermissionsSchema";
import { OrganizationPermissionSection } from "./OrganizationPermissionSection";
import { WorkspacePermissionsSection } from "./WorkspacePermissionsSection";

interface EditGroupPermissionsModalProps {
  group: GroupRead;
  organizationId: string;
  onCancel: () => void;
  onComplete: () => void;
}

const toRowValue = (permission: GroupPermissionRead): PermissionRowValue => ({
  permissionId: permission.permissionId,
  // `GroupPermissionRead.permissionType` is the full, 13-value `PermissionType`; this modal can
  // only ever write the 11-value `PublicPermissionType`. A row outside that set is rendered
  // disabled (see `isPublicPermissionType` in `groupPermissionsOperations.ts`) and this cast never
  // reaches an API call for it.
  permissionType: permission.permissionType as PublicPermissionType,
  workspaceId: permission.workspaceId ?? undefined,
});

export const EditGroupPermissionsModal: React.FC<EditGroupPermissionsModalProps> = ({
  group,
  organizationId,
  onCancel,
  onComplete,
}) => {
  // Deliberately a plain query result, not a suspense read: `listGroupPermissions` is
  // organization-admin gated plus entitlement gated, so a 403 is reachable for a real organization
  // admin and must render inline in the modal body.
  const { data, isInitialLoading, isError } = useListGroupPermissions(group.groupId);

  if (isInitialLoading) {
    return (
      <ModalBody>
        <FlexContainer justifyContent="center" alignItems="center">
          <LoadingSpinner />
        </FlexContainer>
      </ModalBody>
    );
  }

  if (isError || !data) {
    return (
      <ModalBody>
        <Text color="red">
          <FormattedMessage id="settings.organization.groups.editPermissions.load.error" />
        </Text>
      </ModalBody>
    );
  }

  const initialValues: GroupPermissionsFormValues = {
    organizationPermission: data.permissions.filter((permission) => permission.organizationId).map(toRowValue),
    workspacePermissions: data.permissions.filter((permission) => permission.workspaceId).map(toRowValue),
  };

  return (
    <EditGroupPermissionsForm
      group={group}
      organizationId={organizationId}
      initialValues={initialValues}
      onCancel={onCancel}
      onComplete={onComplete}
    />
  );
};

interface EditGroupPermissionsFormProps {
  group: GroupRead;
  organizationId: string;
  initialValues: GroupPermissionsFormValues;
  onCancel: () => void;
  onComplete: () => void;
}

const EditGroupPermissionsForm: React.FC<EditGroupPermissionsFormProps> = ({
  group,
  organizationId,
  initialValues,
  onCancel,
  onComplete,
}) => {
  const queryClient = useQueryClient();
  const { mutateAsync: createGroupPermissionMutation } = useCreateGroupPermission();
  const { mutateAsync: deleteGroupPermissionMutation } = useDeleteGroupPermission();
  // A mutable baseline that successful operations advance, so a retry after a partial failure
  // recomputes exactly the outstanding work (D7). Deliberately a ref, not state: it must not
  // trigger a re-render, and it must survive across submits within this one modal instance.
  const baselineRef = useRef<GroupPermissionsFormValues>(initialValues);

  const mutations = useMemo(
    () => ({
      createGroupPermission: createGroupPermissionMutation,
      deleteGroupPermission: (permissionId: string) =>
        deleteGroupPermissionMutation({ groupId: group.groupId, permissionId }).then(() => undefined),
    }),
    [createGroupPermissionMutation, deleteGroupPermissionMutation, group.groupId]
  );

  const onSubmit: FormSubmissionHandler<GroupPermissionsFormValues> = async (values, methods) => {
    const operations = computeOperations({ groupId: group.groupId, organizationId }, baselineRef.current, values);
    if (operations.length === 0) {
      return { resetValues: values };
    }

    const results = await executeOperations(operations, mutations);
    const outcome = applyOperationResults(baselineRef.current, values, results);
    baselineRef.current = outcome.baseline;

    // Once per save, after every operation has settled, and on the failure path too — a partial
    // failure still changed the server. The mutation hooks deliberately do not invalidate, because
    // this query is active while the modal is open and per-call invalidation would refetch
    // repeatedly mid-save, racing the baseline this modal treats as the source of truth.
    queryClient.invalidateQueries(groupKeys.permissionList(group.groupId));

    if (outcome.hasFailure) {
      // Fix the row values in place (D8) before setting errors, so a subsequent Save reissues
      // only the outstanding work and the visible rows already reflect what actually happened.
      methods.reset(outcome.values, { keepDirty: false });
      outcome.errors.forEach(({ rowPath, messageId }) => {
        methods.setError(rowPath as never, { type: "manual", message: messageId });
      });
      // `Form.tsx` resets on a resolved submission, which would wipe the errors just set. It does
      // not reset on a rejection, so throwing here is what keeps them on screen (D8).
      throw new Error("EditGroupPermissionsModal: one or more permission changes failed");
    }

    return { resetValues: outcome.values };
  };

  return (
    <Form<GroupPermissionsFormValues>
      zodSchema={groupPermissionsFormSchema}
      defaultValues={initialValues}
      onSubmit={onSubmit}
      onSuccess={onComplete}
    >
      <EditGroupPermissionsFormBody
        group={group}
        organizationId={organizationId}
        baselineRef={baselineRef}
        onCancel={onCancel}
      />
    </Form>
  );
};

interface EditGroupPermissionsFormBodyProps {
  group: GroupRead;
  organizationId: string;
  baselineRef: React.MutableRefObject<GroupPermissionsFormValues>;
  onCancel: () => void;
}

const EditGroupPermissionsFormBody: React.FC<EditGroupPermissionsFormBodyProps> = ({
  group,
  organizationId,
  baselineRef,
  onCancel,
}) => {
  return (
    <>
      <ModalBody>
        <OrganizationPermissionSection />
        <WorkspacePermissionsSection organizationId={organizationId} />
      </ModalBody>
      <ModalFooter>
        <ModalFooterButtons
          group={group}
          organizationId={organizationId}
          baselineRef={baselineRef}
          onCancel={onCancel}
        />
      </ModalFooter>
    </>
  );
};

interface ModalFooterButtonsProps {
  group: GroupRead;
  organizationId: string;
  baselineRef: React.MutableRefObject<GroupPermissionsFormValues>;
  onCancel: () => void;
}

/**
 * Save is gated on the computed operation list being empty, not on `formState.isDirty` (D6): the
 * submit handler has to compute this diff anyway, so this is the one source of truth, and
 * `isDirty` is unreliable across `useFieldArray`. Written inline rather than with
 * `FormSubmissionButtons`, which has no prop to override its own dirty gate.
 */
const ModalFooterButtons: React.FC<ModalFooterButtonsProps> = ({ group, organizationId, baselineRef, onCancel }) => {
  const { isSubmitting } = useFormState<GroupPermissionsFormValues>();
  const values = useWatch<GroupPermissionsFormValues>();

  const operations = useMemo(
    () =>
      computeOperations(
        { groupId: group.groupId, organizationId },
        baselineRef.current,
        values as GroupPermissionsFormValues
      ),
    [group.groupId, organizationId, baselineRef, values]
  );

  return (
    <FlexContainer justifyContent="flex-end">
      <Button type="button" variant="secondary" disabled={isSubmitting} onClick={onCancel}>
        <FormattedMessage id="form.cancel" />
      </Button>
      <Button type="submit" isLoading={isSubmitting} disabled={operations.length === 0 || isSubmitting}>
        <FormattedMessage id="settings.organization.groups.editPermissions.save" />
      </Button>
    </FlexContainer>
  );
};
