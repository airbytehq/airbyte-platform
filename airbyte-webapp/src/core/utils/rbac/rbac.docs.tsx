import React, { Suspense, useState } from "react";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";

import { PermissionType } from "core/api/types/AirbyteClient";

import {
  RbacQuery,
  RbacResource,
  RbacResourceHierarchy,
  RbacRole,
  RbacRoleHierarchy,
  useRbacPermissionsQuery,
} from "./rbacPermissionsQuery";
import { withProviders } from "../../../../.storybook/withProvider";

const PermissionBuilder: React.FC<{
  resource: RbacResource;
  setResource: (resource: RbacResource) => void;
  role: RbacRole;
  setRole: (role: RbacRole) => void;
  id: string;
  setId: (id: string) => void;
}> = ({ resource, setResource, role, setRole, id, setId }) => {
  return (
    <div style={{ display: "inline-block" }}>
      <FlexContainer>
        <FlexItem>
          <ListBox
            selectedValue={resource}
            options={[
              { label: "Instance", value: RbacResourceHierarchy[0] },
              { label: "Organization", value: RbacResourceHierarchy[1] },
              { label: "Workspace", value: RbacResourceHierarchy[2] },
            ]}
            onSelect={(value) => {
              value === RbacResourceHierarchy[0] && setId("");
              setResource(value);
            }}
          />
        </FlexItem>
        <FlexItem>
          <ListBox
            selectedValue={role}
            options={[
              { label: "Admin", value: RbacRoleHierarchy[0] },
              { label: "Editor", value: RbacRoleHierarchy[1] },
              { label: "Reader", value: RbacRoleHierarchy[2] },
            ]}
            onSelect={setRole}
          />
        </FlexItem>
        <FlexItem>
          <Input
            placeholder="resource id"
            value={id}
            disabled={resource === RbacResourceHierarchy[0]} // disable uuid input for Instance permissions
            onChange={(e) => {
              setId(e.target.value);
            }}
          />
        </FlexItem>
      </FlexContainer>
    </div>
  );
};

interface PermissionQueryResultProps {
  resourceType: RbacResource;
  resourceId: string;
  role: RbacRole;
  permissions: RbacQuery[];
}
const PermissionQueryResult: React.FC<PermissionQueryResultProps> = ({
  resourceType,
  resourceId,
  role,
  permissions,
}) => {
  const query = resourceType === "INSTANCE" ? { resourceType, role } : { resourceType, role, resourceId };

  const hasMatchingPermissions = useRbacPermissionsQuery(
    permissions.map(({ resourceType, role, resourceId }) => {
      return {
        permissionId: "",
        permissionType: `${resourceType.toLowerCase() as Lowercase<typeof resourceType>}_${
          role.toLowerCase() as Lowercase<typeof role>
        }` as PermissionType,
        userId: "",
        organizationId: resourceType === "ORGANIZATION" ? resourceId || undefined : undefined,
        workspaceId: resourceType === "WORKSPACE" ? resourceId || undefined : undefined,
      };
    }),
    query
  );

  return <span>{hasMatchingPermissions ? "✅" : "❌"}</span>;
};

class PermissionQueryResultWithErrorBoundary extends React.Component<PermissionQueryResultProps> {
  state = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidUpdate(prevProps: PermissionQueryResultProps) {
    if (prevProps.resourceType !== this.props.resourceType || prevProps.resourceId !== this.props.resourceId) {
      this.setState({ hasError: false });
    }
  }

  render() {
    const { resourceType, resourceId, role, permissions } = this.props;

    return this.state.hasError ? (
      "Error executing query, are you sure it's an UUID of an existing workspace?"
    ) : (
      <>
        Is permission granted?{" "}
        <PermissionQueryResult
          resourceType={resourceType}
          resourceId={resourceId}
          role={role}
          permissions={permissions}
        />
      </>
    );
  }
}

const PermisisonTestViewInner = () => {
  const [queryResource, setQueryResource] = useState<RbacResource>(RbacResourceHierarchy[0]);
  const [queryRole, setQueryRole] = useState<RbacRole>(RbacRoleHierarchy[0]);
  const [queryId, setQueryId] = useState<string>("");

  const [permissions, setPermissions] = useState<RbacQuery[]>([]);
  const updatePermission = (index: number, type: "resourceType" | "role" | "resourceId", value: string) => {
    const nextPermissions = [...permissions];
    // typescript can't validate that `value` is assignable to `type`
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore-next-line
    nextPermissions[index][type] = value;
    setPermissions(nextPermissions);
  };

  return (
    <div style={{ width: 500, border: "1px solid #ccc", marginLeft: "auto", marginRight: "auto", padding: 15 }}>
      <PermissionQueryResultWithErrorBoundary
        resourceType={queryResource}
        resourceId={queryId}
        role={queryRole}
        permissions={permissions}
      />
      <br />
      <br />
      <div>
        <strong>Query</strong>
        <br />
        <PermissionBuilder
          resource={queryResource}
          setResource={setQueryResource}
          role={queryRole}
          setRole={setQueryRole}
          id={queryId}
          setId={setQueryId}
        />
      </div>
      <div>&nbsp;</div>
      <div>
        <strong>
          User permissions{" "}
          <Button
            variant="secondary"
            icon={<Icon type="plus" />}
            onClick={() => {
              setPermissions([...permissions, { resourceType: RbacResourceHierarchy[0], role: RbacRoleHierarchy[0] }]);
            }}
          />
        </strong>
        <br />
        {permissions.map((permission, index) => (
          <div style={{ marginTop: 5 }} key={index}>
            <PermissionBuilder
              resource={permission.resourceType}
              setResource={(resource) => {
                updatePermission(index, "resourceType", resource);
              }}
              role={permission.role}
              setRole={(role) => {
                updatePermission(index, "role", role);
              }}
              id={permission.resourceId ?? ""}
              setId={(id) => {
                updatePermission(index, "resourceId", id);
              }}
            />
            &nbsp;
            <Button
              variant="secondary"
              icon={<Icon type="cross" />}
              onClick={() => {
                const nextPermissions = [...permissions];
                nextPermissions.splice(index, 1);
                setPermissions(nextPermissions);
              }}
            />
          </div>
        ))}
      </div>
    </div>
  );
};

export const PermissionTestView = () => {
  return withProviders(() => (
    <Suspense fallback={<div>Loading organization:workspace mapping</div>}>
      <PermisisonTestViewInner />
    </Suspense>
  ));
};
