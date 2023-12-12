import pick from "lodash/pick";
import { useContext, useState, createContext, useCallback } from "react";
import { useIntl } from "react-intl";
import { useAsyncFn } from "react-use";

import { SchemaError, useGetConnection, useGetConnectionQuery, useUpdateConnection } from "core/api";
import {
  AirbyteCatalog,
  ConnectionStatus,
  WebBackendConnectionRead,
  WebBackendConnectionUpdate,
} from "core/api/types/AirbyteClient";

import { ConnectionFormServiceProvider } from "../ConnectionForm/ConnectionFormService";
import { useNotificationService } from "../Notification";

interface ConnectionEditProps {
  connectionId: string;
}

export interface ConnectionCatalog {
  syncCatalog: AirbyteCatalog;
  catalogId?: string;
}

interface ConnectionEditHook {
  connection: WebBackendConnectionRead;
  setConnection: (connection: WebBackendConnectionRead) => void;
  connectionUpdating: boolean;
  schemaError?: Error;
  schemaRefreshing: boolean;
  schemaHasBeenRefreshed: boolean;
  updateConnection: (connectionUpdates: WebBackendConnectionUpdate) => Promise<void>;
  refreshSchema: () => Promise<void>;
  discardRefreshedSchema: () => void;
}

const getConnectionCatalog = (connection: WebBackendConnectionRead): ConnectionCatalog =>
  pick(connection, ["syncCatalog", "catalogId"]);

const useConnectionEdit = ({ connectionId }: ConnectionEditProps): ConnectionEditHook => {
  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const getConnectionQuery = useGetConnectionQuery();
  const [connection, setConnection] = useState(useGetConnection(connectionId));
  const [catalog, setCatalog] = useState<ConnectionCatalog>(() => getConnectionCatalog(connection));
  const [schemaHasBeenRefreshed, setSchemaHasBeenRefreshed] = useState(false);

  const [{ loading: schemaRefreshing, error: schemaError }, refreshSchema] = useAsyncFn(async () => {
    unregisterNotificationById("connection.noDiff");

    const refreshedConnection = await getConnectionQuery({ connectionId, withRefreshedCatalog: true });
    if (refreshedConnection.catalogDiff && refreshedConnection.catalogDiff.transforms?.length > 0) {
      setConnection(refreshedConnection);
      setSchemaHasBeenRefreshed(true);
    } else {
      setConnection((connection) => ({
        ...connection,
        schemaChange: refreshedConnection.schemaChange,
      }));

      registerNotification({
        id: "connection.noDiff",
        text: formatMessage({ id: "connection.updateSchema.noDiff" }),
      });
    }
  });

  const discardRefreshedSchema = useCallback(() => {
    setConnection((connection) => ({
      ...connection,
      ...catalog,
      catalogDiff: undefined,
    }));
    setSchemaHasBeenRefreshed(false);
  }, [catalog]);

  const { mutateAsync: updateConnectionAction, isLoading: connectionUpdating } = useUpdateConnection();

  const updateConnection = useCallback(
    async (connectionUpdates: WebBackendConnectionUpdate) => {
      const updatedConnection = await updateConnectionAction(connectionUpdates);
      const updatedKeys = Object.keys(connectionUpdates).map((key) => (key === "sourceCatalogId" ? "catalogId" : key));
      const connectionPatch = pick(updatedConnection, updatedKeys);
      const wasSyncCatalogUpdated = !!connectionPatch.syncCatalog;

      // Ensure that the catalog diff cleared and that the schemaChange status has been updated
      const syncCatalogUpdates: Partial<WebBackendConnectionRead> | undefined = wasSyncCatalogUpdated
        ? {
            catalogDiff: undefined,
            schemaChange: updatedConnection.schemaChange,
          }
        : undefined;

      // Mutate the current connection state only with the values that were updated
      setConnection((connection) => ({
        ...connection,
        ...connectionPatch,
        ...syncCatalogUpdates,
      }));

      if (wasSyncCatalogUpdated) {
        // The catalog ws also saved, so update the current catalog
        setCatalog(getConnectionCatalog(updatedConnection));
        setSchemaHasBeenRefreshed(false);
      }
    },
    [updateConnectionAction]
  );

  return {
    connection,
    // only use `setConnection` directly if you have a good reason: it is handled for you
    // by `updateConnection`, `refreshSchema`, et al.
    setConnection,
    connectionUpdating,
    schemaError,
    schemaRefreshing,
    schemaHasBeenRefreshed,
    updateConnection,
    refreshSchema,
    discardRefreshedSchema,
  };
};
const ConnectionEditContext = createContext<Omit<ConnectionEditHook, "refreshSchema" | "schemaError"> | null>(null);

export const ConnectionEditServiceProvider: React.FC<React.PropsWithChildren<ConnectionEditProps>> = ({
  children,
  ...props
}) => {
  const { refreshSchema, schemaError, ...data } = useConnectionEdit(props);
  return (
    <ConnectionEditContext.Provider value={data}>
      <ConnectionFormServiceProvider
        mode={data.connection.status === ConnectionStatus.deprecated ? "readonly" : "edit"}
        connection={data.connection}
        schemaError={schemaError as SchemaError}
        refreshSchema={refreshSchema}
      >
        {children}
      </ConnectionFormServiceProvider>
    </ConnectionEditContext.Provider>
  );
};

export const useConnectionEditService = () => {
  const context = useContext(ConnectionEditContext);
  if (context === null) {
    throw new Error("useConnectionEditService must be used within a ConnectionEditServiceProvider");
  }
  return context;
};
