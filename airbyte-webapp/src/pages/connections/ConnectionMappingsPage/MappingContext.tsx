import merge from "lodash/merge";
import React, { PropsWithChildren, createContext, useCallback, useContext, useState } from "react";
import { v4 as uuidv4 } from "uuid";

import { useCurrentConnection } from "core/api";
import {
  AirbyteStream,
  HashingMapperConfigurationMethod,
  StreamDescriptor,
  StreamMapperType,
} from "core/api/types/AirbyteClient";
import { useNotificationService } from "hooks/services/Notification";

import { StreamMapperWithId } from "./types";
import { useGetMappingsForCurrentConnection } from "./useGetMappingsForCurrentConnection";
import { useUpdateMappingsForCurrentConnection } from "./useUpdateMappingsForCurrentConnection";

interface MappingContextType {
  streamsWithMappings: Record<string, StreamMapperWithId[]>;
  updateLocalMapping: (
    streamDescriptorKey: string,
    mappingId: string,
    updatedMapping: Partial<StreamMapperWithId>
  ) => void;
  reorderMappings: (streamDescriptorKey: string, newOrder: StreamMapperWithId[]) => void;
  clear: () => void;
  submitMappings: () => Promise<void>;
  removeMapping: (streamDescriptorKey: string, mappingId: string) => void;
  addStreamToMappingsList: (streamDescriptorKey: string) => void;
  addMappingForStream: (streamDescriptorKey: string) => void;
  validateMappings: () => void;
  key: number;
}

export const MAPPING_VALIDATION_ERROR_KEY = "mapping-validation-error";

const MappingContext = createContext<MappingContextType | undefined>(undefined);
export const getKeyForStream = (stream: AirbyteStream) => `${stream.namespace}-${stream.name}`;
export const getStreamDescriptorForKey = (key: string): StreamDescriptor => {
  const [namespace, name] = key.split("-");

  if (namespace === "undefined") {
    return { namespace: undefined, name };
  }
  return { namespace, name };
};

export const MappingContextProvider: React.FC<PropsWithChildren> = ({ children }) => {
  const connection = useCurrentConnection();
  const savedStreamsWithMappings = useGetMappingsForCurrentConnection();
  const { updateMappings } = useUpdateMappingsForCurrentConnection(connection.connectionId);
  const [streamsWithMappings, setStreamsWithMappings] = useState(savedStreamsWithMappings);
  // Key is used to force mapping forms to re-render if a user chooses to reset the form state
  const [key, setKey] = useState(1);
  const { unregisterNotificationById } = useNotificationService();

  const validateMappings = () => {
    console.log(`validateMappings`, streamsWithMappings);
    // TOOD: actually validate mappings via the API :-)
  };

  // Updates a specific mapping in the local state
  const updateLocalMapping = useCallback(
    (streamDescriptorKey: string, mappingId: string, updatedMapping: Partial<StreamMapperWithId>) => {
      console.log(`updating local mapping for stream ${streamDescriptorKey}`, updatedMapping);

      setStreamsWithMappings((prevMappings) => ({
        ...prevMappings,
        [streamDescriptorKey]: prevMappings[streamDescriptorKey].map((mapping) => {
          if (mapping.id === mappingId) {
            if (updatedMapping.type && updatedMapping.type !== mapping.type) {
              return updatedMapping as StreamMapperWithId;
            }
            const merged = merge({}, mapping, updatedMapping);
            return merged;
          }
          return mapping;
        }),
      }));
    },
    []
  );

  const addMappingForStream = (streamDescriptorKey: string) => {
    setStreamsWithMappings((prevMappings) => ({
      ...prevMappings,
      [streamDescriptorKey]: [
        ...prevMappings[streamDescriptorKey],
        {
          type: StreamMapperType.hashing,
          id: uuidv4(),
          validationCallback: () => Promise.reject(false),
          mapperConfiguration: {
            fieldNameSuffix: "_hashed",
            method: HashingMapperConfigurationMethod["SHA-256"],
            targetField: "",
          },
        },
      ],
    }));
  };

  // Reorders the mappings for a specific stream
  const reorderMappings = (streamDescriptorKey: string, newOrder: StreamMapperWithId[]) => {
    setStreamsWithMappings((prevMappings) => ({
      ...prevMappings,
      [streamDescriptorKey]: newOrder,
    }));
  };

  // Clears the mappings back to the saved state
  const clear = () => {
    setKey((prevKey) => prevKey + 1);
    setStreamsWithMappings(savedStreamsWithMappings);
    unregisterNotificationById(MAPPING_VALIDATION_ERROR_KEY);
  };

  const removeMapping = (streamDescriptorKey: string, mappingId: string) => {
    const mappingsForStream = streamsWithMappings[streamDescriptorKey].filter((mapping) => mapping.id !== mappingId);

    setStreamsWithMappings((prevMappings) => {
      return {
        ...prevMappings,
        [streamDescriptorKey]: mappingsForStream,
      };
    });
  };

  // Submits the current mappings state to the backend
  const submitMappings = async () => {
    await updateMappings(streamsWithMappings);
    return Promise.resolve();
  };

  const addStreamToMappingsList = (streamDescriptorKey: string) => {
    const newMapping: Record<string, StreamMapperWithId[]> = {
      [streamDescriptorKey]: [
        {
          type: StreamMapperType.hashing,
          id: uuidv4(),
          validationCallback: () => Promise.reject(false),
          mapperConfiguration: {
            fieldNameSuffix: "_hashed",
            method: HashingMapperConfigurationMethod["SHA-256"],
            targetField: "",
          },
        },
      ],
    };
    setStreamsWithMappings((prevMappings) => ({
      ...prevMappings,
      ...newMapping,
    }));
  };

  return (
    <MappingContext.Provider
      value={{
        streamsWithMappings,
        updateLocalMapping,
        reorderMappings,
        clear,
        submitMappings,
        removeMapping,
        addStreamToMappingsList,
        addMappingForStream,
        validateMappings,
        key,
      }}
    >
      {children}
    </MappingContext.Provider>
  );
};

export const useMappingContext = () => {
  const context = useContext(MappingContext);
  if (!context) {
    throw new Error("useMappingContext must be used within a MappingContextProvider");
  }
  return context;
};
