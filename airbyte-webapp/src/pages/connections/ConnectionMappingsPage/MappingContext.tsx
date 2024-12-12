import React, { PropsWithChildren, createContext, useContext, useState } from "react";
import { v4 as uuidv4 } from "uuid";

import { useCurrentConnection } from "core/api";
import { HashingMapperConfigurationMethod, StreamMapperType } from "core/api/types/AirbyteClient";

import { StreamMapperWithId } from "./types";
import { useGetMappingsForCurrentConnection } from "./useGetMappingsForCurrentConnection";
import { useUpdateMappingsForCurrentConnection } from "./useUpdateMappingsForCurrentConnection";

interface MappingContextType {
  streamsWithMappings: Record<string, StreamMapperWithId[]>;
  updateLocalMapping: (streamName: string, updatedMapping: StreamMapperWithId) => void;
  reorderMappings: (streamName: string, newOrder: StreamMapperWithId[]) => void;
  clear: () => void;
  submitMappings: () => Promise<void>;
  removeMapping: (streamName: string, mappingId: string) => void;
  addStreamToMappingsList: (streamName: string) => void;
  addMappingForStream: (streamName: string) => void;
  validateMappings: () => void;
}

const MappingContext = createContext<MappingContextType | undefined>(undefined);

export const MappingContextProvider: React.FC<PropsWithChildren> = ({ children }) => {
  const connection = useCurrentConnection();
  const savedStreamsWithMappings = useGetMappingsForCurrentConnection();
  const { updateMappings } = useUpdateMappingsForCurrentConnection(connection.connectionId);
  const [streamsWithMappings, setStreamsWithMappings] = useState(savedStreamsWithMappings);

  const validateMappings = () => {
    console.log(`validateMappings`, streamsWithMappings);
    // TOOD: actually validate mappings via the API :-)
  };

  // Updates a specific mapping in the local state
  const updateLocalMapping = (streamName: string, updatedMapping: StreamMapperWithId) => {
    console.log(`updating local mapping for stream ${streamName}`, updatedMapping);
    setStreamsWithMappings((prevMappings) => ({
      ...prevMappings,
      [streamName]: prevMappings[streamName].map((mapping) =>
        mapping.id === updatedMapping.id ? updatedMapping : mapping
      ),
    }));
  };

  const addMappingForStream = (streamName: string) => {
    setStreamsWithMappings((prevMappings) => ({
      ...prevMappings,
      [streamName]: [
        ...prevMappings[streamName],
        {
          type: StreamMapperType.hashing,
          id: uuidv4(),
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
  const reorderMappings = (streamName: string, newOrder: StreamMapperWithId[]) => {
    setStreamsWithMappings((prevMappings) => ({
      ...prevMappings,
      [streamName]: newOrder,
    }));
  };

  // Clears the mappings back to the saved state
  const clear = () => {
    setStreamsWithMappings(savedStreamsWithMappings);
  };

  const removeMapping = (streamName: string, mappingId: string) => {
    const mappingsForStream = streamsWithMappings[streamName].filter((mapping) => mapping.id !== mappingId);

    setStreamsWithMappings((prevMappings) => {
      if (mappingsForStream.length === 0) {
        const { [streamName]: removedStream, ...rest } = prevMappings;
        return rest;
      }
      return {
        ...prevMappings,
        [streamName]: mappingsForStream,
      };
    });
  };

  // Submits the current mappings state to the backend
  const submitMappings = async () => {
    await updateMappings(streamsWithMappings);
    return Promise.resolve();
  };

  const addStreamToMappingsList = (streamName: string) => {
    const newMapping: Record<string, StreamMapperWithId[]> = {
      [streamName]: [
        {
          type: StreamMapperType.hashing,
          id: uuidv4(),
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
