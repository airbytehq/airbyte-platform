import { useParams } from "react-router-dom";

export const useGetActorIdFromParams = () => {
  const { sourceId, destinationId } = useParams();

  if (sourceId) {
    return sourceId;
  }

  if (destinationId) {
    return destinationId;
  }

  return undefined;
};
