import { ConfiguredStreamMapper, HashingMapperConfiguration } from "core/api/types/AirbyteClient";

export const isHashingMapper = (
  mapper: ConfiguredStreamMapper
): mapper is { type: "hashing"; mapperConfiguration: HashingMapperConfiguration } => {
  return mapper.type === "hashing";
};
