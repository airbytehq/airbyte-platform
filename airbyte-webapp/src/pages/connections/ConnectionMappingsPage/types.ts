import { MapperConfiguration, ConfiguredStreamMapper, MapperValidationError } from "core/api/types/AirbyteClient";

export interface StreamMapperWithId<T extends MapperConfiguration = MapperConfiguration>
  extends ConfiguredStreamMapper {
  id: string;
  mapperConfiguration: T;
  validationError?: MapperValidationError;
  validationCallback: () => Promise<boolean>;
}
