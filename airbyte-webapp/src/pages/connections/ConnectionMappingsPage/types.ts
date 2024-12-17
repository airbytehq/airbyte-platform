import { MapperConfiguration, ConfiguredStreamMapper } from "core/api/types/AirbyteClient";

export interface StreamMapperWithId<T extends MapperConfiguration = MapperConfiguration>
  extends ConfiguredStreamMapper {
  id: string;
  mapperConfiguration: T;
  validationCallback: () => Promise<boolean>;
}
