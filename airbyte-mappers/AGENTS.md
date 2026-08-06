# AGENTS.md — `oss/airbyte-mappers/`

Conventions and implementation guidance for in-flight record transformations
and destination catalog mapping. Read the root [AGENTS.md](../../AGENTS.md)
and [`oss/AGENTS.md`](../AGENTS.md) first.

## Module scope

- Mapper configuration/specification and JSON-schema validation.
- Record transformations for hashing, encryption, field filtering, field
  renaming, and row filtering.
- Destination catalog generation and runtime record mapping.

Keep catalog transformations and record transformations consistent: schema
changes must match the fields emitted or removed from records. Preserve
mapper ordering and the existing `Mapper` / `MapperSpec` abstractions when
adding a transformation.

## Key implementation paths

- `src/main/kotlin/io/airbyte/mappers/transformations/Mapper.kt`
- `src/main/kotlin/io/airbyte/mappers/transformations/HashingMapper.kt`
- `src/main/kotlin/io/airbyte/mappers/transformations/DestinationCatalogGenerator.kt`
- `src/main/kotlin/io/airbyte/mappers/application/RecordMapper.kt`

Further reading: [field hashing and mappers](field-hashing.md).
