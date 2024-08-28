package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.node.ObjectNode

// TODO: Remove when we have the real abstraction around the data in an airbyte record message
data class Record(val data: ObjectNode)
