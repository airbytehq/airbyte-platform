/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.Permission
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class PermissionHelperTest {
  @Test
  fun allPermissionTypesHaveDefinedRules() {
    // If this assertion fails, it means a new PermissionType was added without defining the rules for
    // which permissions it grants access to. Add the new permission type to the map in
    // PermissionHelper (with appropriate values of course) in order to make this test pass again.
    Assertions.assertEquals(
      PermissionHelper.GRANTED_PERMISSION_TYPES_BY_DEFINED_PERMISSION_TYPE.keys,
      Permission.PermissionType.entries.toSet(),
    )
  }

  @Test
  fun actorScopedWorkspaceEditorsGrantRunnerAndReader() {
    for (actorScopedEditor in listOf(
      Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
    )) {
      Assertions.assertEquals(
        setOf(
          actorScopedEditor,
          Permission.PermissionType.WORKSPACE_RUNNER,
          Permission.PermissionType.WORKSPACE_READER,
        ),
        PermissionHelper.getGrantedPermissions(actorScopedEditor),
      )
    }
  }

  @Test
  fun neitherActorScopedWorkspaceEditorGrantsTheOther() {
    Assertions.assertFalse(
      PermissionHelper.definedPermissionGrantsTargetPermission(
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
        Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
      ),
    )
    Assertions.assertFalse(
      PermissionHelper.definedPermissionGrantsTargetPermission(
        Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      ),
    )
  }

  @Test
  fun actorScopedWorkspaceEditorsDoNotGrantWorkspaceEditor() {
    Assertions.assertFalse(
      PermissionHelper.definedPermissionGrantsTargetPermission(
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
        Permission.PermissionType.WORKSPACE_EDITOR,
      ),
    )
    Assertions.assertFalse(
      PermissionHelper.definedPermissionGrantsTargetPermission(
        Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
        Permission.PermissionType.WORKSPACE_EDITOR,
      ),
    )
  }

  @Test
  fun editorsAndAdminsGrantBothActorScopedWorkspaceEditors() {
    val expectedToGrantBoth =
      setOf(
        Permission.PermissionType.INSTANCE_ADMIN,
        Permission.PermissionType.ORGANIZATION_ADMIN,
        Permission.PermissionType.ORGANIZATION_EDITOR,
        Permission.PermissionType.WORKSPACE_OWNER,
        Permission.PermissionType.WORKSPACE_ADMIN,
        Permission.PermissionType.WORKSPACE_EDITOR,
      )

    for (actorScopedEditor in listOf(
      Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
    )) {
      Assertions.assertEquals(
        expectedToGrantBoth + actorScopedEditor,
        PermissionHelper.getPermissionTypesThatGrantTargetPermission(actorScopedEditor),
      )
    }
  }

  @Test
  fun runnersAndReadersDoNotGrantActorScopedWorkspaceEditors() {
    for (lesserRole in listOf(
      Permission.PermissionType.ORGANIZATION_RUNNER,
      Permission.PermissionType.ORGANIZATION_READER,
      Permission.PermissionType.ORGANIZATION_MEMBER,
      Permission.PermissionType.WORKSPACE_RUNNER,
      Permission.PermissionType.WORKSPACE_READER,
    )) {
      for (actorScopedEditor in listOf(
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
        Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
      )) {
        Assertions.assertFalse(
          PermissionHelper.definedPermissionGrantsTargetPermission(lesserRole, actorScopedEditor),
          "$lesserRole must not grant $actorScopedEditor",
        )
      }
    }
  }
}
