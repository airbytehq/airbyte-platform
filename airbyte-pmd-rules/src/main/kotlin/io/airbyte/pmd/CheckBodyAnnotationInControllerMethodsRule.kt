/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.pmd

import net.sourceforge.pmd.lang.java.ast.ASTAnnotation
import net.sourceforge.pmd.lang.java.ast.ASTClassDeclaration
import net.sourceforge.pmd.lang.java.ast.ASTFormalParameter
import net.sourceforge.pmd.lang.java.ast.ASTMethodDeclaration
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule

class CheckBodyAnnotationInControllerMethodsRule : AbstractJavaRule() {
  override fun visit(
    node: ASTClassDeclaration,
    data: Any,
  ): Any {
    // Check if the class is in the controller package
    if (node.declaredAnnotations.find { it.simpleName == "Controller" } != null) {
      return super.visit(node, data)
    }
    return data
  }

  override fun visit(
    node: ASTMethodDeclaration,
    data: Any,
  ): Any {
    if (node.declaredAnnotations.any { it.simpleName == "Post" }) {
      val params = node.descendants(ASTFormalParameter::class.java).toList()
      val hasViolation = params.isNotEmpty() && params.none { it.descendants(ASTAnnotation::class.java).any { it.simpleName == "Body" } }

      if (hasViolation) {
        asCtx(data).addViolation(node, node.name)
      }
    }
    return data
  }
}
