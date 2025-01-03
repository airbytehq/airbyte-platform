package io.airbyte.commons.server.helpers

import io.fabric8.kubernetes.api.model.Node
import io.fabric8.kubernetes.api.model.NodeList
import io.fabric8.kubernetes.api.model.authorization.v1.SelfSubjectAccessReview
import io.fabric8.kubernetes.api.model.authorization.v1.SelfSubjectAccessReviewSpec
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation
import io.fabric8.kubernetes.client.dsl.Resource
import jakarta.inject.Inject
import jakarta.inject.Singleton

class PermissionDeniedException(message: String) : RuntimeException(message)

@Singleton
class KubernetesClientPermissionHelper
  @Inject
  constructor(
    private val kubernetesClient: KubernetesClient,
  ) {
    fun listNodes(): NonNamespaceOperation<Node, NodeList, Resource<Node>>? {
      if (!allowedToListNodes()) {
        throw PermissionDeniedException("Permission denied: unable to list Kubernetes nodes.")
      }
      return kubernetesClient.nodes()
    }

    private fun allowedToListNodes(): Boolean {
      val review = SelfSubjectAccessReview()
      review.spec =
        SelfSubjectAccessReviewSpec().apply {
          resourceAttributes =
            io.fabric8.kubernetes.api.model.authorization.v1.ResourceAttributes().apply {
              verb = "list"
              resource = "nodes"
            }
        }

      val response = kubernetesClient.authorization().v1().selfSubjectAccessReview().create(review)
      return response.status?.allowed ?: false
    }
  }
