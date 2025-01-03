package io.airbyte.commons.server.helpers

import io.fabric8.kubernetes.api.model.Node
import io.fabric8.kubernetes.api.model.NodeList
import io.fabric8.kubernetes.api.model.authorization.v1.SelfSubjectAccessReview
import io.fabric8.kubernetes.api.model.authorization.v1.SubjectAccessReviewStatus
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation
import io.fabric8.kubernetes.client.dsl.Resource
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class KubernetesClientPermissionHelperTest {
  private lateinit var kubernetesClient: KubernetesClient
  private lateinit var kubernetesClientPermissionHelper: KubernetesClientPermissionHelper

  @BeforeEach
  fun setUp() {
    kubernetesClient = mockk()
    kubernetesClientPermissionHelper = KubernetesClientPermissionHelper(kubernetesClient)
  }

  @Test
  fun `getNodes() returns nodes when permission is granted`() {
    val mockReviewResponse = mockk<SelfSubjectAccessReview>()

    every { mockReviewResponse.status } returns SubjectAccessReviewStatus().apply { allowed = true }
    every {
      kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any())
    } returns mockReviewResponse

    val mockNodesOperation = mockk<NonNamespaceOperation<Node, NodeList, Resource<Node>>>()
    every { kubernetesClient.nodes() } returns mockNodesOperation

    val nodes = kubernetesClientPermissionHelper.listNodes()

    assertNotNull(nodes)
    assertEquals(mockNodesOperation, nodes)

    verify(exactly = 1) { kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any()) }
    verify(exactly = 1) { kubernetesClient.nodes() }
  }

  @Test
  fun `getNodes() throws PermissionDeniedException when permission is denied`() {
    val mockReviewResponse = mockk<SelfSubjectAccessReview>()
    every { mockReviewResponse.status } returns SubjectAccessReviewStatus().apply { allowed = false }

    every {
      kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any())
    } returns mockReviewResponse

    val exception =
      assertThrows<PermissionDeniedException> {
        kubernetesClientPermissionHelper.listNodes()
      }

    assertEquals("Permission denied: unable to list Kubernetes nodes.", exception.message)

    verify(exactly = 1) { kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any()) }
    verify(exactly = 0) { kubernetesClient.nodes() }
  }

  @Test
  fun `getNodes() throws PermissionDeniedException when permission check fails with null status`() {
    val mockReviewResponse = mockk<SelfSubjectAccessReview>()

    every { mockReviewResponse.status } returns null
    every {
      kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any())
    } returns mockReviewResponse

    val exception =
      assertThrows<PermissionDeniedException> {
        kubernetesClientPermissionHelper.listNodes()
      }

    assertEquals("Permission denied: unable to list Kubernetes nodes.", exception.message)

    verify(exactly = 1) { kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any()) }
    verify(exactly = 0) { kubernetesClient.nodes() }
  }

  @Test
  fun `allowedToListNodes() returns true when permission is granted`() {
    val mockReviewResponse = mockk<SelfSubjectAccessReview>()

    every { mockReviewResponse.status } returns SubjectAccessReviewStatus().apply { allowed = true }
    every {
      kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any())
    } returns mockReviewResponse

    val allowedToListNodes = kubernetesClientPermissionHelper.javaClass.getDeclaredMethod("allowedToListNodes")
    allowedToListNodes.isAccessible = true // Access private method
    val result = allowedToListNodes.invoke(kubernetesClientPermissionHelper) as Boolean

    assertTrue(result)

    verify(exactly = 1) { kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any()) }
  }

  @Test
  fun `allowedToListNodes() returns false when permission is denied`() {
    val mockReviewResponse = mockk<SelfSubjectAccessReview>()

    every { mockReviewResponse.status } returns SubjectAccessReviewStatus().apply { allowed = false }
    every {
      kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any())
    } returns mockReviewResponse

    val allowedToListNodes = kubernetesClientPermissionHelper.javaClass.getDeclaredMethod("allowedToListNodes")
    allowedToListNodes.isAccessible = true // Access private method
    val result = allowedToListNodes.invoke(kubernetesClientPermissionHelper) as Boolean

    assertFalse(result)

    verify(exactly = 1) { kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any()) }
  }

  @Test
  fun `allowedToListNodes() returns false when permission status is null`() {
    val mockReviewResponse = mockk<SelfSubjectAccessReview>()

    every { mockReviewResponse.status } returns null
    every {
      kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any())
    } returns mockReviewResponse

    val allowedToListNodes = kubernetesClientPermissionHelper.javaClass.getDeclaredMethod("allowedToListNodes")
    allowedToListNodes.isAccessible = true // Access private method
    val result = allowedToListNodes.invoke(kubernetesClientPermissionHelper) as Boolean

    assertFalse(result)

    verify(exactly = 1) { kubernetesClient.authorization().v1().selfSubjectAccessReview().create(any()) }
  }
}
