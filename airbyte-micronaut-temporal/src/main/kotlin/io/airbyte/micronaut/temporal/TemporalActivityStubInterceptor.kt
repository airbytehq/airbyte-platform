/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.BeanRegistration
import io.temporal.activity.ActivityOptions
import io.temporal.workflow.Workflow
import net.bytebuddy.implementation.bind.annotation.RuntimeType
import net.bytebuddy.implementation.bind.annotation.SuperCall
import net.bytebuddy.implementation.bind.annotation.This
import org.springframework.util.ReflectionUtils
import java.lang.reflect.Field
import java.util.concurrent.Callable

private val log = KotlinLogging.logger {}

/**
 * Custom interceptor that handles invocations of Temporal workflow implementations to ensure that
 * any and all Temporal activity stubs are created prior to the first execution of the workflow.
 * This class is used in conjunction with [TemporalProxyHelper]. This approach is inspired by
 * https://github.com/applicaai/spring-boot-starter-temporal.
 *
 * @param <T> The type of the Temporal workflow.
</T> */
class TemporalActivityStubInterceptor<T : Any>(
  /**
   * The type of the workflow implementation to be proxied.
   */
  private val workflowImplClass: Class<T>,
  /**
   * The collection of configured [ActivityOptions] beans provided by the application framework.
   */
  private val availableActivityOptions: Collection<BeanRegistration<ActivityOptions>>,
) {
  /**
   * Function that generates Temporal activity stubs.
   *
   * Replace this value for unit testing.
   */
  private var activityStubGenerator =
    TemporalActivityStubGeneratorFunction { activityInterface: Class<*>?, options: ActivityOptions? ->
      Workflow.newActivityStub(
        activityInterface,
        options,
      )
    }

  /**
   * Main interceptor method that will be invoked by the proxy.
   *
   * @param workflowImplInstance The actual workflow implementation object invoked on the proxy
   * Temporal workflow instance.
   * @param call A [Callable] used to invoke the proxied method.
   * @return The result of the proxied method execution.
   * @throws Exception if the proxied method throws a checked exception
   * @throws IllegalStateException if the Temporal activity stubs associated with the workflow cannot
   * be initialized.
   */
  @RuntimeType
  @Throws(Exception::class)
  fun execute(
    @This workflowImplInstance: T,
    @SuperCall call: Callable<Any?>,
  ): Any? {
    // Initialize the activity stubs, if not already done, before execution of the workflow method
    initializeActivityStubs(workflowImplClass, workflowImplInstance)
    return call.call()
  }

  /**
   * Initializes all Temporal activity stubs present on the provided workflow instance. A Temporal
   * activity stub is denoted by the use of the [TemporalActivityStub] annotation on the field.
   *
   * @param workflowImplClass The target class of the proxy.
   * @param workflowInstance The workflow instance that may contain Temporal activity stub fields.
   */
  private fun initializeActivityStubs(
    workflowImplClass: Class<T>,
    workflowInstance: T,
  ) {
    for (field in workflowImplClass.declaredFields) {
      if (field.isAnnotationPresent(TemporalActivityStub::class.java)) {
        initializeActivityStub(workflowInstance, field)
      }
    }
  }

  /**
   * Initializes the Temporal activity stub represented by the provided field on the provided object,
   * if not already set.
   *
   * @param workflowInstance The Temporal workflow instance that contains the Temporal activity stub
   * field.
   * @param activityStubField The field that represents the Temporal activity stub.
   */
  private fun initializeActivityStub(
    workflowInstance: T,
    activityStubField: Field,
  ) {
    try {
      log.debug {
        "Attempting to initialize Temporal activity stub for activity '${activityStubField.type}' on workflow '${workflowInstance.javaClass.name}'..."
      }
      ReflectionUtils.makeAccessible(activityStubField)
      if (activityStubField[workflowInstance] == null) {
        val activityOptions = getActivityOptions(activityStubField)
        val activityStub = generateActivityStub(activityStubField, activityOptions)
        activityStubField[workflowInstance] = activityStub
        log.debug { "Initialized Temporal activity stub for activity '${activityStubField.type}' for workflow '${workflowInstance.javaClass.name}'." }
      } else {
        log.debug {
          "Temporal activity stub '${activityStubField.type}' is already initialized for Temporal workflow '${workflowInstance.javaClass.name}'."
        }
      }
    } catch (e: IllegalArgumentException) {
      log.error(e) {
        "Unable to initialize Temporal activity stub for activity '${activityStubField.type}' for workflow '${workflowInstance.javaClass.name}'."
      }
      throw RuntimeException(e)
    } catch (e: IllegalAccessException) {
      log.error(e) {
        "Unable to initialize Temporal activity stub for activity '${activityStubField.type}' for workflow '${workflowInstance.javaClass.name}'."
      }
      throw RuntimeException(e)
    } catch (e: IllegalStateException) {
      log.error(e) {
        "Unable to initialize Temporal activity stub for activity '${activityStubField.type}' for workflow '${workflowInstance.javaClass.name}'."
      }
      throw RuntimeException(e)
    }
  }

  /**
   * Extracts the Temporal [ActivityOptions] from the [Field] on the provided target
   * instance object.
   *
   * @param activityStubField The field that represents the Temporal activity stub.
   * @return The Temporal [ActivityOptions] from the [Field] on the provided Temporal
   * workflow instance object.
   * @throws IllegalStateException if the referenced Temporal [ActivityOptions] bean cannot be
   * located.
   */
  private fun getActivityOptions(activityStubField: Field): ActivityOptions {
    val annotation = activityStubField.getAnnotation(TemporalActivityStub::class.java)
    val activityOptionsBeanName = annotation.activityOptionsBeanName
    val selectedActivityOptions =
      availableActivityOptions
        .stream()
        .filter { b: BeanRegistration<ActivityOptions> -> b.identifier.name.equals(activityOptionsBeanName, ignoreCase = true) }
        .map { obj: BeanRegistration<ActivityOptions> -> obj.bean }
        .findFirst()
    if (selectedActivityOptions.isPresent) {
      return selectedActivityOptions.get()
    } else {
      throw IllegalStateException("No activity options bean of name '$activityOptionsBeanName' exists.")
    }
  }

  /**
   * Retrieve the activity stub generator function associated with the Temporal activity stub.
   *
   * @param activityStubField The field that represents the Temporal activity stub.
   * @return The [TemporalActivityStubGeneratorFunction] associated with the Temporal activity
   * stub.
   * @throws IllegalStateException if the referenced [TemporalActivityStubGeneratorFunction]
   * bean cannot be located.
   */
  private fun generateActivityStub(
    activityStubField: Field,
    activityOptions: ActivityOptions,
  ): Any = activityStubGenerator.apply(activityStubField.type, activityOptions)

  @VisibleForTesting
  fun setActivityStubGenerator(activityStubGenerator: TemporalActivityStubGeneratorFunction<Class<*>, ActivityOptions, Any>) {
    this.activityStubGenerator = activityStubGenerator
  }
}
