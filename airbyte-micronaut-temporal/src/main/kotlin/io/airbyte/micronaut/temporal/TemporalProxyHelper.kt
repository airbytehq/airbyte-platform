/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.BeanRegistration
import io.temporal.activity.ActivityOptions
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowMethod
import jakarta.inject.Singleton
import net.bytebuddy.ByteBuddy
import net.bytebuddy.TypeCache
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatchers
import org.springframework.core.MethodIntrospector
import org.springframework.core.annotation.AnnotationUtils
import org.springframework.util.ReflectionUtils
import java.lang.reflect.Method
import java.util.Optional
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

/**
 * Generates proxy classes which can be registered with Temporal. These proxies delegate all methods
 * to the provided bean/singleton to allow for the dependency injection framework to manage the
 * lifecycle of a Temporal workflow implementation. This approach is inspired by
 * https://github.com/applicaai/spring-boot-starter-temporal.
 */
@Singleton
class TemporalProxyHelper(
  /**
   * Collection of available [ActivityOptions] beans which will be used to initialize Temporal
   * activity stubs in each registered Temporal workflow.
   */
  private val availableActivityOptions: Collection<BeanRegistration<ActivityOptions>>,
) {
  /**
   * Cache of already generated proxies to reduce the cost of creating and loading the proxies.
   */
  private val workflowProxyCache = TypeCache<Class<*>>()

  private var activityStubGenerator = Optional.empty<TemporalActivityStubGeneratorFunction<Class<*>, ActivityOptions, Any>>()

  /**
   * Creates a proxy class for the given workflow class implementation and instance.
   *
   * @param workflowImplClass The workflow implementation class to proxy. proxy.
   * @param <T> The type of the workflow implementation class.
   * @return A proxied workflow implementation class that can be registered with Temporal.
   </T> */
  fun <T : Any> proxyWorkflowClass(workflowImplClass: Class<T>): Class<T> {
    log.debug { "Creating a Temporal proxy for worker class '${workflowImplClass.name}' with interface '${workflowImplClass.interfaces[0]}'..." }
    return workflowProxyCache.findOrInsert(
      workflowImplClass.classLoader,
      workflowImplClass,
    ) {
      val workflowMethods = findAnnotatedMethods(workflowImplClass, WorkflowMethod::class.java)
      val signalMethods = findAnnotatedMethods(workflowImplClass, SignalMethod::class.java)
      val queryMethods = findAnnotatedMethods(workflowImplClass, QueryMethod::class.java)

      val proxiedMethods: MutableSet<Method> = HashSet()
      proxiedMethods.add(workflowMethods.toTypedArray()[0])
      proxiedMethods.addAll(
        signalMethods
          .stream()
          .collect(Collectors.toList()),
      )
      proxiedMethods.addAll(
        queryMethods
          .stream()
          .collect(Collectors.toList()),
      )

      val type =
        ByteBuddy()
          .subclass(workflowImplClass)
          .name(workflowImplClass.simpleName + "Proxy")
          .implement(workflowImplClass.interfaces[0])
          .method(ElementMatchers.anyOf(*proxiedMethods.toTypedArray()))
          .intercept(MethodDelegation.to(generateInterceptor(workflowImplClass, availableActivityOptions)))
          .make()
          .load(workflowImplClass.classLoader, ClassLoadingStrategy.Default.WRAPPER)
          .loaded as Class<T>

      log.debug {
        "Temporal workflow proxy '${type.name}' created for worker class '${workflowImplClass.name}' with interface '${workflowImplClass.interfaces[0]}'."
      }
      type
    } as Class<T>
  }

  /**
   * Finds the methods annotated with the provided annotation type in the given class.
   *
   * @param workflowImplClass The workflow implementation class.
   * @param annotationClass The annotation.
   * @param <A> The type of the annotation.
   * @return The set of methods annotated with the provided annotation.
   </A> */
  private fun <A : Annotation?> findAnnotatedMethods(
    workflowImplClass: Class<*>,
    annotationClass: Class<A>,
  ): Set<Method> =
    MethodIntrospector.selectMethods(
      workflowImplClass,
      ReflectionUtils.MethodFilter { method: Method? -> AnnotationUtils.findAnnotation(method, annotationClass) != null },
    )

  /**
   * Generates a [TemporalActivityStubInterceptor] instance for use with the generated proxy
   * workflow implementation.
   *
   * @param workflowImplClass The workflow implementation class.
   * @param activityOptions The collection of available [ActivityOptions] beans which will be
   * used to initialize Temporal activity stubs in each registered Temporal workflow.
   * @param <T> The workflow implementation type.
   * @return The generated [TemporalActivityStubInterceptor] instance.
   </T> */
  private fun <T : Any> generateInterceptor(
    workflowImplClass: Class<T>,
    activityOptions: Collection<BeanRegistration<ActivityOptions>>,
  ): TemporalActivityStubInterceptor<T> {
    val interceptor: TemporalActivityStubInterceptor<T> = TemporalActivityStubInterceptor(workflowImplClass, activityOptions)
    activityStubGenerator.ifPresent { activityStubGenerator: TemporalActivityStubGeneratorFunction<Class<*>, ActivityOptions, Any>? ->
      interceptor.setActivityStubGenerator(
        activityStubGenerator!!,
      )
    }
    return interceptor
  }

  @VisibleForTesting
  fun setActivityStubGenerator(activityStubGenerator: TemporalActivityStubGeneratorFunction<Class<*>, ActivityOptions, Any>) {
    this.activityStubGenerator = Optional.ofNullable(activityStubGenerator)
  }
}
