/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal;

import com.google.common.annotations.VisibleForTesting;
import io.micronaut.context.BeanRegistration;
import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowMethod;
import jakarta.inject.Singleton;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.TypeCache;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import org.slf4j.Logger;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Generates proxy classes which can be registered with Temporal. These proxies delegate all methods
 * to the provided bean/singleton to allow for the dependency injection framework to manage the
 * lifecycle of a Temporal workflow implementation. This approach is inspired by
 * https://github.com/applicaai/spring-boot-starter-temporal.
 */
@Singleton
public class TemporalProxyHelper {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(TemporalProxyHelper.class);
  /**
   * Cache of already generated proxies to reduce the cost of creating and loading the proxies.
   */
  @SuppressWarnings({"MemberName", "AbbreviationAsWordInName"})
  private final TypeCache<Class<?>> WORKFLOW_PROXY_CACHE = new TypeCache<>();

  /**
   * Collection of available {@link ActivityOptions} beans which will be used to initialize Temporal
   * activity stubs in each registered Temporal workflow.
   */
  private final Collection<BeanRegistration<ActivityOptions>> availableActivityOptions;

  private Optional<TemporalActivityStubGeneratorFunction<Class<?>, ActivityOptions, Object>> activityStubGenerator = Optional.empty();

  public TemporalProxyHelper(final Collection<BeanRegistration<ActivityOptions>> availableActivityOptions) {
    this.availableActivityOptions = availableActivityOptions;
  }

  /**
   * Creates a proxy class for the given workflow class implementation and instance.
   *
   * @param workflowImplClass The workflow implementation class to proxy. proxy.
   * @param <T> The type of the workflow implementation class.
   * @return A proxied workflow implementation class that can be registered with Temporal.
   */
  @SuppressWarnings("PMD.UnnecessaryCast")
  public <T> Class<T> proxyWorkflowClass(final Class<T> workflowImplClass) {
    log.debug("Creating a Temporal proxy for worker class '{}' with interface '{}'...", workflowImplClass.getName(),
        workflowImplClass.getInterfaces()[0]);
    return (Class<T>) WORKFLOW_PROXY_CACHE.findOrInsert(workflowImplClass.getClassLoader(), workflowImplClass, () -> {
      final Set<Method> workflowMethods = findAnnotatedMethods(workflowImplClass, WorkflowMethod.class);
      final Set<Method> signalMethods = findAnnotatedMethods(workflowImplClass, SignalMethod.class);
      final Set<Method> queryMethods = findAnnotatedMethods(workflowImplClass, QueryMethod.class);

      final Set<Method> proxiedMethods = new HashSet<>();
      proxiedMethods.add((Method) workflowMethods.toArray()[0]);
      proxiedMethods.addAll(signalMethods.stream().collect(Collectors.toList()));
      proxiedMethods.addAll(queryMethods.stream().collect(Collectors.toList()));

      final Class<T> type = (Class<T>) new ByteBuddy()
          .subclass(workflowImplClass)
          .name(workflowImplClass.getSimpleName() + "Proxy")
          .implement(workflowImplClass.getInterfaces()[0])
          .method(ElementMatchers.anyOf(proxiedMethods.toArray(new Method[] {})))
          .intercept(
              MethodDelegation.to(generateInterceptor(workflowImplClass, availableActivityOptions)))
          .make()
          .load(workflowImplClass.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
          .getLoaded();

      log.debug("Temporal workflow proxy '{}' created for worker class '{}' with interface '{}'.", type.getName(), workflowImplClass.getName(),
          workflowImplClass.getInterfaces()[0]);
      return type;
    });
  }

  /**
   * Finds the methods annotated with the provided annotation type in the given class.
   *
   * @param workflowImplClass The workflow implementation class.
   * @param annotationClass The annotation.
   * @param <A> The type of the annotation.
   * @return The set of methods annotated with the provided annotation.
   */
  private <A extends Annotation> Set<Method> findAnnotatedMethods(final Class<?> workflowImplClass, final Class<A> annotationClass) {
    return MethodIntrospector.selectMethods(
        workflowImplClass,
        (ReflectionUtils.MethodFilter) method -> AnnotationUtils.findAnnotation(method, annotationClass) != null);
  }

  /**
   * Generates a {@link TemporalActivityStubInterceptor} instance for use with the generated proxy
   * workflow implementation.
   *
   * @param workflowImplClass The workflow implementation class.
   * @param activityOptions The collection of available {@link ActivityOptions} beans which will be
   *        used to initialize Temporal activity stubs in each registered Temporal workflow.
   * @param <T> The workflow implementation type.
   * @return The generated {@link TemporalActivityStubInterceptor} instance.
   */
  private <T> TemporalActivityStubInterceptor<T> generateInterceptor(final Class<T> workflowImplClass,
                                                                     final Collection<BeanRegistration<ActivityOptions>> activityOptions) {
    final TemporalActivityStubInterceptor<T> interceptor = new TemporalActivityStubInterceptor(workflowImplClass, activityOptions);
    activityStubGenerator.ifPresent(interceptor::setActivityStubGenerator);
    return interceptor;
  }

  @VisibleForTesting
  void setActivityStubGenerator(final TemporalActivityStubGeneratorFunction<Class<?>, ActivityOptions, Object> activityStubGenerator) {
    this.activityStubGenerator = Optional.ofNullable(activityStubGenerator);
  }

}
