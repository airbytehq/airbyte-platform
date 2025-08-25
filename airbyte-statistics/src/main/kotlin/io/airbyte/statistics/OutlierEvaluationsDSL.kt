/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.statistics

/**
 * DSL for constructing outlier evaluations rules.
 *
 * Those are not strictly necessary, but since the rules are currently hard-coded,
 * these provide a slightly more math-like syntax to construct the expressions.
 */
operator fun Expression.div(other: Expression): Expression = Operator(this, other, Operator.Type.Divide)

operator fun Expression.minus(other: Expression): Expression = Operator(this, other, Operator.Type.Minus)

operator fun Expression.plus(other: Expression): Expression = Operator(this, other, Operator.Type.Plus)

operator fun Expression.times(other: Expression): Expression = Operator(this, other, Operator.Type.Times)

val Dimension.identity: Dimension get() = copy(f = Dimension.StatisticalFunction.IDENTITY)
val Dimension.mean: Dimension get() = copy(f = Dimension.StatisticalFunction.MEAN)
val Dimension.std: Dimension get() = copy(f = Dimension.StatisticalFunction.STD)
val Dimension.zScore: Dimension get() = copy(f = Dimension.StatisticalFunction.ZSCORE)
