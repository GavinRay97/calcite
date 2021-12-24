/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules

import org.apache.calcite.plan.hep.HepPlanner
/**
 * Logical transformation rule, only logical operator can be rule operand,
 * and only generate logical alternatives. It is only visible to
 * [VolcanoPlanner], [HepPlanner] will ignore this interface.
 * That means, in [HepPlanner], the rule that implements
 * [TransformationRule] can still match with physical operator of
 * [PhysicalNode] and generate physical alternatives.
 *
 *
 * But in [VolcanoPlanner], [TransformationRule] doesn't match
 * with physical operator that implements [PhysicalNode]. It is not
 * allowed to generate physical operators in [TransformationRule],
 * unless you are using it in [HepPlanner].
 *
 * @see VolcanoPlanner
 *
 * @see SubstitutionRule
 */
interface TransformationRule
