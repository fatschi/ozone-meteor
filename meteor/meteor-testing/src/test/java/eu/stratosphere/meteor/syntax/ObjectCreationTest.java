/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.meteor.syntax;

import org.junit.Test;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * 
 */
public class ObjectCreationTest extends SyntaxTest {

	/**
	 * 
	 */
	@Test
	public void testFieldAssignments() {
		final SopremoPlan actualPlan =
			this.parseScript("$li = read from 'file://lineitem.json';\n" +
				"$li = transform $li into {\n" +
				" a: $li.b\n" +
				"};\n" +
				"write $li to 'file://q1.json';\n");

		final Source input = new Source("file://lineitem.json");
		final Projection filter = new Projection().
			withInputs(input).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("a", JsonUtil.createPath("0", "b"))));

		final Sink sink = new Sink("file://q1.json").withInputs(filter);
		final SopremoPlan expectedPlan = new SopremoPlan();
		expectedPlan.setSinks(sink);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	/**
	 * 
	 */
	@Test
	public void testSymbolicAssignments() {
		final SopremoPlan actualPlan =
			this.parseScript("$li = read from 'file://lineitem.json';\n" +
				"$li = transform $li into {\n" +
				" $li.a: $li.b\n" +
				"};\n" +
				"write $li to 'file://q1.json';\n");

		final Source input = new Source("file://lineitem.json");
		final Projection filter = new Projection().
			withInputs(input).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.SymbolicAssignment(JsonUtil.createPath("0", "a"), JsonUtil.createPath("0", "b"))));

		final Sink sink = new Sink("file://q1.json").withInputs(filter);
		final SopremoPlan expectedPlan = new SopremoPlan();
		expectedPlan.setSinks(sink);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	/**
	 * 
	 */
	@Test
	public void testComplexSymbolicAssignments() {
		final SopremoPlan actualPlan =
			this.parseScript("$li = read from 'file://lineitem.json';\n" +
				"$li = transform $li into {\n" +
				" substring($li.a, 1, 3): substring($li.b, 2, 4)\n" +
				"};\n" +
				"write $li to 'file://q1.json';\n");

		final Source input = new Source("file://lineitem.json");
		final Projection filter = new Projection().
			withInputs(input).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.SymbolicAssignment(
					FunctionUtil.createFunctionCall(CoreFunctions.SUBSTRING,
						JsonUtil.createPath("0", "a"), new ConstantExpression(1), new ConstantExpression(3)),
					FunctionUtil.createFunctionCall(CoreFunctions.SUBSTRING,
						JsonUtil.createPath("0", "b"), new ConstantExpression(2), new ConstantExpression(4)))));

		final Sink sink = new Sink("file://q1.json").withInputs(filter);
		final SopremoPlan expectedPlan = new SopremoPlan();
		expectedPlan.setSinks(sink);

		assertPlanEquals(expectedPlan, actualPlan);
	}
}
