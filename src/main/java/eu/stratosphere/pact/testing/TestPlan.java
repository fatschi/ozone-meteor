/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.testing;

import java.util.Collection;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.generic.contract.Contract;

/**
 * The primary resource to test one or more implemented PACT stubs. It is
 * created in a unit tests and performs the following operations.
 * <ul>
 * <li>Adds {@link GenericDataSource<?>}s and {@link GenericDataSink}s if not explicitly specified,
 * <li>locally runs the PACT stubs,
 * <li>checks the results against the pairs as specified in {@link #getExpectedOutput()}, and
 * <li>provides comfortable access to the results with {@link #getActualOutput()}. <br>
 * </ul>
 * <br>
 * The typical usage is inside a unit test. And might look like one of the
 * following examples. <br>
 * <br>
 * <b>Test complete plan<br>
 * <code><pre>
 *    // build plan
 *    GenericDataSource<?>&lt;Key, Value&gt; source = ...;
 *    MapContract&lt;Key, Value, Key, Value&gt; map = new MapContract&lt;Key, Value, Key, Value&gt;(IdentityMap.class, "Map");
 *    map.setInput(source);    
 *    GenericDataSink&lt;Key, Value&gt; output = ...;
 *    output.setInput(map);
 *    // configure test
 *    TestPlan testPlan = new TestPlan(output);
 *    testPlan.getExpectedOutput(output).fromFile(...);
 *    testPlan.run();
 * </pre></code> <b>Test plan with ad-hoc source and sink<br>
 * <code><pre>
 *    // build plan
 *    MapContract&lt;Key, Value, Key, Value&gt; map = new MapContract&lt;Key, Value, Key, Value&gt;(IdentityMap.class, "Map");
 *    // configure test
 *    TestPlan testPlan = new TestPlan(map);
 *    testPlan.getInput().add(pair1).add(pair2).add(pair3);
 *    testPlan.getExpectedOutput(output).add(pair1).add(pair2).add(pair3);
 *    testPlan.run();
 * </pre></code> <b>Access ad-hoc source and sink of Testplan<br>
 * <code><pre>
 *    // build plan
 *    MapContract&lt;Key, Value, Key, Value&gt; map = new MapContract&lt;Key, Value, Key, Value&gt;(IdentityMap.class, "Map");
 *    // configure test
 *    TestPlan testPlan = new TestPlan(map);
 *    testPlan.getInput().add(randomInput1).add(randomInput2).add(randomInput3);
 *    testPlan.run();
 *    // custom assertions
 *    Assert.assertEquals(testPlan.getInput(), testPlan.getOutput());
 * </pre></code> <br>
 * 
 * @author Arvid Heise
 */

public class TestPlan extends GenericTestPlan<PactRecord, TestRecords> {

	public TestPlan(Collection<? extends Contract> contracts) {
		super(contracts);
	}

	public TestPlan(Contract... contracts) {
		super(contracts);
	}

	/**
	 * Sets the default schema of all input and outputs.
	 * 
	 * @param schema
	 *        the schema to set
	 */
	public void setSchema(Class<? extends Value>[] schema) {
		if (schema == null)
			throw new NullPointerException("schema must not be null");

		this.setDefaultConfig(TestRecords.getPactRecordConfig(schema));
	}

	/**
	 * Sets the default schema of all input and outputs.
	 * 
	 * @param schema
	 *        the schema to set
	 */
	public TestPlan withSchema(Class<? extends Value>[] schema) {
		this.setSchema(schema);
		return this;
	}

	/**
	 * Sets the default schema of all input and outputs.
	 * 
	 * @param schema
	 *        the schema to set
	 */
	public void setSchema(Class<? extends Value> firstField, Class<?>... additionalFields) {
		this.setSchema(SchemaUtils.combineSchema(firstField, additionalFields));
	}

	/**
	 * Sets the default schema of all input and outputs.
	 * 
	 * @param schema
	 *        the schema to set
	 */
	public TestPlan withSchema(Class<? extends Value> firstField, Class<?>... additionalFields) {
		this.setSchema(SchemaUtils.combineSchema(firstField, additionalFields));
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.GenericTestPlan#run()
	 */
	@Override
	public void run() {
		this.inferSchemaOfAdhocInputs();

		super.run();
	}

	/**
	 * 
	 */
	private void inferSchemaOfAdhocInputs() {
		for (final GenericDataSource<?> source : this.getSources()) {
			final TestRecords input = this.getInput(source);
			if (input.isAdhoc() && input.getTypeConfig() == null)
				input.inferTypeConfig();
		}
	}

	/**
	 * Returns the schema.
	 * 
	 * @return the schema
	 */
	public Class<? extends Value>[] getSchema() {
		return ((PactRecordEqualer) this.getDefaultConfig().getEqualer()).getSchema();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.GenericTestPlan#createTestRecords(eu.stratosphere.pact.testing.TypeConfig)
	 */
	@Override
	protected TestRecords createTestRecords(TypeConfig<PactRecord> typeConfig) {
		return new TestRecords(typeConfig);
	}

	/**
	 * Returns the expected output {@link GenericTestRecords} with the given schema of the TestPlan
	 * associated with the given sink. This is the recommended method to set expected
	 * output records for more complex TestPlans.
	 * 
	 * @param sink
	 *        the sink of which the associated expected output GenericTestRecords
	 *        should be returned
	 * @return the expected output {@link GenericTestRecords} of the TestPlan associated
	 *         with the given sink
	 */
	public TestRecords getExpectedOutput(Class<? extends Value> values, Class<?>... additionalFields) {
		return this.getExpectedOutput(TestRecords.getPactRecordConfig(SchemaUtils.combineSchema(values,
			additionalFields)));
	}

	/**
	 * Returns the expected output {@link GenericTestRecords} with the given schema of the TestPlan
	 * associated with the given sink. This is the recommended method to set expected
	 * output records for more complex TestPlans.
	 * 
	 * @param sink
	 *        the sink of which the associated expected output GenericTestRecords
	 *        should be returned
	 * @return the expected output {@link GenericTestRecords} of the TestPlan associated
	 *         with the given sink
	 */
	public TestRecords getExpectedOutput(GenericDataSink output, Class<? extends Value>[] schema) {
		return this.getExpectedOutput(output, TestRecords.getPactRecordConfig(schema));
	}
	
	/**
	 * Returns the expected output {@link GenericTestRecords} with the given schema of the TestPlan
	 * associated with the given sink. This is the recommended method to set expected
	 * output records for more complex TestPlans.
	 * 
	 * @param sink
	 *        the sink of which the associated expected output GenericTestRecords
	 *        should be returned
	 * @return the expected output {@link GenericTestRecords} of the TestPlan associated
	 *         with the given sink
	 */
	public TestRecords getExpectedOutput(int sink, Class<? extends Value>[] schema) {
		return this.getExpectedOutput(sink, TestRecords.getPactRecordConfig(schema));
	}

	public TestRecords getActualOutput(Class<? extends Value>[] schema) {
		return this.getActualOutput(TestRecords.getPactRecordConfig(schema));
	}
}