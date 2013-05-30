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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;
import eu.stratosphere.pact.testing.fuzzy.DoubleValueSimilarity;
import eu.stratosphere.pact.testing.fuzzy.FuzzyValueMatcher;
import eu.stratosphere.pact.testing.fuzzy.NaiveFuzzyValueMatcher;
import eu.stratosphere.pact.testing.fuzzy.PactRecordDistance;

/**
 * Represents the input or output values of a {@link TestPlan}. The class is
 * especially important when setting the expected values in the TestPlan.<br>
 * <br>
 * There are two ways to specify the values:
 * <ol>
 * <li>From a file: with {@link #fromFile(Class, String)} and {@link #fromFile(Class, String, Configuration)} the
 * location, format, and configuration of the data can be specified. The file is lazily loaded and thus can be
 * comparable large.
 * <li>Ad-hoc: key/value records can be added with {@link #add(Key, Value)}, {@link #add(KeyValuePair...)}, and
 * {@link #add(Iterable)}. Please note that the actual amount of records is quite for a test case as the TestPlan
 * already involves a certain degree of overhead.<br>
 * <br>
 * TestRecords are directly comparable with equals and hashCode based on its content. Please note that in the case of
 * large file-based TestRecords, the time needed to compute the {@link #hashCode()} or to compare two instances with
 * {@link #equals(Object)} can become quite long. Currently, the comparison result is order-dependent as TestRecords are
 * interpreted as a list.<br>
 * <br>
 * Currently there is no notion of an empty set of records.
 * 
 * @author Arvid Heise
 */
public class TestRecords extends GenericTestRecords<PactRecord> {

	/**
	 * @author arv
	 */
	private static final class PactRecordComparatorFactory implements TypeComparatorFactory<PactRecord> {
		private final TypeConfig<PactRecord>[] typeConfigReference;

		/**
		 * Initializes PactRecordComparatorFactory.
		 * 
		 * @param typeConfigReference
		 */
		public PactRecordComparatorFactory(TypeConfig<PactRecord>[] typeConfigReference) {
			this.typeConfigReference = typeConfigReference;
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.generic.types.TypeComparatorFactory#createComparator()
		 */
		@SuppressWarnings("unchecked")
		@Override
		public TypeComparator<PactRecord> createComparator() {
			TypeConfig<PactRecord> typeConfig = this.typeConfigReference[0];
			final PactRecordKeyExtractor keyExtractor = (PactRecordKeyExtractor) typeConfig.getKeyExtractor();
			return new PactRecordComparator(keyExtractor.getIndices().toIntArray(), keyExtractor.getKeyClasses().toArray(new Class[0]));
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.generic.types.TypeComparatorFactory#readParametersFromConfig(eu.stratosphere.nephele.configuration.Configuration, java.lang.ClassLoader)
		 */
		@Override
		public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.generic.types.TypeComparatorFactory#writeParametersToConfig(eu.stratosphere.nephele.configuration.Configuration)
		 */
		@Override
		public void writeParametersToConfig(Configuration config) {
		}
	}

	/**
	 * Initializes TestRecords.
	 */
	public TestRecords() {
	}

	/**
	 * Initializes TestRecords.
	 * 
	 * @param typeConfig
	 */
	public TestRecords(TypeConfig<PactRecord> typeConfig) {
		super(typeConfig);
	}

	public TestRecords(Class<? extends Value> firstEntry, Class<?>... remainingEntries) {
		super(getPactRecordConfig(firstEntry, remainingEntries));
	}

	public static final TypeConfig<PactRecord> getPactRecordConfig(Class<? extends Value> firstEntry,
			Class<?>... remainingEntries) {
		return getPactRecordConfig(SchemaUtils.combineSchema(firstEntry, remainingEntries));
	}

	public void setAllowedPactDoubleDelta(double delta) {
		final TypeConfig<PactRecord> typeConfig = this.getTypeConfig().clone();
		final FuzzyValueMatcher<PactRecord> fuzzyValueMatcher = typeConfig.getFuzzyValueMatcher();
		final PactRecordDistance pactRecordDistance;
		if (fuzzyValueMatcher instanceof NaiveFuzzyValueMatcher)
			pactRecordDistance =
				(PactRecordDistance) ((NaiveFuzzyValueMatcher<PactRecord>) fuzzyValueMatcher).getTypeDistance();
		else
			typeConfig.setFuzzyValueMatcher(new NaiveFuzzyValueMatcher<PactRecord>(
				pactRecordDistance = new PactRecordDistance()));

		final Class<? extends Value>[] schema = getSchema(typeConfig);
		final DoubleValueSimilarity similarity = new DoubleValueSimilarity(delta);
		final PactRecordKeyExtractor keyExtractor = (PactRecordKeyExtractor) typeConfig.getKeyExtractor();
		for (int index = 0; index < schema.length; index++) {
			if (similarity.isApplicable(schema[index])) {
				pactRecordDistance.addSimilarity(index, similarity);
				keyExtractor.removeKey(index);
			}
		}
		this.setTypeConfig(typeConfig);
	}

	public TestRecords withAllowedPactDoubleDelta(double delta) {
		this.setAllowedPactDoubleDelta(delta);
		return this;
	}

	@SuppressWarnings("unchecked")
	public static final TypeConfig<PactRecord> getPactRecordConfig(Class<? extends Value>[] schema) {
		IntList indices = new IntArrayList();
		List<Class<? extends Key>> keyTypes = new ArrayList<Class<? extends Key>>();
		for (int index = 0; index < schema.length; index++)
			if (Key.class.isAssignableFrom(schema[index])) {
				indices.add(index);
				keyTypes.add((Class<? extends Key>) schema[index]);
			}

		final PactRecordKeyExtractor keyExtractor =
			new PactRecordKeyExtractor(indices, keyTypes);
		final TypeConfig<PactRecord>[] typeConfigReference = new TypeConfig[1];
		typeConfigReference[0] =
			new TypeConfig<PactRecord>(new PactRecordComparatorFactory(typeConfigReference),
				PactRecordPairComparatorFactory.get(),
				PactRecordSerializerFactory.get(), new PactRecordStringifier(schema), keyExtractor,
				new PactRecordEqualer(schema));
		return typeConfigReference[0];
	}

	public static final Class<? extends Value>[] getSchema(TypeConfig<PactRecord> config) {
		return ((PactRecordEqualer) config.getEqualer()).getSchema();
	}

	/**
	 * @param pactInteger
	 * @param pactDouble
	 * @return
	 */
	public TestRecords add(Value... values) {
		final PactRecord pactRecord = new PactRecord();
		for (Value value : values)
			pactRecord.addField(value);
		this.add(pactRecord);
		return this;
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	public void inferTypeConfig() {
		final List<PactRecord> records = this.getRecords();
		if (records.isEmpty())
			this.setTypeConfig(getPactRecordConfig(new Class[0]));
		else {
			PactRecord first = records.get(0);
			Class<? extends Value>[] schema = new Class[first.getNumFields()];
			for (int index = 0; index < schema.length; index++)
				schema[index] = first.getField(index, Value.class).getClass();
			this.setTypeConfig(getPactRecordConfig(schema));
		}

	}

}