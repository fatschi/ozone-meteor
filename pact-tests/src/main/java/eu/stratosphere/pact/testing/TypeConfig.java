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
package eu.stratosphere.pact.testing;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypePairComparator;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.testing.fuzzy.EqualityValueMatcher;
import eu.stratosphere.pact.testing.fuzzy.FuzzyValueMatcher;

/**
 * @author arv
 */
public class TypeConfig<T extends Record> implements Cloneable {
	private final TypeComparatorFactory<T> typeComparatorFactory;

	private final TypePairComparatorFactory<T, T> typePairComparatorFactory;

	private final TypeSerializerFactory<T> typeSerializerFactory;

	private final TypeStringifier<T> typeStringifier;

	private final Configuration configuration = new Configuration();

	private KeyExtractor<T> keyExtractor;

	private FuzzyValueMatcher<T> fuzzyValueMatcher;

	private final Equaler<T> equaler;

	protected TypeConfig(TypeComparatorFactory<T> typeComparatorFactory,
			TypePairComparatorFactory<T, T> typePairComparatorFactory, TypeSerializerFactory<T> typeSerializerFactory,
			TypeStringifier<T> typeStringifier, KeyExtractor<T> keyExtractor, Equaler<T> equaler) {
		this.typeComparatorFactory = typeComparatorFactory;
		this.typePairComparatorFactory = typePairComparatorFactory;
		this.typeSerializerFactory = typeSerializerFactory;
		this.typeStringifier = typeStringifier;
		this.equaler = equaler;
		this.keyExtractor = keyExtractor;
		this.fuzzyValueMatcher = new EqualityValueMatcher<T>(equaler);
	}

	/**
	 * Returns the fuzzyValueMatcher.
	 * 
	 * @return the fuzzyValueMatcher
	 */
	public FuzzyValueMatcher<T> getFuzzyValueMatcher() {
		return this.fuzzyValueMatcher;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public TypeConfig<T> clone() {
		try {
			return (TypeConfig<T>) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new IllegalStateException();
		}
	}

	/**
	 * Returns the equaler.
	 * 
	 * @return the equaler
	 */
	public Equaler<T> getEqualer() {
		return this.equaler;
	}

	/**
	 * Returns the keyExtractor.
	 * 
	 * @return the keyExtractor
	 */
	public KeyExtractor<T> getKeyExtractor() {
		return this.keyExtractor;
	}

	/**
	 * Returns the typeStringifier.
	 * 
	 * @return the typeStringifier
	 */
	public TypeStringifier<T> getTypeStringifier() {
		return this.typeStringifier;
	}

	public TypeSerializer<T> getTypeSerializer() {
		return this.getTypeSerializerFactory().getSerializer();
	}

	public TypePairComparator<T, T> getTypePairComparator() {
		TypeComparator<T> typeComparator = this.getTypeComparator();
		return this.getTypePairComparatorFactory().createComparator12(typeComparator, typeComparator);
	}

	public TypeComparator<T> getTypeComparator() {
		try {
			final TypeComparatorFactory<T> factory = this.getTypeComparatorFactory();
			factory.readParametersFromConfig(this.getConfiguration(),
				ClassLoader.getSystemClassLoader());
			return factory.createComparator();
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Class cannot be found in a local test setting", e);
		}
	}

	public TypeComparatorFactory<T> getTypeComparatorFactory() {
		return this.typeComparatorFactory;
	}

	public TypePairComparatorFactory<T, T> getTypePairComparatorFactory() {
		return this.typePairComparatorFactory;
	}

	public TypeSerializerFactory<T> getTypeSerializerFactory() {
		return this.typeSerializerFactory;
	}

	/**
	 * Returns the configuration.
	 * 
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return this.configuration;
	}

	/**
	 * Sets the keyExtractor to the specified value.
	 * 
	 * @param keyExtractor
	 *        the keyExtractor to set
	 */
	public void setKeyExtractor(KeyExtractor<T> keyExtractor) {
		if (keyExtractor == null)
			throw new NullPointerException("keyExtractor must not be null");

		this.keyExtractor = keyExtractor;
	}

	/**
	 * Sets the fuzzyValueMatcher to the specified value.
	 * 
	 * @param fuzzyValueMatcher
	 *        the fuzzyValueMatcher to set
	 */
	public void setFuzzyValueMatcher(FuzzyValueMatcher<T> fuzzyValueMatcher) {
		if (fuzzyValueMatcher == null)
			throw new NullPointerException("fuzzyValueMatcher must not be null");

		this.fuzzyValueMatcher = fuzzyValueMatcher;
	}

}
