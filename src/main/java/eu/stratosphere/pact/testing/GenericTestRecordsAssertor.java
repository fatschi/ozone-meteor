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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.testing.fuzzy.FuzzyValueMatcher;

/**
 * Utility class that is used from {@link GenericTestRecords#assertEquals(GenericTestRecords)}.<br>
 * It tries to align all matching records by successively eliminating matching records from the multiset of expected and
 * actual values.<br>
 * To reduce memory consumption, only tuples with the same key are held within memory. The key is simply determined by
 * using all parts of the schema that implement {@link Key} and that are not matched fuzzily.
 * 
 * @author Arvid Heise
 */
class GenericTestRecordsAssertor<T extends Record> {
	private GenericTestRecords<T> expectedValues, actualRecords;

	private Iterator<T> actualIterator;

	private Iterator<T> expectedIterator;

	private final TypeSerializer<T> typeSerializer;

	private final TypeStringifier<T> typeStringifier;

	private final KeyExtractor<T> keyExtractor;

	private final FuzzyValueMatcher<T> fuzzyMatcher;

	private final TypeConfig<T> typeConfig;

	public GenericTestRecordsAssertor(TypeConfig<T> typeConfig, GenericTestRecords<T> expectedValues,
			GenericTestRecords<T> actualRecords) {
		this.expectedValues = expectedValues;
		this.actualRecords = actualRecords;
		this.actualIterator = actualRecords.iterator();
		this.expectedIterator = expectedValues.iterator();

		this.typeConfig = typeConfig;
		this.fuzzyMatcher = typeConfig.getFuzzyValueMatcher();
		this.keyExtractor = typeConfig.getKeyExtractor();
		this.typeSerializer = typeConfig.getTypeSerializer();
		this.typeStringifier = typeConfig.getTypeStringifier();
	}

	@SuppressWarnings("unchecked")
	public void assertEquals() {
		try {
			// initialize with null
			final int keySize = this.keyExtractor.getKeySize();
			Comparable<T>[] currentKeys = new Comparable[keySize];
			Comparable<T>[] nextKeys = new Comparable[keySize];
			int itemIndex = 0;
			List<T> expectedValuesWithCurrentKey = new ArrayList<T>();
			List<T> actualValuesWithCurrentKey = new ArrayList<T>();
			if (this.expectedIterator.hasNext()) {
				T expected = this.typeSerializer.createCopy(this.expectedIterator.next());
				this.keyExtractor.fill(currentKeys, expected);
				expectedValuesWithCurrentKey.add(expected);

				// take chunks of expected values with the same keys and match them
				while (this.actualIterator.hasNext() && this.expectedIterator.hasNext()) {
					expected = this.typeSerializer.createCopy(this.expectedIterator.next());
					this.keyExtractor.fill(nextKeys, expected);
					if (!Arrays.equals(currentKeys, nextKeys)) {
						this.matchValues(currentKeys, itemIndex, expectedValuesWithCurrentKey,
							actualValuesWithCurrentKey);
						this.keyExtractor.fill(currentKeys, expected);
					}
					expectedValuesWithCurrentKey.add(expected);

					itemIndex++;
				}

				// remaining values
				if (!expectedValuesWithCurrentKey.isEmpty())
					this.matchValues(currentKeys, itemIndex, expectedValuesWithCurrentKey, actualValuesWithCurrentKey);
			}

			if (!expectedValuesWithCurrentKey.isEmpty() || this.expectedIterator.hasNext())
				Assert.fail("More elements expected: " + expectedValuesWithCurrentKey
					+ IteratorUtil.stringify(this.typeStringifier, this.expectedIterator));
			if (!actualValuesWithCurrentKey.isEmpty() || this.actualIterator.hasNext())
				Assert.fail("Less elements expected: " + actualValuesWithCurrentKey
					+ IteratorUtil.stringify(this.typeStringifier, this.actualIterator));
		} finally {
			this.actualRecords.close();
			this.expectedValues.close();
		}
	}

	@SuppressWarnings("unchecked")
	private void matchValues(Comparable<T>[] currentKeys, int itemIndex, List<T> expectedValuesWithCurrentKey,
			List<T> actualValuesWithCurrentKey) throws AssertionError {

		final int keySize = this.keyExtractor.getKeySize();
		Comparable<T>[] actualKeys = new Comparable[keySize];

		// collect all actual values with the same key
		T actualRecord = null;
		while (this.actualIterator.hasNext()) {
			actualRecord = this.typeSerializer.createCopy(this.actualIterator.next());
			this.keyExtractor.fill(actualKeys, actualRecord);

			if (!Arrays.equals(currentKeys, actualKeys))
				break;
			actualValuesWithCurrentKey.add(actualRecord);
			actualRecord = null;
		}

		if (actualValuesWithCurrentKey.isEmpty()) {
			final int diffIndex = itemIndex + expectedValuesWithCurrentKey.size() - 1;
			Assert.fail(String.format("No value for key %s @ %d, expected: %s, but was: %s",
				Arrays.toString(currentKeys), diffIndex,
				IteratorUtil.stringify(this.typeStringifier, expectedValuesWithCurrentKey.iterator()),
				this.typeStringifier.toString(actualRecord)));
		}

		// and invoke the fuzzy matcher
		this.fuzzyMatcher.removeMatchingValues(this.typeConfig, expectedValuesWithCurrentKey,
			actualValuesWithCurrentKey);

		if (!expectedValuesWithCurrentKey.isEmpty() || !actualValuesWithCurrentKey.isEmpty()) {
			int diffIndex = itemIndex - expectedValuesWithCurrentKey.size();
			Assert.fail(String.format("Unmatched values for key %s @ %d, expected: %s, but was: %s",
				Arrays.toString(currentKeys), diffIndex,
				IteratorUtil.stringify(this.typeStringifier, expectedValuesWithCurrentKey.iterator()),
				IteratorUtil.stringify(this.typeStringifier, actualValuesWithCurrentKey.iterator())));
		}

		// don't forget the first record that has a different key
		if (actualRecord != null)
			actualValuesWithCurrentKey.add(actualRecord);
	}
}