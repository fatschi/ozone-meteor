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

package eu.stratosphere.pact.testing.fuzzy;

import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.testing.Equaler;
import eu.stratosphere.pact.testing.TypeConfig;

/**
 * Matches all exact matching pairs using equals.
 * 
 * @author Arvid Heise
 * @param <V>
 */
public class EqualityValueMatcher<T extends Record> implements FuzzyValueMatcher<T> {
	private Equaler<T> recordEqualer;

	public EqualityValueMatcher(Equaler<T> recordEqualer) {
		this.recordEqualer = recordEqualer;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.FuzzyValueMatcher#removeMatchingValues(eu.stratosphere.pact.testing.TypeConfig,
	 * java.util.Collection, java.util.Collection)
	 */
	@Override
	public void removeMatchingValues(TypeConfig<T> typeConfig, Collection<T> expectedValues, Collection<T> actualValues) {
		Iterator<T> actualIterator = actualValues.iterator();
		while (!expectedValues.isEmpty() && actualIterator.hasNext()) {
			// match
			final T actual = actualIterator.next();

			Iterator<T> expectedIterator = expectedValues.iterator();
			while (expectedIterator.hasNext())
				if (this.recordEqualer.equal(actual, expectedIterator.next())) {
					actualIterator.remove();
					expectedIterator.remove();
					break;
				}
		}
	}

}
