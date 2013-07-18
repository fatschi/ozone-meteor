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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.testing.TypeConfig;

/**
 * Simple matching algorithm that returns unmatched values but allows a value from one bag to be matched several times
 * against items from another bag.
 * 
 * @author Arvid.Heise
 * @param <PactRecord>
 */
public class NaiveFuzzyValueMatcher<T extends Record> implements FuzzyValueMatcher<T> {
	private final TypeDistance<T> typeDistance;

	public NaiveFuzzyValueMatcher(TypeDistance<T> typeDistance) {
		this.typeDistance = typeDistance;
	}

	/**
	 * Returns the typeDistance.
	 * 
	 * @return the typeDistance
	 */
	public TypeDistance<T> getTypeDistance() {
		return this.typeDistance;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.FuzzyValueMatcher#removeMatchingValues(eu.stratosphere.pact.testing.TypeConfig,
	 * java.util.Collection, java.util.Collection)
	 */
	@Override
	public void removeMatchingValues(TypeConfig<T> typeConfig, Collection<T> expectedValues,
			Collection<T> actualValues) {
		Iterator<T> expectedIterator = expectedValues.iterator();
		List<T> matchedActualValues = new ArrayList<T>();
		while (expectedIterator.hasNext()) {
			T expected = expectedIterator.next();
			boolean matched = false;
			for (T actual : actualValues)
				if (this.typeDistance.getDistance(typeConfig, expected, actual) >= 0) {
					matched = true;
					matchedActualValues.add(actual);
				}
			if (matched)
				expectedIterator.remove();
		}

		actualValues.removeAll(matchedActualValues);
	}

}
