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

import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.testing.TypeConfig;

/**
 * Global match algorithm that uses a {@link FuzzyTestValueSimilarity} to match a bag of expected values with a bag of
 * actual values.
 * 
 * @author Arvid Heise
 * @param <V>
 *        the value type
 */
public interface FuzzyValueMatcher<T extends Record> {

	/**
	 * Removes all pairs of matching items from the two collections. The cardinality of both collections is guaranteed
	 * to be the same on input and does not have to be equal on output. The given {@link FuzzyTestValueSimilarity}
	 * defines the similarity measure between two values.<br>
	 * Since both collections have the bag semantic, some values may appear multiple times. The algorithm should remove
	 * only one value per matching pair unless otherwise documented.
	 * 
	 * @param similarities
	 *        the similarity measures
	 * @param expectedValues
	 *        a bag of expected values
	 * @param actualValues
	 *        a bag of actual values
	 */
	public void removeMatchingValues(TypeConfig<T> typeConfig,
			Collection<T> expectedValues, Collection<T> actualValues);
}