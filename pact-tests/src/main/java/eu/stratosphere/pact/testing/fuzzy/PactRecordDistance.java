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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.testing.Equaler;
import eu.stratosphere.pact.testing.TestRecords;
import eu.stratosphere.pact.testing.TypeConfig;

/**
 * Simple matching algorithm that returns unmatched values but allows a value from one bag to be matched several times
 * against items from another bag.
 * 
 * @author Arvid.Heise
 * @param <PactRecord>
 */
public class PactRecordDistance implements TypeDistance<PactRecord> {
	private Int2ObjectMap<List<ValueSimilarity<?>>> similarities =
		new Int2ObjectOpenHashMap<List<ValueSimilarity<?>>>();

	public void addSimilarity(int index, ValueSimilarity<?> similarity) {
		List<ValueSimilarity<?>> list = this.similarities.get(index);
		if (list == null)
			this.similarities.put(index, list = new ArrayList<ValueSimilarity<?>>());
		list.add(similarity);
	}

	/**
	 * Calculates the overall distance between the expected and actual record.
	 */
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public double getDistance(TypeConfig<PactRecord> typeConfig, PactRecord expectedRecord, PactRecord actualRecord) {
		Class<? extends Value>[] schema = TestRecords.getSchema(typeConfig);

		double distance = 0;
		for (int index = 0; index < schema.length; index++) {
			Value expected = expectedRecord.getField(index, schema[index]);
			Value actual = actualRecord.getField(index, schema[index]);

			List<ValueSimilarity<?>> sims = this.similarities.get(index);
			List<ValueSimilarity<?>> allSims = this.similarities.get(-1);
			if (sims == null && allSims == null) {
				if (!Equaler.SafeEquals.equal(actual, expected))
					return ValueSimilarity.NO_MATCH;
				continue;
			}

			double valueDistance = 0;
			int appliedSimilarities = 0;
			if (sims != null) {
				for (ValueSimilarity sim : sims) {
					double simDistance = sim.getDistance(expected, actual);
					if (simDistance < 0)
						return simDistance;
					valueDistance += simDistance;
				}
				appliedSimilarities = sims.size();
			}
			if (allSims != null)
				for (ValueSimilarity sim : allSims) {
					if (sim.isApplicable(schema[index])) {
						double simDistance = sim.getDistance(expected, actual);
						if (simDistance < 0)
							return simDistance;
						valueDistance += simDistance;
						appliedSimilarities++;
					}
				}
			distance += valueDistance / appliedSimilarities;
		}
		return distance;
	}
}
