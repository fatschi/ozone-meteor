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

import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * @author arv
 */
public class PactRecordKeyExtractor implements KeyExtractor<PactRecord> {
	private final IntList indices;

	private final List<Class<? extends Key>> keyClasses;

	/**
	 * Initializes PactRecordKeyExtractor.
	 * 
	 * @param indexes
	 */
	public PactRecordKeyExtractor(IntList indices, List<Class<? extends Key>> keyClasses) {
		this.indices = indices;
		this.keyClasses = keyClasses;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.KeyExtractor#getKeySize()
	 */
	@Override
	public int getKeySize() {
		return this.indices.size();
	}

	/**
	 * Returns the indices.
	 * 
	 * @return the indices
	 */
	public IntList getIndices() {
		return this.indices;
	}

	/**
	 * Returns the keyClasses.
	 * 
	 * @return the keyClasses
	 */
	public List<Class<? extends Key>> getKeyClasses() {
		return this.keyClasses;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.KeyExtractor#fill(java.lang.Comparable<?>[],
	 * eu.stratosphere.nephele.types.Record)
	 */
	@Override
	public void fill(Comparable<?>[] keys, PactRecord record) {
		for (int index = 0; index < this.indices.size(); index++)
			keys[index] = record.getField(this.indices.getInt(index), this.keyClasses.get(index));
	}

	/**
	 * @param index
	 */
	public void removeKey(int schemaIndex) {
		final int index = this.indices.indexOf(schemaIndex);
		if (index != -1) {
			this.indices.remove(index);
			this.keyClasses.remove(index);
		}
	}
}
