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

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeSerializer;

/**
 * @author Arvid Heise
 */
public class ImmutableRecordIterator<T extends Record> implements Iterator<T> {
	private T next;

	private MutableObjectIterator<T> iterator;

	private boolean hasNext;

	private final TypeSerializer<T> typeSerializer;

	public ImmutableRecordIterator(TypeSerializer<T> typeSerializer, MutableObjectIterator<T> iterator) {
		this.typeSerializer = typeSerializer;
		this.iterator = iterator;
		try {
			this.hasNext = iterator.next(this.next = this.typeSerializer.createInstance());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return this.hasNext;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {
		T next = this.next;
		try {
			this.hasNext = this.iterator.next(this.next = this.typeSerializer.createInstance());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return next;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
