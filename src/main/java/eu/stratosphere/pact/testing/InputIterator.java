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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Assert;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.pact.generic.types.TypeSerializer;

/**
 * Provides an {@link Iterator} for {@link InputFormat}s. If multiple formats are specified, it is assumed that they are
 * homogeneous and most likely the result of a parallel execution of the previous {@link Stub}.
 * 
 * @author Arvid Heise
 * @param <K>
 *        the type of the keys
 * @param <V>
 *        the type of the values
 */
public class InputIterator<T extends Record> implements Iterator<T>, Closeable {
	private final List<InputFormat<T, ?>> inputFormats = new ArrayList<InputFormat<T, ?>>();

	private final Iterator<InputFormat<T, ?>> formatIterator;

	private InputFormat<T, ?> currentFormat;

	private final Object[] buffer = new Object[2];

	private int bufferIndex;

	private T nextRecord;

	private final T NO_MORE_PAIR, NOT_LOADED;

	/**
	 * Initializes InputIterator from already configured and opened {@link InputFormat}s.
	 * 
	 * @param reusePair
	 *        true if the pair needs only to be created once and is refilled for each subsequent {@link #next()}
	 * @param inputFormats
	 *        the inputFormats to wrap
	 */
	public InputIterator(TypeSerializer<T> serializer, final List<? extends InputFormat<T, ?>> inputFormats) {
		for (InputFormat<T, ?> inputFormat : inputFormats)
			this.inputFormats.add(inputFormat);
		this.formatIterator = this.inputFormats.iterator();
		this.currentFormat = this.formatIterator.next();
		this.buffer[0] = serializer.createInstance();
		this.buffer[1] = serializer.createInstance();
		this.NO_MORE_PAIR = serializer.createInstance();
		this.NOT_LOADED = serializer.createInstance();
		this.nextRecord = this.NOT_LOADED;
		this.loadNextPair();
	}

	public InputIterator(TypeSerializer<T> serializer, final InputFormat<T, ?> inputFormats) {
		this(serializer, Arrays.asList(inputFormats));
	}

	@Override
	public boolean hasNext() {
		this.loadNextPair();
		return this.nextRecord != this.NO_MORE_PAIR;
	}

	@SuppressWarnings("unchecked")
	private void loadNextPair() {
		if (this.nextRecord != this.NOT_LOADED)
			return;
		try {
			do {
				while (this.currentFormat != null && this.currentFormat.reachedEnd())
					if (this.formatIterator.hasNext())
						this.currentFormat = this.formatIterator.next();
					else {
						this.nextRecord = this.NO_MORE_PAIR;
						return;
					}

				this.nextRecord = (T) this.buffer[this.bufferIndex++ % 2];
			} while (!this.currentFormat.nextRecord(this.nextRecord));

		} catch (final IOException e) {
			this.nextRecord = this.NO_MORE_PAIR;
			Assert.fail("reading expected values " + StringUtils.stringifyException(e));
		}
	}

	@Override
	public T next() {
		if (!this.hasNext())
			throw new NoSuchElementException();
		final T pair = this.nextRecord;
		this.nextRecord = this.NOT_LOADED;
		return pair;
	}

	@Override
	public void close() throws IOException {
		for (final InputFormat<T, ?> inputFormat : this.inputFormats)
			inputFormat.close();
	}

	/**
	 * Not supported.
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}