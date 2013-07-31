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

import java.util.Iterator;

import eu.stratosphere.nephele.types.Record;

/**
 * Additional assertions for unit tests.
 * 
 * @author Arvid Heise
 */
public class AssertUtil {
	/**
	 * Asserts that two iterators generate equal series of objects.
	 * 
	 * @param message
	 * @param expectedIterator
	 * @param actualIterator
	 */
	public static <T> void assertIteratorEquals(String message, Iterator<? extends T> expectedIterator,
			Iterator<? extends T> actualIterator,
			Equaler<T> equaler, TypeStringifier<T> stringifier) {

		int index = 0;
		for (; actualIterator.hasNext() && expectedIterator.hasNext(); index++) {
			final T expected = expectedIterator.next(), actual = actualIterator.next();
			if (!equaler.equal(expected, actual))
				throw new AssertionError(String.format("%s @ %d, expected: %s, but was %s", message, index,
					stringifier.toString(expected), stringifier.toString(actual)));
		}

		if (expectedIterator.hasNext())
			throw new AssertionError(String.format("%s: More elements expected @ ", message, index));
		if (actualIterator.hasNext())
			throw new AssertionError(String.format("%s: Less elements expected @ ", message, index));
	}

	/**
	 * Asserts that two iterators generate equal series of objects.
	 * 
	 * @param expectedIterator
	 * @param actualIterator
	 */
	public static <T> void assertIteratorEquals(Iterator<? extends T> expectedIterator,
			Iterator<? extends T> actualIterator,
			Equaler<T> equaler,
			TypeStringifier<T> stringifier) {
		assertIteratorEquals(null, expectedIterator, actualIterator, equaler, stringifier);
	}

	// not used
	// /**
	// * Asserts that two iterators generate equal series of objects.
	// *
	// * @param expectedIterator
	// * @param actualIterator
	// */
	// public static <T> void assertIteratorEquals(Iterator<? extends T>
	// expectedIterator, Iterator<T> actualIterator) {
	// assertIteratorEquals(null, expectedIterator, actualIterator,
	// Equaler.JavaEquals, TypeStringifier.JavaToString);
	// }

	/**
	 * Asserts that two iterators generate equal series of objects.
	 * 
	 * @param expectedIterator
	 * @param actualIterator
	 */
	public static <T extends Record> void assertIteratorEquals(Iterator<T> expectedIterator,
			Iterator<T> actualIterator,
			TypeConfig<T> config) {
		assertIteratorEquals(null, expectedIterator, actualIterator, config.getEqualer(), config.getTypeStringifier());
	}

	/**
	 * Asserts that two iterators generate equal series of objects.
	 * 
	 * @param expectedIterator
	 * @param actualIterator
	 */
	public static <T extends Record> void assertIteratorEquals(String message, Iterator<T> expectedIterator,
			Iterator<T> actualIterator,
			TypeConfig<T> config) {
		assertIteratorEquals(message, expectedIterator, actualIterator, config.getEqualer(),
			config.getTypeStringifier());
	}

	/**
	 * Asserts that two iterators generate equal series of objects.
	 * 
	 * @param message
	 * @param expectedIterator
	 * @param actualIterator
	 */
	public static <T> void assertIteratorEquals(String message, Iterator<? extends T> expectedIterator,
			Iterator<? extends T> actualIterator, Equaler<T> equaler) {
		TypeStringifier<T> typeStringifier = DefaultStringifier.get();
		assertIteratorEquals(message, expectedIterator, actualIterator, equaler, typeStringifier);
	}

	/**
	 * Asserts that two iterators generate equal series of objects.
	 * 
	 * @param expectedIterator
	 * @param actualIterator
	 */
	public static <T> void assertIteratorEquals(Iterator<? extends T> expectedIterator,
			Iterator<? extends T> actualIterator,
			Equaler<T> equaler) {
		assertIteratorEquals(null, expectedIterator, actualIterator, equaler);
	}

	/**
	 * Asserts that two iterators generate equal series of objects.
	 * 
	 * @param expectedIterator
	 * @param actualIterator
	 */
	public static <T> void assertIteratorEquals(Iterator<? extends T> expectedIterator,
			Iterator<? extends T> actualIterator) {
		assertIteratorEquals(null, expectedIterator, actualIterator, DefaultEqualer.get());
	}
}
