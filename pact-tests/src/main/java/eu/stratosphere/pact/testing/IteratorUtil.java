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

/**
 * Convenience methods for working with {@link PactRecord}s.
 * 
 * @author Arvid Heise
 */
public class IteratorUtil {

	/**
	 * Generates a string for the {@link PactRecord}s using the given schema.<br>
	 * This method stringifies at most 20 records.<br>
	 * To exert more control over the amounts of records, use {@link #toString(Iterator, Class[], int)}.
	 * 
	 * @param record
	 *        the record to stringify
	 * @param schema
	 *        the schema that is used to retrieve the values of the record
	 * @return a string representation of the records
	 */
	public static <T extends Record> String stringify(TypeStringifier<T> typeStringifier, Iterator<T> iterator) {
		return stringify(typeStringifier, iterator, 20);
	}

	/**
	 * Generates a string for the first <i>maxNum</i> {@link PactRecord}s using the given schema.
	 * 
	 * @param record
	 *        the record to stringify
	 * @param schema
	 *        the schema that is used to retrieve the values of the record
	 * @param maxNum
	 *        the maximum number of records to stringify
	 * @return a string representation of the records
	 */
	public static <T extends Record> String stringify(TypeStringifier<T> typeStringifier, Iterator<T> iterator,
			int maxNum) {
		StringBuilder builder = new StringBuilder();
		try {
			for (int index = 0; index < maxNum && iterator.hasNext(); index++) {
				typeStringifier.appendAsString(builder, iterator.next());
				if (iterator.hasNext())
					builder.append(", ");
			}
		} catch (IOException e) {
		}
		if (iterator.hasNext())
			builder.append("...");
		return builder.toString();
	}

}
