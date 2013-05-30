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

import java.io.IOException;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * Generates a string for the {@link PactRecord} using the given schema.
 */
public class PactRecordStringifier implements TypeStringifier<PactRecord> {
	private final Class<? extends Value>[] schema;

	public PactRecordStringifier(Class<? extends Value>[] schema) {
		this.schema = schema;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.TypeStringifier#appendAsString(java.lang.Appendable, java.lang.Object)
	 */
	@Override
	public void appendAsString(Appendable appendable, PactRecord record) throws IOException {
		if (record == null) {
			appendable.append("null");
			return;
		}

		appendable.append("(");
		for (int index = 0; index < record.getNumFields(); index++) {
			if (index > 0)
				appendable.append(", ");
			appendable.append(record.getField(index, this.schema[index]).toString());
		}
		appendable.append(")");
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.TypeStringifier#toString(java.lang.Object)
	 */
	@Override
	public String toString(PactRecord object) {
		final StringBuilder builder = new StringBuilder();
		try {
			this.appendAsString(builder, object);
		} catch (IOException e) {
		}
		return builder.toString();
	}
}
