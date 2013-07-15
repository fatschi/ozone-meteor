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

/**
 * @author arvid
 *
 */
public final class DefaultStringifier implements TypeStringifier<Object> {
	private final static DefaultStringifier Instance = new DefaultStringifier();
	
	/**
	 * Returns the instance.
	 * 
	 * @return the instance
	 */
	@SuppressWarnings("unchecked")
	public static <T> TypeStringifier<T> get() {
		return (TypeStringifier<T>) Instance;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.TypeStringifier#appendAsString(java.lang.Appendable, java.lang.Object)
	 */
	@Override
	public void appendAsString(Appendable appendable, Object object) throws IOException {
		appendable.append(object.toString());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.TypeStringifier#toString(java.lang.Object)
	 */
	@Override
	public String toString(Object object) {
		return object.toString();
	}
}