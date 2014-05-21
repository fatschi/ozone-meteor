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
package eu.stratosphere.sopremo;

import java.io.File;
import java.io.IOException;

public class TestBase {

	public static AssertionError newAssertionError(String message, Throwable cause) {
		final AssertionError assertionError = new AssertionError(message);
		assertionError.initCause(cause);
		return assertionError;
	}

	public File getResource(final String name) {
		try {
			return new File(getClass().getClassLoader().getResources(name).nextElement().getFile());
		} catch (IOException e) {
			throw newAssertionError("Could not locate resource " + name, e);
		}
	}

}