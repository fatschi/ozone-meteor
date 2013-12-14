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
package eu.stratosphere.sopremo.serialization;

import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author arvid
 */
public class JsonNodeHolder {
	private IJsonNode node;

	/**
	 * Returns the node.
	 * 
	 * @return the node
	 */
	public IJsonNode getNode() {
		return this.node;
	}

	/**
	 * Sets the node to the specified value.
	 * 
	 * @param node
	 *        the node to set
	 */
	public void setNode(final IJsonNode node) {
		if (node == null)
			throw new NullPointerException("node must not be null");

		this.node = node;
	}

}
