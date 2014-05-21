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
package eu.stratosphere.sopremo.type;

import java.io.IOException;

/**
 * @author arvid
 *
 */
public class TypeNode extends AbstractJsonNode {

	private Class<? extends IJsonNode> nodeType;

	/**
	 * Initializes FunctionNode.
	 */
	public TypeNode() {
	}

	public TypeNode(final Class<? extends IJsonNode> nodeType) {
		this.nodeType = nodeType;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		appendable.append(this.nodeType.getSimpleName());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#clear()
	 */
	@Override
	public void clear() {
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#compareToSameType(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public int compareToSameType(final IJsonNode other) {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#copyValueFrom(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public void copyValueFrom(final IJsonNode otherNode) {
		this.nodeType = ((TypeNode) otherNode).nodeType;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final TypeNode other = (TypeNode) obj;
		return this.nodeType.equals(other.nodeType);
	}

	/**
	 * Returns the function.
	 * 
	 * @return the function
	 */
	public Class<? extends IJsonNode> getNodeType() {
		return this.nodeType;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#getType()
	 */
	@Override
	public Class<TypeNode> getType() {
		return TypeNode.class;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.nodeType.hashCode();
		return result;
	}

	/**
	 * Sets the function to the specified value.
	 * 
	 * @param function
	 *        the function to set
	 */
	public void setNodeType(final Class<? extends IJsonNode> function) {
		if (function == null)
			throw new NullPointerException("function must not be null");

		this.nodeType = function;
	}
}
