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
package eu.stratosphere.sopremo.type;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Interface for all nodes that store numerical values.
 * 
 */
public interface INumericNode extends IPrimitiveNode {

	/**
	 * Returns this nodes value as an <code>int</code>.
	 */
	public abstract int getIntValue();

	/**
	 * Returns this nodes value as a <code>long</code>.
	 */
	public abstract long getLongValue();

	/**
	 * Returns this nodes value as a {@link BigInteger}.
	 */
	public abstract BigInteger getBigIntegerValue();

	/**
	 * Returns this nodes value as a {@link BigDecimal}.
	 */
	public abstract BigDecimal getDecimalValue();

	/**
	 * Returns this nodes value as a <code>double</code>.
	 */
	public abstract double getDoubleValue();

	/**
	 * Returns the String representation of this nodes value.
	 */
	public abstract String getValueAsText();

	/**
	 * Returns either this node represents a floating point number or not.
	 */
	public abstract boolean isFloatingPointNumber();

	/**
	 * Returns either this node represents an integral number or not.
	 */
	public abstract boolean isIntegralNumber();

	/**
	 * Returns an integer that represents the generality. For two numbers with generality X and Y, the following should
	 * hold in most occasions: X&lt;Y -&gt; the first number can be converted to the type of the second number without
	 * loss of information.<br/>
	 * <br/>
	 * The five built-in types have the following order:<br/>
	 * {@link IntNode} < {@link LongNode} < {@link BigIntegerNode} < {@link DoubleNode} < {@link DecimalNode}.<br/>
	 * There are obviously cases where the conversion of BigIntegerNode to DoubleNode is not lossless, but that is in
	 * general to be expected when working with DoubleNode instead of DecimalNode.
	 * The standard values are equi-distant from 16 to 80 with a step size of 16 to allow custom numeric types to
	 * seamlessly integrate.
	 * 
	 * @return
	 */
	public byte getGeneralilty();

	/**
	 * Returns the java number.
	 */
	public abstract Number getJavaValue();

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#clone()
	 */
	@Override
	public INumericNode clone();
}