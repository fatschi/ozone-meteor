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
package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import eu.stratosphere.sopremo.expressions.tree.ChildIterator;
import eu.stratosphere.sopremo.expressions.tree.NamedChildIterator;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Determines a set contains an element or not.
 */
@OptimizerHints(scope = Scope.ANY, iterating = true)
public class ElementInSetExpression extends BinaryBooleanExpression {
	private EvaluationExpression elementExpr;

	private EvaluationExpression setExpr;

	private final Quantor quantor;

	/**
	 * Initializes an ElementInSetExpression.
	 * 
	 * @param elementExpr
	 *        the expression which evaluates to the element that should be found
	 * @param quantor
	 *        the {@link Quantor} that should be used
	 * @param setExpr
	 *        the expression which evaluates to the set that should be used
	 */
	public ElementInSetExpression(final EvaluationExpression elementExpr, final Quantor quantor,
			final EvaluationExpression setExpr) {
		this.elementExpr = elementExpr;
		this.setExpr = setExpr;
		this.quantor = quantor;
	}

	/**
	 * Initializes ElementInSetExpression.
	 */
	ElementInSetExpression() {
		this.elementExpr = null;
		this.setExpr = null;
		this.quantor = null;
	}

	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		this.elementExpr.appendAsString(appendable);
		appendable.append(this.quantor == Quantor.EXISTS_NOT_IN ? " \u2209 " : " \u2208 ");
		this.setExpr.appendAsString(appendable);
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ElementInSetExpression other = (ElementInSetExpression) obj;
		return this.quantor == other.quantor
			&& this.elementExpr.equals(other.elementExpr)
			&& this.setExpr.equals(other.setExpr);
	}

	// @Override
	// public Iterator<IJsonNode> evaluate(Iterator<IJsonNode>... inputs) {
	// return new AbstractIterator<IJsonNode>() {
	// @Override
	// protected IJsonNode loadNext() {
	// return isIn(elementExpr.evaluate(inputs[0].e).next(), setExpr.evaluate(inputs)) != notIn ? BooleanNode.TRUE
	// : BooleanNode.FALSE;
	// ;
	// }
	// };
	//
	// }
	//
	// @Override
	// public Iterator<IJsonNode> evaluate(Iterator<IJsonNode> input) {
	// return super.evaluate(input);
	// }

	@Override
	public BooleanNode evaluate(final IJsonNode node) {
		// we can ignore 'target' because no new Object is created
		return this.quantor.evaluate(this.elementExpr.evaluate(node),
			ElementInSetExpression.asIterator(this.setExpr.evaluate(node)));
	}

	/**
	 * Returns the element expression.
	 * 
	 * @return the element expression
	 */
	public EvaluationExpression getElementExpr() {
		return this.elementExpr;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.BinaryBooleanExpression#getExpr1()
	 */
	@Override
	public EvaluationExpression getExpr1() {
		return this.elementExpr;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.BinaryBooleanExpression#getExpr2()
	 */
	@Override
	public EvaluationExpression getExpr2() {
		return this.setExpr;
	}

	/**
	 * Returns the quantor.
	 * 
	 * @return the quantor
	 */
	public Quantor getQuantor() {
		return this.quantor;
	}

	/**
	 * Returns the set expression.
	 * 
	 * @return the set expression
	 */
	public EvaluationExpression getSetExpr() {
		return this.setExpr;
	}

	//
	// @Override
	// public IJsonNode evaluate(IJsonNode... nodes) {
	// return quantor.evaluate(this.elementExpr.evaluate(nodes), this.asIterator(this.setExpr.evaluate(nodes)));
	// }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.elementExpr.hashCode();
		result = prime * result + this.quantor.hashCode();
		result = prime * result + this.setExpr.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ExpressionParent#iterator()
	 */
	@Override
	public ChildIterator iterator() {
		return new NamedChildIterator("elementExpr", "setExpr") {

			@Override
			protected EvaluationExpression get(final int index) {
				if (index == 0)
					return ElementInSetExpression.this.elementExpr;
				return ElementInSetExpression.this.setExpr;
			}

			@Override
			protected void set(final int index, final EvaluationExpression childExpression) {
				if (index == 0)
					ElementInSetExpression.this.elementExpr = childExpression;
				else
					ElementInSetExpression.this.setExpr = childExpression;
			}
		};
	}

	@SuppressWarnings("unchecked")
	static Iterator<IJsonNode> asIterator(final IJsonNode evaluate) {
		if (evaluate instanceof IArrayNode<?>)
			return ((IArrayNode<IJsonNode>) evaluate).iterator();
		return Arrays.asList(evaluate).iterator();
	}

	/**
	 * All supported quantors.
	 */
	public static enum Quantor {
		EXISTS_IN, EXISTS_NOT_IN {
			@Override
			protected BooleanNode evaluate(final IJsonNode element, final Iterator<IJsonNode> set) {
				return super.evaluate(element, set).negate();
			}
		};

		protected BooleanNode evaluate(final IJsonNode element, final Iterator<IJsonNode> set) {
			while (set.hasNext())
				if (element.equals(set.next()))
					return BooleanNode.TRUE;
			return BooleanNode.FALSE;
		}
	}

}