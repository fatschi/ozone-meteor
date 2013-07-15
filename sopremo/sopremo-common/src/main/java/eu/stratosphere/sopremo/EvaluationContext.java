package eu.stratosphere.sopremo;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import com.esotericsoftware.kryo.Kryo;

import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.packages.DefaultConstantRegistry;
import eu.stratosphere.sopremo.packages.DefaultFunctionRegistry;
import eu.stratosphere.sopremo.packages.DefaultTypeRegistry;
import eu.stratosphere.sopremo.packages.EvaluationScope;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;
import eu.stratosphere.sopremo.packages.ITypeRegistry;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

/**
 * Provides additional context to the evaluation of {@link Evaluable}s, such as access to all registered functions.
 * 
 * @author Arvid Heise
 */
public class EvaluationContext extends AbstractSopremoType implements ISopremoType, EvaluationScope {
	private final IFunctionRegistry methodRegistry;

	private final IConstantRegistry constantRegistry;

	private final ITypeRegistry typeRegistry;

	private Path workingPath = new Path(new File(".").toURI().toString());

	private String operatorDescription;

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	// public LinkedList<Operator<?>> getOperatorStack() {
	// return this.operatorStack;
	// }

	private int taskId;

	private final transient Kryo kryo;

	/**
	 * Initializes EvaluationContext.
	 */
	public EvaluationContext() {
		this(new DefaultFunctionRegistry(), new DefaultConstantRegistry(), new DefaultTypeRegistry());

	}

	/**
	 * Initializes EvaluationContext.
	 */
	public EvaluationContext(IFunctionRegistry methodRegistry,
			IConstantRegistry constantRegistry, ITypeRegistry typeRegistry) {
		this.methodRegistry = methodRegistry;
		this.constantRegistry = constantRegistry;
		this.typeRegistry = typeRegistry;

		this.kryo = new Kryo();
		for (Class<? extends IJsonNode> type : TypeCoercer.NUMERIC_TYPES)
			register(type);
		@SuppressWarnings("unchecked")
		List<Class<? extends Object>> defaultTypes =
			Arrays.asList(BooleanNode.class, TextNode.class, IObjectNode.class, IArrayNode.class, NullNode.class,
				MissingNode.class, TreeMap.class, ArrayList.class, BigInteger.class, BigDecimal.class);
		for (Class<?> type : defaultTypes)
			register(type);

		final List<Class<? extends IJsonNode>> types = typeRegistry.getTypes();
		for (Class<? extends IJsonNode> type : types)
			register(type);
		this.kryo.setReferences(false);
	}

	private void register(Class<?> type) {
		this.kryo.register(type);
	}

	/**
	 * Initializes EvaluationContext.
	 */
	public EvaluationContext(EvaluationContext context) {
		this(context.methodRegistry, context.constantRegistry, context.typeRegistry);
		this.copyPropertiesFrom(context);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#getKryo()
	 */
	public Kryo getKryo() {
		return this.kryo;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.SopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		appendable.append("Context @ ").append(this.operatorDescription).append("\n").
			append("Methods: ");
		this.methodRegistry.appendAsString(appendable);
		appendable.append("\nConstants: ");
		this.constantRegistry.appendAsString(appendable);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#clone()
	 */
	@Override
	public EvaluationContext clone() {
		return (EvaluationContext) super.clone();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#shallowClone()
	 */
	@Override
	public EvaluationContext shallowClone() {
		return (EvaluationContext) super.shallowClone();
	}

	// /**
	// * Returns the classResolver.
	// *
	// * @return the classResolver
	// */
	// public ClassResolver getClassResolver() {
	// if(this.classResolver == null)
	// this.classResolver = new SopremoClassResolver(this.getTypeRegistry());
	// return this.classResolver;
	// }
	//
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#copyPropertiesFrom(eu.stratosphere.sopremo.AbstractSopremoType)
	 */
	public void copyPropertiesFrom(ISopremoType original) {
		final EvaluationContext context = (EvaluationContext) original;
		this.resultProjection = context.resultProjection.clone();
		this.operatorDescription = context.operatorDescription;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.RegistryScope#getConstantRegistry()
	 */
	@Override
	public IConstantRegistry getConstantRegistry() {
		return this.constantRegistry;
	}

	/**
	 * Returns the {@link FunctionRegistry} containing all registered function in the current evaluation context.
	 * 
	 * @return the FunctionRegistry
	 */
	@Override
	public IFunctionRegistry getFunctionRegistry() {
		return this.methodRegistry;
	}

	/**
	 * Returns the typeRegistry.
	 * 
	 * @return the typeRegistry
	 */
	@Override
	public ITypeRegistry getTypeRegistry() {
		return this.typeRegistry;
	}

	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	public int getTaskId() {
		return this.taskId;
	}

	/**
	 * Sets the operatorDescription to the specified value.
	 * 
	 * @param operatorDescription
	 *        the operatorDescription to set
	 */
	public void setOperatorDescription(String operatorDescription) {
		if (operatorDescription == null)
			throw new NullPointerException("operatorDescription must not be null");

		this.operatorDescription = operatorDescription;
	}

	/**
	 * Returns the operatorDescription.
	 * 
	 * @return the operatorDescription
	 */
	public String getOperatorDescription() {
		return this.operatorDescription;
	}

	public void setResultProjection(final EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}

	public void setTaskId(final int taskId) {
		this.taskId = taskId;
	}

	/**
	 * Returns the hdfsPath.
	 * 
	 * @return the hdfsPath
	 */
	public Path getWorkingPath() {
		return this.workingPath;
	}

	/**
	 * Sets the hdfsPath to the specified value.
	 * 
	 * @param hdfsPath
	 *        the hdfsPath to set
	 */
	public void setWorkingPath(Path hdfsPath) {
		if (hdfsPath == null)
			throw new NullPointerException("hdfsPath must not be null");

		this.workingPath = hdfsPath;
	}

}
