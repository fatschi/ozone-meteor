package eu.stratosphere.sopremo.io;

import java.io.IOException;
import java.net.URI;

import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.util.Equaler;

/**
 * Represents a data source in a PactPlan.
 */
@InputCardinality(0)
public class Source extends ElementaryOperator<Source> {
	private Path inputPath;

	private EvaluationExpression adhocExpression;

	private SopremoFormat format;

	/**
	 * Initializes a Source with the given {@link EvaluationExpression}. This expression serves as the data provider.
	 * 
	 * @param adhocValue
	 *        the expression that should be used
	 */
	public Source(final EvaluationExpression adhocValue) {
		this.adhocExpression = adhocValue;
		this.format = new JsonFormat();
	}

	/**
	 * Initializes a Source with the given {@link FileInputFormat} and the given path.
	 * 
	 * @param inputFormat
	 *        the InputFormat that should be used
	 * @param inputPath
	 *        the path to the input file
	 */
	public Source(final SopremoFormat format, final String inputPath) {
		// check and normalize
		this.inputPath = inputPath == null ? null : new Path(inputPath);
		this.format = format;

		if (format.getInputFormat() == null)
			throw new IllegalArgumentException("given format does not support reading");

		if (this.inputPath != null)
			checkPath();
	}

	/**
	 * Initializes a Source with the given {@link FileInputFormat}.
	 * 
	 * @param inputFormat
	 *        the InputFormat that should be used
	 */
	public Source(final SopremoFormat format) {
		this(format, null);
	}

	/**
	 * Initializes a Source with the given path. This Source uses {@link Source#Source(Class, String)} with the given
	 * path and a {@link JsonInputFormat} to read the data.
	 * 
	 * @param inputPath
	 *        the path to the input file
	 */
	public Source(final String inputPath) {
		this(new JsonFormat(), inputPath);
	}

	/**
	 * Initializes a Source. This Source uses {@link Source#Source(EvaluationExpression)} with an {@link ArrayCreation}.
	 * This means the provided input data of this Source is empty.
	 */
	public Source() {
		this(new ArrayCreation());
	}

	/**
	 * Returns the inputPath.
	 * 
	 * @return the path
	 */
	public String getInputPath() {
		return this.inputPath == null ? null : this.inputPath.toUri().toString();
	}

	/**
	 * Sets the path to the input file.
	 * 
	 * @param inputPath
	 *        the path
	 */
	public void setInputPath(final String inputPath) {
		if (inputPath == null)
			throw new NullPointerException("inputPath must not be null");

		this.adhocExpression = null;
		this.inputPath = new Path(inputPath);
		checkPath();
	}

	/**
	 * 
	 */
	private void checkPath() {
		final URI validURI = this.inputPath.toUri();
		if (validURI.getScheme() == null)
			throw new IllegalStateException(
				"File name of source does not have a valid schema (such as hdfs or file): " + this.inputPath);
	}

	/**
	 * Returns the format.
	 * 
	 * @return the format
	 */
	public SopremoFormat getFormat() {
		return this.format;
	}

	/**
	 * Sets the format to the specified value.
	 * 
	 * @param format
	 *        the format to set
	 */
	@Property(preferred = true)
	public void setFormat(SopremoFormat format) {
		if (format == null)
			throw new NullPointerException("format must not be null");
		if (format.getInputFormat() == null)
			throw new IllegalArgumentException("reading for the given format is not supported");

		this.format = format;
	}

	/**
	 * Sets the adhoc expression of this Source.
	 * 
	 * @param adhocExpression
	 *        the expression that should be used
	 */
	public void setAdhocExpression(final EvaluationExpression adhocExpression) {
		if (adhocExpression == null)
			throw new NullPointerException("adhocExpression must not be null");

		this.inputPath = null;
		this.adhocExpression = adhocExpression;
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context, SopremoRecordLayout layout) {
		final String name = this.getName();
		GenericDataSource<?> contract;
		if (this.isAdhoc()) {
			contract = new GenericDataSource<GeneratorInputFormat>(
				GeneratorInputFormat.class, String.format("Adhoc %s", name));
			SopremoUtil.setObject(contract.getParameters(), GeneratorInputFormat.ADHOC_EXPRESSION_PARAMETER_KEY,
				this.adhocExpression);
		} else {
			contract = new GenericDataSource<InputFormat<?, ?>>(this.format.getInputFormat(), name);
			this.format.configureForInput(contract.getParameters(), this.inputPath);
		}
		final PactModule pactModule = new PactModule(0, 1);
		SopremoUtil.setEvaluationContext(contract.getParameters(), context);
		SopremoUtil.setLayout(contract.getParameters(), layout);
		contract.setDegreeOfParallelism(getDegreeOfParallelism());
		pactModule.getOutput(0).setInput(contract);
		// pactModule.setInput(0, contract);
		return pactModule;
	}

	/**
	 * Determines if this Source is adhoc (read his data from an {@link EvaluationExpression}) or not (read his data
	 * from a file)
	 * 
	 * @return either this Source is adhoc or not
	 */
	public boolean isAdhoc() {
		return this.adhocExpression != null;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final Source other = (Source) obj;
		return Equaler.SafeEquals.equal(this.inputPath, other.inputPath)
			&& Equaler.SafeEquals.equal(this.format, other.format)
			&& Equaler.SafeEquals.equal(this.adhocExpression, other.adhocExpression);
	}

	/**
	 * Returns the adhoc expression of this Source
	 * 
	 * @return the expression
	 */
	public EvaluationExpression getAdhocExpression() {
		return this.adhocExpression;
	}

	/**
	 * If this Source is adhoc ({@link Source#isAdhoc()}) this method evaluates the adhoc expression and returns the
	 * result or throws an exception otherwise.
	 * 
	 * @return the adhoc values
	 */
	public IJsonNode getAdhocValues() {
		if (!this.isAdhoc())
			throw new IllegalStateException();
		return this.getAdhocExpression().evaluate(NullNode.getInstance());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (this.adhocExpression == null ? 0 : this.adhocExpression.hashCode());
		result = prime * result + (this.format == null ? 0 : this.format.hashCode());
		result = prime * result + (this.inputPath == null ? 0 : this.inputPath.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.ElementaryOperator#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("Source [");
		if (this.isAdhoc()) {
			this.adhocExpression.appendAsString(appendable);
		} else {
			appendable.append(this.inputPath.toUri().toString()).append(", ");
			this.format.appendAsString(appendable);
		}
		appendable.append("]");
	}
}
