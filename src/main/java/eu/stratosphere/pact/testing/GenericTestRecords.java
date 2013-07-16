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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.internal.ArrayComparisonFailure;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.io.FileInputFormat;
import eu.stratosphere.pact.generic.io.FormatUtil;
import eu.stratosphere.pact.generic.io.GenericInputFormat;
import eu.stratosphere.pact.generic.io.SequentialOutputFormat;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypePairComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;

/**
 * Represents the input or output values of a {@link TestPlan}. The class is
 * especially important when setting the expected values in the TestPlan.<br>
 * <br>
 * There are two ways to specify the values:
 * <ol>
 * <li>From a file: with {@link #fromFile(Class, String)} and {@link #fromFile(Class, String, Configuration)} the
 * location, format, and configuration of the data can be specified. The file is lazily loaded and thus can be
 * comparable large.
 * <li>Ad-hoc: key/value records can be added with {@link #add(Key, Value)}, {@link #add(KeyValuePair...)}, and
 * {@link #add(Iterable)}. Please note that the actual amount of records is quite for a test case as the TestPlan
 * already involves a certain degree of overhead.<br>
 * <br>
 * TestPairs are directly comparable with equals and hashCode based on its content. Please note that in the case of
 * large file-based TestPairs, the time needed to compute the {@link #hashCode()} or to compare two instances with
 * {@link #equals(Object)} can become quite long. Currently, the comparison result is order-dependent as TestPairs are
 * interpreted as a list.<br>
 * <br>
 * Currently there is no notion of an empty set of records.
 * 
 * @author Arvid Heise
 * @param <K>
 *        the type of the keys
 * @param <V>
 *        the type of the values
 */
public class GenericTestRecords<T extends Record> implements Closeable, Iterable<T> {
	private final class TestRecordReader implements MutableObjectIterator<T> {
		private final Iterator<T> inputFileIterator;

		private TypeSerializer<T> typeSerializer = GenericTestRecords.this.typeConfig.getTypeSerializer();

		private TestRecordReader(final Iterator<T> inputFileIterator) {
			this.inputFileIterator = inputFileIterator;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.util.MutableObjectIterator#next(java.lang.Object)
		 */
		@Override
		public boolean next(T target) throws IOException {
			if (this.inputFileIterator.hasNext()) {
				this.typeSerializer.copyTo(this.inputFileIterator.next(), target);
				return true;
			}
			return false;
		}

	}

	private final Iterator<T> EMPTY_ITERATOR = new ArrayList<T>().iterator();

	private Configuration configuration;

	private Class<? extends GenericInputFormat<T>> inputFormatClass;

	private final List<T> records = new ArrayList<T>();

	private String path;

	private ClosableManager closableManager = new ClosableManager();

	private boolean empty;

	private TypeConfig<T> typeConfig;

	public GenericTestRecords(TypeConfig<T> typeConfig) {
		this.setTypeConfig(typeConfig);
	}

	/**
	 * Initializes GenericTestRecords.
	 */
	public GenericTestRecords() {
	}

	/**
	 * Sets the typeConfig to the specified value.
	 * 
	 * @param typeConfig
	 *        the typeConfig to set
	 */
	public void setTypeConfig(TypeConfig<T> typeConfig) {
		// if (typeConfig == null)
		// throw new NullPointerException("typeConfig must not be null");

		this.typeConfig = typeConfig;
	}

	/**
	 * Returns the typeConfig.
	 * 
	 * @return the typeConfig
	 */
	public TypeConfig<T> getTypeConfig() {
		return this.typeConfig;
	}

	private boolean isEmpty() {
		return this.empty;
	}

	private void setEmpty(boolean empty) {
		this.empty = empty;
	}

	/**
	 * Specifies that the set of key/value records is empty. This method is primarily used to distinguish between an
	 * empty
	 * uninitialized set and a set deliberately left empty. Further calls to {@link #fromFile(Class, String)} or
	 * {@link #add(Iterable)} will reset the effect of this method invocation and vice-versa.
	 */
	public void setEmpty() {
		this.setEmpty(true);
		this.inputFormatClass = null;
		this.records.clear();
	}

	/**
	 * Adds several records at once.
	 * 
	 * @param records
	 *        the records to add
	 * @return this
	 */
	public GenericTestRecords<T> add(final Iterable<? extends T> records) {
		for (final T record : records)
			this.records.add(record);
		this.setEmpty(false);
		this.inputFormatClass = null;
		return this;
	}

	/**
	 * Adds several records at once.
	 * 
	 * @param records
	 *        the records to add
	 * @return this
	 */
	public GenericTestRecords<T> add(final GenericTestRecords<T> records) {
		if (records.isEmpty())
			this.setEmpty();
		else {
			for (final T record : records)
				this.records.add(record);
			this.setEmpty(false);
			records.close();
		}
		return this;
	}

	/**
	 * Adds several records at once.
	 * 
	 * @param records
	 *        the records to add
	 * @return this
	 */
	public GenericTestRecords<T> add(final T... records) {
		for (final T record : records)
			this.records.add(record);
		this.setEmpty(false);
		return this;
	}

	/**
	 * Returns the records.
	 * 
	 * @return the records
	 */
	protected List<T> getRecords() {
		return this.records;
	}

	/**
	 * Uses {@link UnilateralSortMerger} to sort the files of the {@link SplitInputIterator}.
	 */
	private Iterator<T> createSortedIterator(final Iterator<T> inputFileIterator) {
		int memSize = 10;

		try {
			final StringBuilder testName = new StringBuilder();
			StackTraceElement[] stackTrace = new Throwable().getStackTrace();
			for (int index = 0; index < stackTrace.length; index++)
				if (!stackTrace[index].getClassName().startsWith("eu.stratosphere.pact.testing.")) {
					testName.append(stackTrace[index].toString());
					break;
				}
			// instantiate a sort-merger
			AbstractTask parentTask = new AbstractTask() {
				@Override
				public String toString() {
					return "TestPair Sorter " + testName;
				}

				@Override
				public void registerInputOutput() {
				}

				@Override
				public void invoke() throws Exception {
				}
			};

			final TypeComparator<T> comparator = this.typeConfig.getTypeComparator();
			final TypeSerializer<T> serializer = this.typeConfig.getTypeSerializerFactory().getSerializer();
			final UnilateralSortMerger<T> sortMerger =
				new UnilateralSortMerger<T>(TestEnvironment.getInstance().getMemoryManager(),
					TestEnvironment.getInstance().getIoManager(), new TestRecordReader(inputFileIterator), parentTask,
					serializer, comparator, memSize * 1024L * 1024L, 2, 0.7f);
			this.closableManager.add(sortMerger);

			// obtain and return a grouped iterator from the sort-merger
			return new ImmutableRecordIterator<T>(serializer, sortMerger.getIterator());
		} catch (final MemoryAllocationException mae) {
			throw new RuntimeException(
				"MemoryManager is not able to provide the required amount of memory for ReduceTask", mae);
		} catch (final IOException ioe) {
			throw new RuntimeException("IOException caught when obtaining SortMerger for ReduceTask", ioe);
		} catch (final InterruptedException iex) {
			throw new RuntimeException("InterruptedException caught when obtaining iterator over sorted data.", iex);
		}
	}

	@Override
	public void close() {
		try {
			this.closableManager.close();
		} catch (IOException e) {
		}
	}

	/**
	 * Asserts that the contained set of records is equal to the set of records of the given {@link TestPairs}.
	 * 
	 * @param expectedValues
	 *        the other TestPairs defining the expected result
	 * @throws ArrayComparisonFailure
	 *         if the sets differ
	 */
	public void assertEquals(final GenericTestRecords<T> expectedValues) throws AssertionError {
		new GenericTestRecordsAssertor<T>(expectedValues.getTypeConfig(), expectedValues, this).assertEquals();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		@SuppressWarnings("unchecked")
		final GenericTestRecords<T> other = (GenericTestRecords<T>) obj;

		try {
			other.assertEquals(this);
		} catch (AssertionError e) {
			return false;
		}
		return true;
	}

	/**
	 * Initializes this {@link TestPairs} from the given file.
	 * 
	 * @param inputFormatClass
	 *        the class of the {@link FileInputFormat}
	 * @param file
	 *        the path to the file, can be relative
	 * @return this
	 */
	@SuppressWarnings("rawtypes")
	public GenericTestRecords<T> load(final Class<? extends FileInputFormat> inputFormatClass,
			final String file) {
		this.load(inputFormatClass, file, new Configuration());
		return this;
	}

	/**
	 * Initializes this {@link TestPairs} from the given file.
	 * 
	 * @param inputFormatClass
	 *        the class of the {@link FileInputFormat}
	 * @param file
	 *        the path to the file, can be relative
	 * @param configuration
	 *        the configuration for the {@link FileInputFormat}.
	 * @return this
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public GenericTestRecords<T> load(final Class<? extends FileInputFormat> inputFormatClass,
			final String file, final Configuration configuration) {
		this.path = file;
		this.inputFormatClass = (Class) inputFormatClass;
		this.configuration = configuration;
		this.setEmpty(false);
		this.records.clear();
		return this;
	}

	/**
	 * Initializes this {@link TestPairs} from the given file.
	 * 
	 * @param inputFormatClass
	 *        the class of the {@link FileInputFormat}
	 * @param file
	 *        the path to the file, can be relative
	 * @param configuration
	 *        the configuration for the {@link FileInputFormat}.
	 * @return this
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public GenericTestRecords<T> load(final Class<? extends GenericInputFormat> inputFormatClass,
			final Configuration configuration) {
		this.path = null;
		this.inputFormatClass = (Class<? extends GenericInputFormat<T>>) inputFormatClass;
		this.configuration = configuration;
		this.setEmpty(false);
		this.records.clear();
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		final Iterator<T> iterator = this.iterator();
		while (iterator.hasNext())
			result = prime * result + iterator.next().hashCode();
		return result;
	}

	/**
	 * Returns true if any add method has been called at least one.
	 * 
	 * @return true if records were specified in an ad-hoc manner
	 */
	public boolean isAdhoc() {
		return !this.records.isEmpty();
	}

	/**
	 * Returns true if either records were added manually or with {@link #fromFile(Class, String, Configuration)}.
	 * 
	 * @return true if either records were added manually or with {@link #fromFile(Class, String, Configuration)}.
	 */
	public boolean isInitialized() {
		return this.isEmpty() || !this.records.isEmpty() || this.inputFormatClass != null;
	}

	@Override
	public Iterator<T> iterator() {
		if (this.isEmpty() || !this.isInitialized())
			return this.EMPTY_ITERATOR;

		if (this.typeConfig == null)
			throw new IllegalArgumentException(
				"No type configuration given. Please set default config for the TestPlan with TestPlan#setTypeConfig or specify them when accessing the inputs/outputs");

		if (this.isAdhoc()) {
			final TypePairComparator<T, T> typePairComparator = this.typeConfig.getTypePairComparator();
			Collections.sort(this.records, new Comparator<T>() {
				@Override
				public int compare(T o1, T o2) {
					typePairComparator.setReference(o2);
					return typePairComparator.compareToReference(o1);
				}
			});
			return this.records.iterator();
		}

		if (this.path != null) {
			final InputIterator<T> inputFileIterator = this.getInputFileIterator();

			if (!inputFileIterator.hasNext())
				return inputFileIterator;

			return this.createSortedIterator(inputFileIterator);
		}

		try {
			return this.createSortedIterator(
				new InputIterator<T>(this.typeConfig.getTypeSerializer(),
					FormatUtil.openInput(this.inputFormatClass, this.configuration)));
		} catch (IOException e) {
			Assert.fail("creating input format " + StringUtils.stringifyException(e));
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected InputIterator<T> getInputFileIterator() {

		final InputIterator<T> inputFileIterator;
		try {
			inputFileIterator = new InputIterator<T>(this.typeConfig.getTypeSerializer(),
				FormatUtil.openAllInputs((Class) this.inputFormatClass, this.path, this.configuration));
		} catch (final IOException e) {
			Assert.fail("reading values from " + this.path + ": " + StringUtils.stringifyException(e));
			return null;
		} catch (final Exception e) {
			Assert.fail("creating input format " + StringUtils.stringifyException(e));
			return null;
		}
		return inputFileIterator;
	}

	// protected Iterator<T> getUnsortedIterator() {
	// if (this.isEmpty())
	// return this.EMPTY_ITERATOR;
	// if (this.isAdhoc())
	// return this.records.iterator();
	// if (this.inputFormatClass != null)
	// return this.getInputFileIterator();
	// return this.EMPTY_ITERATOR;
	// }

	/**
	 * Saves the data to the given path in an internal format.
	 * 
	 * @param path
	 *        the path to write to, may be relative
	 * @throws IOException
	 *         if an I/O error occurred
	 */
	public void saveToFile(final String path) throws IOException {
		final SequentialOutputFormat outputFormat = FormatUtil.openOutput(SequentialOutputFormat.class, path, null);

		final Iterator<T> iterator = this.iterator();
		while (iterator.hasNext())
			outputFormat.writeRecord(iterator.next());
		outputFormat.close();
	}

	@Override
	public String toString() {
		final StringBuilder stringBuilder = new StringBuilder("TestRecords: ");
		final Iterator<T> iterator = this.iterator();
		try {
			for (int index = 0; index < 25 && iterator.hasNext(); index++) {
				if (index > 0)
					stringBuilder.append("; ");
				this.typeConfig.getTypeStringifier().appendAsString(stringBuilder, iterator.next());
			}
		} catch (IOException e) {
		}
		if (iterator.hasNext())
			stringBuilder.append("...");
		return stringBuilder.toString();
	}
}