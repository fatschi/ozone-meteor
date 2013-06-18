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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.generic.io.FormatUtil;
import eu.stratosphere.pact.generic.io.SequentialInputFormat;
import eu.stratosphere.pact.generic.io.SequentialOutputFormat;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;

/**
 * Tests {@link InputIterator}.
 * 
 * @author Arvid Heise
 */
public class InputFileIteratorTest {
	/**
	 * Tests if a file iterator of an empty file returns any pairs at all.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@Test
	public void emptyIteratorShouldReturnNoElements() throws IOException {
		InputIterator<PactRecord> inputFileIterator = createFileIterator();

		AssertUtil.assertIteratorEquals("input file iterator is not empty", new ArrayList<PactRecord>().iterator(),
			inputFileIterator, 
			TestRecords.getPactRecordConfig(PactInteger.class, PactString.class));
	}

	/**
	 * Tests if a file iterator of an empty file returns any pairs at all.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@Test
	public void filledIteratorShouldReturnExactlyTheGivenArguments() throws IOException {
		PactRecord[] pairs = { new PactRecord(new PactInteger(1), new PactString("test1")),
			new PactRecord(new PactInteger(2), new PactString("test2")) };
		InputIterator<PactRecord> inputFileIterator = createFileIterator(pairs);

		AssertUtil.assertIteratorEquals("input file iterator does not return the right sequence of pairs", Arrays
			.asList(pairs).iterator(), inputFileIterator, 
			TestRecords.getPactRecordConfig(PactInteger.class, PactString.class));
	}

	/**
	 * Tests if a file iterator of an empty file returns any pairs at all.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@Test
	public void filledIteratorShouldReturnExactlyTheGivenArguments2() throws IOException {
		PactRecord[] pairs = {
			new PactRecord(new PactInteger(1), new PactString("test1")),
			new PactRecord(new PactInteger(2), new PactString("test2")),
			new PactRecord(new PactInteger(3), new PactString("test3")),
			new PactRecord(new PactInteger(4), new PactString("test4")),
			new PactRecord(new PactInteger(5), new PactString("test5")),
			new PactRecord(new PactInteger(6), new PactString("test6")) };
		InputIterator<PactRecord> inputFileIterator = createFileIterator(pairs);

		AssertUtil.assertIteratorEquals("input file iterator does not return the right sequence of pairs", Arrays
			.asList(pairs).iterator(), inputFileIterator, 
			TestRecords.getPactRecordConfig(PactInteger.class, PactString.class));
	}

	/**
	 * Tests if a file iterator of a non-existent file fails.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@Test
	public void emptyIteratorIfInputFileDoesNotExists() throws IOException {
		String testPlanFile = GenericTestPlan.getTestPlanFile("fileIteratorTest");
		@SuppressWarnings("unchecked")
		SequentialInputFormat<PactRecord> inputFormat = FormatUtil.openInput(SequentialInputFormat.class, testPlanFile,
			null);
		InputIterator<PactRecord> inputFileIterator = 
				new InputIterator<PactRecord>(PactRecordSerializerFactory.get().getSerializer(), inputFormat);

		AssertUtil.assertIteratorEquals("input file iterator is not empty",
			new ArrayList<PactRecord>().iterator(),
			inputFileIterator,
			TestRecords.getPactRecordConfig(PactInteger.class, PactString.class));
	}

	/**
	 * Tests if a file iterator of a non-existent file fails.
	 * 
	 * @throws IOException
	 *         if an I/O exception occurred
	 */
	@Test
	public void failIfReadTwoManyItems() throws IOException {
		PactRecord[] pairs = { new PactRecord(new PactInteger(1), new PactString("test1")),
			new PactRecord(new PactInteger(2), new PactString("test2")) };
		InputIterator<PactRecord> inputFileIterator = createFileIterator(pairs);

		while (inputFileIterator.hasNext())
			Assert.assertNotNull(inputFileIterator.next());

		try {
			inputFileIterator.next();
			Assert.fail("should have thrown Exception");
		} catch (NoSuchElementException e) {
		}
	}

	@SuppressWarnings("unchecked")
	private InputIterator<PactRecord> createFileIterator(PactRecord... pairs)
			throws IOException {
		String testPlanFile = GenericTestPlan.getTestPlanFile("fileIteratorTest");
		SequentialOutputFormat output =
			FormatUtil.openOutput(SequentialOutputFormat.class, testPlanFile, null);
		for (PactRecord keyValuePair : pairs)
			output.writeRecord(keyValuePair);
		output.close();
		SequentialInputFormat<PactRecord> inputFormat =
			FormatUtil.openInput(SequentialInputFormat.class, testPlanFile, null);
		InputIterator<PactRecord> inputFileIterator =
			new InputIterator<PactRecord>(PactRecordSerializerFactory.get().getSerializer(), inputFormat);
		return inputFileIterator;
	}
}
