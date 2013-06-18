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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Collects other {@link java.io.Closeable}s and closes them once. Instances can be used further after a call to
 * {@link #close()}.
 * 
 * @author Arvid.Heise
 */
public class ClosableManager implements Closeable {
	private Queue<Closeable> closeables = new LinkedList<Closeable>();

	@Override
	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}

	@Override
	public synchronized void close() throws IOException {
		List<IOException> exceptions = null;

		while(!this.closeables.isEmpty())
			try {
				this.closeables.poll().close();
			} catch (IOException e) {
				if (exceptions == null)
					exceptions = new ArrayList<IOException>();
				exceptions.add(e);
			}
		this.closeables.clear();

		if (exceptions != null)
			throw new IOException("exception(s) while closing: " + exceptions);
	}

	/**
	 * Adds a new {@link Closeable}.
	 * 
	 * @param closeable
	 *        the closable to add
	 */
	public synchronized void add(Closeable closeable) {
		this.closeables.add(closeable);
	}
}
