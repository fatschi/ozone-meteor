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

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;

/**
 * Mocks the {@link TaskManager} without building up any network connections. It supports memory and file channels for
 * an execution graph.
 * 
 * @author Arvid Heise
 */
class TestEnvironment {
	// at least 64 mb
	private static final long MEMORY_SIZE = Runtime.getRuntime().maxMemory() / 2;

	private MemoryManager memoryManager;

	private IOManager ioManager;
	
	private static TestEnvironment Instance = new TestEnvironment();
	
	/**
	 * Returns the instance.
	 * 
	 * @return the instance
	 */
	public static TestEnvironment getInstance() {
		if(Instance == null)
			Instance = new TestEnvironment();
		return Instance;
	}

	private TestEnvironment() {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		// this.memoryManager = new MockMemoryManager();
		// Initialize the io manager
		final String tmpDirPath = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
		this.ioManager = new IOManager(tmpDirPath);
	}

	/**
	 * Returns the ioManager.
	 * 
	 * @return the ioManager
	 */
	public IOManager getIoManager() {
		return this.ioManager;
	}

	/**
	 * Returns the memoryManager.
	 * 
	 * @return the memoryManager
	 */
	public MemoryManager getMemoryManager() {
		return this.memoryManager;
	}
}
