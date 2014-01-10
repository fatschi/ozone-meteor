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

package eu.stratosphere.sopremo.serialization;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * A factory that create a serializer for the {@link SopremoRecord} data type.
 */
public class SopremoRecordSerializerFactory implements TypeSerializerFactory<SopremoRecord> {

	private SopremoRecordLayout layout;

	// --------------------------------------------------------------------------------------------

	/**
	 * Initializes SopremoRecordSerializerFactory.
	 */
	public SopremoRecordSerializerFactory() {
	}

	public SopremoRecordSerializerFactory(final SopremoRecordLayout layout) {
		if (layout == null)
			throw new NullPointerException();
		this.layout = layout;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeSerializerFactory#getSerializer()
	 */
	@Override
	public TypeSerializer<SopremoRecord> getSerializer() {
		return new SopremoRecordSerializer(this.layout);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.layout.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final SopremoRecordSerializerFactory other = (SopremoRecordSerializerFactory) obj;
		return this.layout.equals(other.layout);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypeSerializerFactory#getDataType()
	 */
	@Override
	public Class<SopremoRecord> getDataType() {
		return SopremoRecord.class;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.api.typeutils.TypeSerializerFactory#writeParametersToConfig(eu.stratosphere.nephele.
	 * configuration.Configuration)
	 */
	@Override
	public void writeParametersToConfig(final Configuration config) {
		SopremoUtil.setObject(config, SopremoRecordLayout.LAYOUT_KEY, this.layout);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.api.typeutils.TypeSerializerFactory#readParametersFromConfig(eu.stratosphere.nephele.
	 * configuration.Configuration, java.lang.ClassLoader)
	 */
	@Override
	public void readParametersFromConfig(final Configuration config, final ClassLoader cl)
			throws ClassNotFoundException {
		this.layout = SopremoUtil.getObject(config, SopremoRecordLayout.LAYOUT_KEY, null);
	}
}
