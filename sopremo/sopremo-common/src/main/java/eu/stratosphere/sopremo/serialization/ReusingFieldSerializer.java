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
package eu.stratosphere.sopremo.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import eu.stratosphere.sopremo.type.ReusingSerializer;

/**
 * @author arvid
 */
public final class ReusingFieldSerializer<T> extends FieldSerializer<T> implements ReusingSerializer<T> {

	/**
	 * Initializes ReusingFieldSerializer.
	 * 
	 * @param kryo
	 * @param type
	 */
	public ReusingFieldSerializer(Kryo kryo, Class<? extends T> type) {
		super(kryo, type);
	}

	private transient T oldInstance;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.ReusingSerializer#read(com.esotericsoftware.kryo.Kryo,
	 * com.esotericsoftware.kryo.io.Input, java.lang.Object, java.lang.Class)
	 */
	@Override
	public T read(Kryo kryo, Input input, T oldInstance, Class<T> type) {
		this.oldInstance = oldInstance;
		return super.read(kryo, input, type);
	}
	
	/* (non-Javadoc)
	 * @see com.esotericsoftware.kryo.serializers.FieldSerializer#read(com.esotericsoftware.kryo.Kryo, com.esotericsoftware.kryo.io.Input, java.lang.Class)
	 */
	@Override
	public T read(Kryo kryo, Input input, Class<T> type) {
		this.oldInstance = null;
		return super.read(kryo, input, type);
	}

	/*
	 * (non-Javadoc)
	 * @see com.esotericsoftware.kryo.serializers.FieldSerializer#create(com.esotericsoftware.kryo.Kryo,
	 * com.esotericsoftware.kryo.io.Input, java.lang.Class)
	 */
	@Override
	protected T create(Kryo kryo, Input input, Class<T> type) {
		if (this.oldInstance != null)
			return this.oldInstance;
		return super.create(kryo, input, type);
	}
}
