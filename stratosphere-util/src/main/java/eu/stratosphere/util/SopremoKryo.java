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
package eu.stratosphere.util;

import static com.esotericsoftware.kryo.util.Util.className;
import static com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.Opcodes.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;

import org.objenesis.instantiator.ObjectInstantiator;
import org.objenesis.strategy.InstantiatorStrategy;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.util.Util;
import com.esotericsoftware.reflectasm.FieldAccess;
import com.esotericsoftware.reflectasm.MethodAccess;
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassWriter;
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.MethodVisitor;

public class SopremoKryo extends Kryo {
	/**
	 * Returns the best matching serializer for a class. This method can be overridden to implement custom logic to
	 * choose a
	 * serializer.
	 */
	@Override
	@SuppressWarnings({ "rawtypes", "cast" })
	public Serializer getDefaultSerializer(final Class type) {
		if (type == null)
			throw new IllegalArgumentException("type cannot be null.");

		if (!type.isInterface())
			for (Class<?> baseType = type; baseType != Object.class; baseType = baseType.getSuperclass())
				if (baseType.isAnnotationPresent(DefaultSerializer.class))
					return this.newSerializer(
						((DefaultSerializer) baseType.getAnnotation(DefaultSerializer.class)).value(), type);

		return super.getDefaultSerializer(type);
	}

	/**
	 * Returns a new instantiator for creating new instances of the specified type. By default, an instantiator is
	 * returned that
	 * uses reflection if the class has a zero argument constructor, an exception is thrown. If a
	 * {@link #setInstantiatorStrategy(InstantiatorStrategy) strategy} is set, it will be used instead of throwing an
	 * exception.
	 */
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" }) 
	protected ObjectInstantiator newInstantiator(final Class type) {
		if (!Util.isAndroid) {
			// Use ReflectASM if the class is not a non-static member class.
			Class enclosingType = type.getEnclosingClass();
			boolean isNonStaticMemberClass = enclosingType != null && type.isMemberClass()
				&& !Modifier.isStatic(type.getModifiers());
			if (!isNonStaticMemberClass) {
				try {
					final ConstructorAccess access = ConstructorAccess.get(type);
					return new ObjectInstantiator() {
						@Override
						public Object newInstance() {
							try {
								return access.newInstance();
							} catch (Exception ex) {
								throw new KryoException("Error constructing instance of class: " + className(type), ex);
							}
						}
					};
				} catch (Exception ignored) {
				}
			}
		}
		// Reflection.
		try {
			Constructor ctor;
			try {
				ctor = type.getConstructor((Class[]) null);
			} catch (Exception ex) {
				ctor = type.getDeclaredConstructor((Class[]) null);
			}
			ctor.setAccessible(true);
			final Constructor constructor = ctor;
			return new ObjectInstantiator() {
				@Override
				public Object newInstance() {
					try {
						return constructor.newInstance();
					} catch (Exception ex) {
						throw new KryoException("Error constructing instance of class: " + className(type), ex);
					}
				}
			};
		} catch (Exception ignored) {
		}
		return super.newInstantiator(type);
	}
}

@SuppressWarnings({ "rawtypes", "unchecked" }) 
abstract class ConstructorAccess<T> {
	static public <T> ConstructorAccess<T> get(Class<T> type) {
		try {
			type.getConstructor((Class[]) null);
		} catch (Exception ex) {
			if (type.isMemberClass() && !Modifier.isStatic(type.getModifiers()))
				throw new RuntimeException("Class cannot be created (non-static member class): " + type.getName());
			throw new RuntimeException("Class cannot be created (missing no-arg constructor): " + type.getName());
		}

		AccessClassLoader loader = AccessClassLoader.get(type);

		String className = type.getName();
		String accessClassName = className + "ConstructorAccess";
		if (accessClassName.startsWith("java."))
			accessClassName = "reflectasm." + accessClassName;
		Class accessClass = null;
		try {
			accessClass = loader.loadClass(accessClassName);
		} catch (ClassNotFoundException ignored) {
		}
		if (accessClass == null) {
			String accessClassNameInternal = accessClassName.replace('.', '/');
			String classNameInternal = className.replace('.', '/');

			ClassWriter cw = new ClassWriter(0);
			cw.visit(V1_1, ACC_PUBLIC + ACC_SUPER, accessClassNameInternal, null,
				"com/esotericsoftware/reflectasm/ConstructorAccess", null);
			MethodVisitor mv;
			{
				mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
				mv.visitCode();
				mv.visitVarInsn(ALOAD, 0);
				mv.visitMethodInsn(INVOKESPECIAL, "com/esotericsoftware/reflectasm/ConstructorAccess", "<init>", "()V");
				mv.visitInsn(RETURN);
				mv.visitMaxs(1, 1);
				mv.visitEnd();
			}
			// fixed instantiiation for different classloaders
			if (type.getClassLoader() == ClassLoader.getSystemClassLoader()) {
				mv = cw.visitMethod(ACC_PUBLIC, "newInstance", "()Ljava/lang/Object;", null, null);
				mv.visitCode();
				mv.visitTypeInsn(NEW, classNameInternal);
				mv.visitInsn(DUP);
				mv.visitMethodInsn(INVOKESPECIAL, classNameInternal, "<init>", "()V");
				mv.visitInsn(ARETURN);
				mv.visitMaxs(2, 1);
				mv.visitEnd();
			} else  {
				mv = cw.visitMethod(ACC_PUBLIC, "newInstance", "()Ljava/lang/Object;", null, null);
				mv.visitCode();
				mv.visitLdcInsn(type);
				mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Class", "newInstance", "()Ljava/lang/Object;");
				mv.visitInsn(DUP);
				mv.visitMethodInsn(INVOKESPECIAL, classNameInternal, "<init>", "()V");
				mv.visitInsn(ARETURN);
				mv.visitMaxs(2, 1);
				mv.visitEnd();
			}
			cw.visitEnd();
			byte[] data = cw.toByteArray();
			accessClass = loader.defineClass(accessClassName, data);
		}
		try {
			return (ConstructorAccess) accessClass.newInstance();
		} catch (Exception ex) {
			throw new RuntimeException("Error constructing constructor access class: " + accessClassName, ex);
		}
	}

	abstract public T newInstance();
}

@SuppressWarnings({ "rawtypes", "unchecked" }) 
class AccessClassLoader extends ClassLoader {
	static private final ArrayList<AccessClassLoader> accessClassLoaders = new ArrayList();

	static AccessClassLoader get(Class type) {
		ClassLoader parent = type.getClassLoader();
		synchronized (accessClassLoaders) {
			for (int i = 0, n = accessClassLoaders.size(); i < n; i++) {
				AccessClassLoader accessClassLoader = accessClassLoaders.get(i);
				if (accessClassLoader.getParent() == parent)
					return accessClassLoader;
			}
			AccessClassLoader accessClassLoader = new AccessClassLoader(parent);
			accessClassLoaders.add(accessClassLoader);
			return accessClassLoader;
		}
	}

	private AccessClassLoader(ClassLoader parent) {
		super(parent);
	}

	@Override
	protected synchronized java.lang.Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		// These classes come from the classloader that loaded AccessClassLoader.
		if (name.equals(FieldAccess.class.getName()))
			return FieldAccess.class;
		if (name.equals(MethodAccess.class.getName()))
			return MethodAccess.class;
		if (name.equals(ConstructorAccess.class.getName()))
			return ConstructorAccess.class;
		// All other classes come from the classloader that loaded the type we are accessing.
		return super.loadClass(name, resolve);
	}

	Class<?> defineClass(String name, byte[] bytes) throws ClassFormatError {
		try {
			// Attempt to load the access class in the same loader, which makes protected and default access members
			// accessible.
			Method method =
				ClassLoader.class.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class,
					int.class });
			method.setAccessible(true);
			return (Class) method.invoke(getParent(),
				new Object[] { name, bytes, Integer.valueOf(0), Integer.valueOf(bytes.length) });
		} catch (Exception ignored) {
		}
		return defineClass(name, bytes, 0, bytes.length);
	}
}