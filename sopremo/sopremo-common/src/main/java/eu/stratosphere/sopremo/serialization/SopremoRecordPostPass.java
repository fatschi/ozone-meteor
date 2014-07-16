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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import eu.stratosphere.api.common.distributions.DataDistribution;
import eu.stratosphere.api.common.operators.DualInputOperator;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.CompilerPostPassException;
import eu.stratosphere.compiler.dag.MapNode;
import eu.stratosphere.compiler.dag.TempMode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.operators.MapDescriptor;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.NAryUnionPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.compiler.plan.SourcePlanNode;
import eu.stratosphere.compiler.plan.WorksetIterationPlanNode;
import eu.stratosphere.compiler.postpass.ConflictingFieldTypeInfoException;
import eu.stratosphere.compiler.postpass.GenericFlatTypePostPass;
import eu.stratosphere.compiler.postpass.MissingFieldTypeInfoException;
import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.sopremo.packages.ITypeRegistry;
import eu.stratosphere.sopremo.pact.SopremoCoGroupOperator;
import eu.stratosphere.sopremo.pact.SopremoReduceOperator;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 */
public class SopremoRecordPostPass extends GenericFlatTypePostPass<Class<? extends IJsonNode>, SopremoRecordSchema> {

	private SopremoRecordLayout layout;

	private ITypeRegistry typeRegistry;

	public final static boolean PRUNE_LAYOUT = true;

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.compiler.postpass.GenericRecordPostPass#postPass(eu.stratosphere.compiler.dag.candidate
	 * .OptimizedPlan)
	 */
	@Override
	public void postPass(final OptimizedPlan plan) {
		// SchemaCollectingSopremoRecordPostPass schemaCollectingSopremoRecordPostPass = new
		// SchemaCollectingSopremoRecordPostPass();
		// schemaCollectingSopremoRecordPostPass.postPass(plan);
		final PlanWithSopremoPostPass planWithSopremoPostPass = (PlanWithSopremoPostPass) plan.getOriginalPactPlan();
		this.layout = planWithSopremoPostPass.getLayout();
		this.typeRegistry = planWithSopremoPostPass.getTypeRegistry();

		this.removeDummyNodes(plan);
		super.postPass(plan);
		if (PRUNE_LAYOUT)
			this.addIdentityMapsToOutputsWithMultipleChannels(plan);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.compiler.postpass.GenericRecordPostPass#traverse(eu.stratosphere.compiler.dag.candidate
	 * .PlanNode, eu.stratosphere.compiler.postpass.AbstractSchema, boolean)
	 */
	@Override
	protected void traverse(final PlanNode node, final SopremoRecordSchema parentSchema, final boolean createUtilities) {
		// FIXME: workaround for Stratosphere #206
		if (node instanceof SourcePlanNode)
			((SourcePlanNode) node).setSerializer(createSerializer(new SopremoRecordSchema()));
		else if (node instanceof SinkPlanNode)
			this.setOrdering(((SingleInputPlanNode) node).getInput(),
				((GenericDataSink) node.getPactContract()).getLocalOrder());
		else if (node.getPactContract() instanceof SopremoReduceOperator)
			this.setOrdering(((SingleInputPlanNode) node).getInput(),
				((SopremoReduceOperator) node.getPactContract()).getInnerGroupOrder());
		else if (node.getPactContract() instanceof SopremoCoGroupOperator) {
			this.setOrdering(((DualInputPlanNode) node).getInput1(),
				((SopremoCoGroupOperator) node.getPactContract()).getFirstInnerGroupOrdering());
			this.setOrdering(((DualInputPlanNode) node).getInput2(),
				((SopremoCoGroupOperator) node.getPactContract()).getSecondInnerGroupOrdering());
		}
		super.traverse(node, parentSchema, createUtilities);
	}

	private void removeDummyNodes(final OptimizedPlan plan) {
		Collection<SinkPlanNode> dataSinks = plan.getDataSinks();
		for (SinkPlanNode sinkPlanNode : new ArrayList<SinkPlanNode>(dataSinks))
			if (sinkPlanNode.getPlanNode().getPactContract().getUserCodeWrapper().getUserCodeClass() == DevNullOutputFormat.class) {
				dataSinks.remove(sinkPlanNode);
				// should be union node; code is written to be easily performed on other node types, but that could
				// result in unforseeable side-effects
				NAryUnionPlanNode source = (NAryUnionPlanNode) sinkPlanNode.getInput().getSource();
				Iterator<Channel> inputs = source.getInputs();
				while (inputs.hasNext()) {
					Channel channel = inputs.next();
					// here we depend on a modifiable return
					channel.getSource().getOutgoingChannels().remove(channel);
				}
			}
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.compiler.postpass.GenericRecordPostPass#createComparator(eu.stratosphere.util
	 * .FieldList, boolean[], eu.stratosphere.compiler.postpass.AbstractSchema)
	 */
	@Override
	protected TypeComparatorFactory<?> createComparator(final FieldList fields, final boolean[] directions,
			final SopremoRecordSchema schema) {
		if (PRUNE_LAYOUT) {
			final int[] usedKeys = schema.getUsedKeys().toIntArray();
			final int[] sortFields = fields.toArray();

			for (int index = 0; index < sortFields.length; index++)
				if (sortFields[index] != SopremoRecordLayout.VALUE_INDEX)
					sortFields[index] = Arrays.binarySearch(usedKeys, sortFields[index]);
			return new SopremoRecordComparatorFactory(this.layout.project(usedKeys), this.typeRegistry, sortFields, directions);
		}
		return new SopremoRecordComparatorFactory(this.layout, this.typeRegistry, fields.toArray(), directions);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.compiler.postpass.GenericRecordPostPass#createPairComparator(eu.stratosphere.pact.common
	 * .util.FieldList, eu.stratosphere.util.FieldList, boolean[],
	 * eu.stratosphere.compiler.postpass.AbstractSchema, eu.stratosphere.compiler.postpass.AbstractSchema)
	 */
	@Override
	protected TypePairComparatorFactory<?, ?> createPairComparator(final FieldList fields1, final FieldList fields2,
			final boolean[] sortDirections,
			final SopremoRecordSchema schema1, final SopremoRecordSchema schema2) throws MissingFieldTypeInfoException {
		return new SopremoRecordPairComparatorFactory();
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.compiler.postpass.GenericRecordPostPass#createSerializer(eu.stratosphere.compiler.postpass
	 * .AbstractSchema)
	 */
	@Override
	protected TypeSerializerFactory<?> createSerializer(final SopremoRecordSchema schema) {
		if (PRUNE_LAYOUT)
			return new SopremoRecordSerializerFactory(this.layout.project(schema.getUsedKeys().toIntArray()), this.typeRegistry);
		return new SopremoRecordSerializerFactory(this.layout, this.typeRegistry);
	}

	private void addIdentityMapsToOutputsWithMultipleChannels(OptimizedPlan plan) {
		for (PlanNode node : plan.getAllNodes()) {
			if(node instanceof WorksetIterationPlanNode) 
				node = addDummyNode(node, node.getOutgoingChannels(), node.getOutgoingChannels(), 
					((SopremoRecordSerializerFactory)((WorksetIterationPlanNode) node).getSolutionSetSerializer()).getLayout());

			List<Channel> outgoingChannels = node.getOutgoingChannels();
			if (outgoingChannels.size() > 1) {
				ListMultimap<SopremoRecordLayout, Channel> layouts = ArrayListMultimap.create();
				for (Channel channel : outgoingChannels)
					layouts.put(((SopremoRecordSerializerFactory) channel.getSerializer()).getLayout(), channel);

				// we need indeed different layouts; create a dummy map node for each layout
				if (layouts.keySet().size() > 1) {
					// layout to dummy node is empty, so we can ignore all nodes that also require empty layout
					layouts.removeAll(SopremoRecordLayout.EMPTY);

					for (SopremoRecordLayout layout : layouts.keySet()) {
						List<Channel> channelsToBeChanged = layouts.get(layout);
						addDummyNode(node, outgoingChannels, channelsToBeChanged, SopremoRecordLayout.EMPTY);
					}
				}
			}
		}
	}

	/**
	 * @param node
	 * @param outgoingChannels
	 * @param channelsToBeChanged
	 */
	private PlanNode addDummyNode(PlanNode node, List<Channel> outgoingChannels, List<Channel> channelsToBeChanged, SopremoRecordLayout layout) {
		ITypeRegistry registry = ((SopremoRecordSerializerFactory) outgoingChannels.get(0).getSerializer()).getTypeRegistry();
		Channel inMemoryChannel = new Channel(node);
		inMemoryChannel.setShipStrategy(ShipStrategyType.FORWARD);
		inMemoryChannel.setLocalStrategy(LocalStrategy.NONE);

		MapDescriptor mapDescriptor = new MapDescriptor();
		MapNode mapNode = new MapNode(new MapOperatorBase<IdentityMap>(IdentityMap.class, "dummy"));
		mapNode.setDegreeOfParallelism(node.getDegreeOfParallelism());
		mapNode.setSubtasksPerInstance(node.getSubtasksPerInstance());
		SingleInputPlanNode dummyNode = mapDescriptor.instantiate(inMemoryChannel, mapNode);
		inMemoryChannel.setTarget(dummyNode);
		inMemoryChannel.setSerializer(new SopremoRecordSerializerFactory(layout, registry));

		for (Channel originalChannel : channelsToBeChanged) {
			Channel channelWithNewSource = new ForwardingChannel(dummyNode, originalChannel);
			PlanNode target = originalChannel.getTarget();
			if (target instanceof SingleInputPlanNode)
				ReflectUtil.setField(target, "input", channelWithNewSource);
			else if (target instanceof DualInputPlanNode) {
				if (target instanceof WorksetIterationPlanNode) {
					System.out.println(target);
				} else if (((DualInputPlanNode) target).getInput1() == originalChannel)
					ReflectUtil.setField(target, "input1", channelWithNewSource);
				else
					ReflectUtil.setField(target, "input2", channelWithNewSource);
			} else
				throw new UnsupportedOperationException();

			outgoingChannels.set(outgoingChannels.indexOf(originalChannel), inMemoryChannel);
		}
		return dummyNode;
	}

	/**
	 * @author arvid
	 */
	public static class ForwardingChannel extends Channel {
		private Channel originalChannel;

		public ForwardingChannel(SingleInputPlanNode inputNode, Channel originalChannel) {
			super(inputNode);
			this.originalChannel = originalChannel;
		}

		/**
		 * @see eu.stratosphere.compiler.plan.Channel#adjustGlobalPropertiesForFullParallelismChange()
		 */
		@Override
		public void adjustGlobalPropertiesForFullParallelismChange() {
			this.originalChannel.adjustGlobalPropertiesForFullParallelismChange();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getGlobalProperties()
		 */
		@Override
		public GlobalProperties getGlobalProperties() {
			return this.originalChannel.getGlobalProperties();
		}

		/**
		 * @see eu.stratosphere.compiler.plan.Channel#adjustGlobalPropertiesForLocalParallelismChange()
		 */
		@Override
		public void adjustGlobalPropertiesForLocalParallelismChange() {
			this.originalChannel.adjustGlobalPropertiesForLocalParallelismChange();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getDataDistribution()
		 */
		@Override
		public DataDistribution getDataDistribution() {
			return this.originalChannel.getDataDistribution();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getLocalProperties()
		 */
		@Override
		public LocalProperties getLocalProperties() {
			return this.originalChannel.getLocalProperties();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getLocalStrategy()
		 */
		@Override
		public LocalStrategy getLocalStrategy() {
			return this.originalChannel.getLocalStrategy();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getLocalStrategyComparator()
		 */
		@Override
		public TypeComparatorFactory<?> getLocalStrategyComparator() {
			return this.originalChannel.getLocalStrategyComparator();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getLocalStrategyKeys()
		 */
		@Override
		public FieldList getLocalStrategyKeys() {
			return this.originalChannel.getLocalStrategyKeys();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getLocalStrategySortOrder()
		 */
		@Override
		public boolean[] getLocalStrategySortOrder() {
			return this.originalChannel.getLocalStrategySortOrder();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getMemoryGlobalStrategy()
		 */
		@Override
		public long getMemoryGlobalStrategy() {
			return this.originalChannel.getMemoryGlobalStrategy();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getMemoryLocalStrategy()
		 */
		@Override
		public long getMemoryLocalStrategy() {
			return this.originalChannel.getMemoryLocalStrategy();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getReplicationFactor()
		 */
		@Override
		public int getReplicationFactor() {
			return this.originalChannel.getReplicationFactor();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getSerializer()
		 */
		@Override
		public TypeSerializerFactory<?> getSerializer() {
			return this.originalChannel.getSerializer();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getShipStrategy()
		 */
		@Override
		public ShipStrategyType getShipStrategy() {
			return this.originalChannel.getShipStrategy();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getShipStrategyComparator()
		 */
		@Override
		public TypeComparatorFactory<?> getShipStrategyComparator() {
			return this.originalChannel.getShipStrategyComparator();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getShipStrategyKeys()
		 */
		@Override
		public FieldList getShipStrategyKeys() {
			return this.originalChannel.getShipStrategyKeys();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getShipStrategySortOrder()
		 */
		@Override
		public boolean[] getShipStrategySortOrder() {
			return this.originalChannel.getShipStrategySortOrder();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getTarget()
		 */
		@Override
		public PlanNode getTarget() {
			return this.originalChannel.getTarget();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getTempMemory()
		 */
		@Override
		public long getTempMemory() {
			return this.originalChannel.getTempMemory();
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#getTempMode()
		 */
		@Override
		public TempMode getTempMode() {
			return this.originalChannel.getTempMode();
		}

		/**
		 * @param dataDistribution
		 * @see eu.stratosphere.compiler.plan.Channel#setDataDistribution(eu.stratosphere.api.common.distributions.DataDistribution)
		 */
		@Override
		public void setDataDistribution(DataDistribution dataDistribution) {
			this.originalChannel.setDataDistribution(dataDistribution);
		}

		/**
		 * @param strategy
		 * @see eu.stratosphere.compiler.plan.Channel#setLocalStrategy(eu.stratosphere.pact.runtime.task.util.LocalStrategy)
		 */
		@Override
		public void setLocalStrategy(LocalStrategy strategy) {
			this.originalChannel.setLocalStrategy(strategy);
		}

		/**
		 * @param strategy
		 * @param keys
		 * @see eu.stratosphere.compiler.plan.Channel#setLocalStrategy(eu.stratosphere.pact.runtime.task.util.LocalStrategy,
		 *      eu.stratosphere.api.common.operators.util.FieldList)
		 */
		@Override
		public void setLocalStrategy(LocalStrategy strategy, FieldList keys) {
			this.originalChannel.setLocalStrategy(strategy, keys);
		}

		/**
		 * @param strategy
		 * @param keys
		 * @param sortDirection
		 * @see eu.stratosphere.compiler.plan.Channel#setLocalStrategy(eu.stratosphere.pact.runtime.task.util.LocalStrategy,
		 *      eu.stratosphere.api.common.operators.util.FieldList, boolean[])
		 */
		@Override
		public void setLocalStrategy(LocalStrategy strategy, FieldList keys, boolean[] sortDirection) {
			this.originalChannel.setLocalStrategy(strategy, keys, sortDirection);
		}

		/**
		 * @param localStrategyComparator
		 * @see eu.stratosphere.compiler.plan.Channel#setLocalStrategyComparator(eu.stratosphere.api.common.typeutils.TypeComparatorFactory)
		 */
		@Override
		public void setLocalStrategyComparator(TypeComparatorFactory<?> localStrategyComparator) {
			this.originalChannel.setLocalStrategyComparator(localStrategyComparator);
		}

		/**
		 * @param memoryGlobalStrategy
		 * @see eu.stratosphere.compiler.plan.Channel#setMemoryGlobalStrategy(long)
		 */
		@Override
		public void setMemoryGlobalStrategy(long memoryGlobalStrategy) {
			this.originalChannel.setMemoryGlobalStrategy(memoryGlobalStrategy);
		}

		/**
		 * @param memoryLocalStrategy
		 * @see eu.stratosphere.compiler.plan.Channel#setMemoryLocalStrategy(long)
		 */
		@Override
		public void setMemoryLocalStrategy(long memoryLocalStrategy) {
			this.originalChannel.setMemoryLocalStrategy(memoryLocalStrategy);
		}

		/**
		 * @param factor
		 * @see eu.stratosphere.compiler.plan.Channel#setReplicationFactor(int)
		 */
		@Override
		public void setReplicationFactor(int factor) {
			this.originalChannel.setReplicationFactor(factor);
		}

		/**
		 * @param serializer
		 * @see eu.stratosphere.compiler.plan.Channel#setSerializer(eu.stratosphere.api.common.typeutils.TypeSerializerFactory)
		 */
		@Override
		public void setSerializer(TypeSerializerFactory<?> serializer) {
			this.originalChannel.setSerializer(serializer);
		}

		/**
		 * @param strategy
		 * @see eu.stratosphere.compiler.plan.Channel#setShipStrategy(eu.stratosphere.pact.runtime.shipping.ShipStrategyType)
		 */
		@Override
		public void setShipStrategy(ShipStrategyType strategy) {
			this.originalChannel.setShipStrategy(strategy);
		}

		/**
		 * @param strategy
		 * @param keys
		 * @see eu.stratosphere.compiler.plan.Channel#setShipStrategy(eu.stratosphere.pact.runtime.shipping.ShipStrategyType,
		 *      eu.stratosphere.api.common.operators.util.FieldList)
		 */
		@Override
		public void setShipStrategy(ShipStrategyType strategy, FieldList keys) {
			this.originalChannel.setShipStrategy(strategy, keys);
		}

		/**
		 * @param strategy
		 * @param keys
		 * @param sortDirection
		 * @see eu.stratosphere.compiler.plan.Channel#setShipStrategy(eu.stratosphere.pact.runtime.shipping.ShipStrategyType,
		 *      eu.stratosphere.api.common.operators.util.FieldList, boolean[])
		 */
		@Override
		public void setShipStrategy(ShipStrategyType strategy, FieldList keys, boolean[] sortDirection) {
			this.originalChannel.setShipStrategy(strategy, keys, sortDirection);
		}

		/**
		 * @param shipStrategyComparator
		 * @see eu.stratosphere.compiler.plan.Channel#setShipStrategyComparator(eu.stratosphere.api.common.typeutils.TypeComparatorFactory)
		 */
		@Override
		public void setShipStrategyComparator(TypeComparatorFactory<?> shipStrategyComparator) {
			this.originalChannel.setShipStrategyComparator(shipStrategyComparator);
		}

		/**
		 * @param target
		 * @see eu.stratosphere.compiler.plan.Channel#setTarget(eu.stratosphere.compiler.plan.PlanNode)
		 */
		@Override
		public void setTarget(PlanNode target) {
			this.originalChannel.setTarget(target);
		}

		/**
		 * @param tempMemory
		 * @see eu.stratosphere.compiler.plan.Channel#setTempMemory(long)
		 */
		@Override
		public void setTempMemory(long tempMemory) {
			this.originalChannel.setTempMemory(tempMemory);
		}

		/**
		 * @param tempMode
		 * @see eu.stratosphere.compiler.plan.Channel#setTempMode(eu.stratosphere.compiler.dag.TempMode)
		 */
		@Override
		public void setTempMode(TempMode tempMode) {
			this.originalChannel.setTempMode(tempMode);
		}

		/**
		 * @return
		 * @see eu.stratosphere.compiler.plan.Channel#toString()
		 */
		@Override
		public String toString() {
			return "Channel (" + this.getSource() + (this.getTarget() == null ? ')' : ") -> (" + this.getTarget() + ')') +
				'[' + this.getShipStrategy() + "] [" + this.getLocalStrategy() + "] " +
				(this.getTempMode() == null || this.getTempMode() == TempMode.NONE ? "{NO-TEMP}" : this.getTempMode());
		}

	}

	{
		this.setPropagateParentSchemaDown(false);
	}

	private void addOrderingToSchema(final Ordering o, final SopremoRecordSchema schema) {
		for (int i = 0; i < o.getNumberOfFields(); i++)
			schema.add(o.getFieldNumber(i));
	}

	private void setOrdering(final Channel input, final Ordering localOrder) {
		if (localOrder != null) {
			Ordering mergedOrder;
			if (input.getLocalProperties().getOrdering() != null) {
				mergedOrder = input.getLocalProperties().getOrdering().clone();

				final int[] fieldPositions = localOrder.getFieldPositions();
				final Order[] fieldOrders = localOrder.getFieldOrders();
				final IntList coveredFields = new IntArrayList(mergedOrder.getFieldPositions());

				for (int index = 0; index < fieldOrders.length; index++)
					if (!coveredFields.contains(fieldPositions[index]))
						mergedOrder.appendOrdering(fieldPositions[index], null, fieldOrders[index]);
			} else
				mergedOrder = localOrder;
			input.getLocalProperties().setOrdering(mergedOrder);
			input.setLocalStrategy(input.getLocalStrategy(), new FieldList(mergedOrder.getFieldPositions()),
				mergedOrder.getFieldSortDirections());
		}
	}

	@Override
	protected void getDualInputNodeSchema(final DualInputPlanNode node, final SopremoRecordSchema input1Schema,
			final SopremoRecordSchema input2Schema)
	{
		// add the nodes local information. this automatically consistency checks
		final DualInputOperator<?> contract = node.getTwoInputNode().getPactContract();

		final int[] localPositions1 = contract.getKeyColumns(0);
		final int[] localPositions2 = contract.getKeyColumns(1);

		if (localPositions1.length != localPositions2.length)
			throw new CompilerException(
				"Error: The keys for the first and second input have a different number of fields.");

		for (int i = 0; i < localPositions1.length; i++)
			input1Schema.add(localPositions1[i]);
		for (int i = 0; i < localPositions2.length; i++)
			input2Schema.add(localPositions2[i]);

		// this is a temporary fix, we should solve this more generic
		if (contract instanceof SopremoCoGroupOperator) {
			final Ordering groupOrder1 = ((SopremoCoGroupOperator) contract).getFirstInnerGroupOrdering();
			final Ordering groupOrder2 = ((SopremoCoGroupOperator) contract).getSecondInnerGroupOrdering();

			if (groupOrder1 != null)
				this.addOrderingToSchema(groupOrder1, input1Schema);
			if (groupOrder2 != null)
				this.addOrderingToSchema(groupOrder2, input2Schema);
		}
	}

	@Override
	protected void getSingleInputNodeSchema(final SingleInputPlanNode node, final SopremoRecordSchema schema)
			throws CompilerPostPassException, ConflictingFieldTypeInfoException
	{

		// add the information to the schema
		FieldList groupedFields = node.getLocalProperties().getGroupedFields();
		if (groupedFields != null)
			for (int i = 0; i < groupedFields.size(); i++)
				schema.add(groupedFields.get(i));

		FieldList partitioning = node.getGlobalProperties().getPartitioningFields();
		if (partitioning != null)
			for (int i = 0; i < partitioning.size(); i++)
				schema.add(partitioning.get(i));

		SingleInputOperator<?> contract = node.getSingleInputNode().getPactContract();
		int[] keyColumns = contract.getKeyColumns(0);
		for (int index = 0; index < keyColumns.length; index++)
			schema.add(keyColumns[index]);

		// this is a temporary fix, we should solve this more generic
		if (contract instanceof SopremoReduceOperator) {
			final Ordering groupOrder = ((SopremoReduceOperator) contract).getInnerGroupOrder();
			if (groupOrder != null)
				this.addOrderingToSchema(groupOrder, schema);
		}
	}

	@Override
	protected void getSinkSchema(final SinkPlanNode sinkPlanNode, final SopremoRecordSchema schema)
			throws CompilerPostPassException {
		final GenericDataSink sink = sinkPlanNode.getSinkNode().getPactContract();
		final Ordering partitioning = sink.getPartitionOrdering();
		final Ordering sorting = sink.getLocalOrder();

		if (partitioning != null)
			this.addOrderingToSchema(partitioning, schema);
		if (sorting != null)
			this.addOrderingToSchema(sorting, schema);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.compiler.postpass.GenericRecordPostPass#createEmptySchema()
	 */
	@Override
	protected SopremoRecordSchema createEmptySchema() {
		return new SopremoRecordSchema();
	}
}