/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.sort.FloatBucketedSort;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Aggregates the top N field values for float.
 * <p>
 *     This class is generated. Edit `X-TopAggregator.java.st` to edit this file.
 * </p>
 */
@Aggregator({ @IntermediateState(name = "top", type = "FLOAT_BLOCK") })
@GroupingAggregator
class TopFloatAggregator {
    public static SingleState initSingle(BigArrays bigArrays, int limit, boolean ascending) {
        return new SingleState(bigArrays, limit, ascending);
    }

    public static void combine(SingleState state, float v) {
        state.add(v);
    }

    public static void combineIntermediate(SingleState state, FloatBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getFloat(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays, int limit, boolean ascending) {
        return new GroupingState(bigArrays, limit, ascending);
    }

    public static void combine(GroupingState state, int groupId, float v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(GroupingState state, int groupId, FloatBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getFloat(i));
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class GroupingState implements GroupingAggregatorState {
        private final FloatBucketedSort sort;

        private GroupingState(BigArrays bigArrays, int limit, boolean ascending) {
            this.sort = new FloatBucketedSort(bigArrays, ascending ? SortOrder.ASC : SortOrder.DESC, limit);
        }

        public void add(int groupId, float value) {
            sort.collect(value, groupId);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            return sort.toBlock(blockFactory, selected);
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(sort);
        }
    }

    public static class SingleState implements AggregatorState {
        private final GroupingState internalState;

        private SingleState(BigArrays bigArrays, int limit, boolean ascending) {
            this.internalState = new GroupingState(bigArrays, limit, ascending);
        }

        public void add(float value) {
            internalState.add(0, value);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            try (var intValues = blockFactory.newConstantIntVector(0, 1)) {
                return internalState.toBlock(blockFactory, intValues);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(internalState);
        }
    }
}
