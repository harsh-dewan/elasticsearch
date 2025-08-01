/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Base class for IVF vectors writer.
 */
public abstract class IVFVectorsWriter extends KnnVectorsWriter {

    private final List<FieldWriter> fieldWriters = new ArrayList<>();
    private final IndexOutput ivfCentroids, ivfClusters;
    private final IndexOutput ivfMeta;
    private final FlatVectorsWriter rawVectorDelegate;

    @SuppressWarnings("this-escape")
    protected IVFVectorsWriter(SegmentWriteState state, FlatVectorsWriter rawVectorDelegate) throws IOException {
        this.rawVectorDelegate = rawVectorDelegate;
        final String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            IVFVectorsFormat.IVF_META_EXTENSION
        );

        final String ivfCentroidsFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            IVFVectorsFormat.CENTROID_EXTENSION
        );
        final String ivfClustersFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            IVFVectorsFormat.CLUSTER_EXTENSION
        );
        boolean success = false;
        try {
            ivfMeta = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(
                ivfMeta,
                IVFVectorsFormat.NAME,
                IVFVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            ivfCentroids = state.directory.createOutput(ivfCentroidsFileName, state.context);
            CodecUtil.writeIndexHeader(
                ivfCentroids,
                IVFVectorsFormat.NAME,
                IVFVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            ivfClusters = state.directory.createOutput(ivfClustersFileName, state.context);
            CodecUtil.writeIndexHeader(
                ivfClusters,
                IVFVectorsFormat.NAME,
                IVFVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public final KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        if (fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE) {
            throw new IllegalArgumentException("IVF does not support cosine similarity");
        }
        final FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            @SuppressWarnings("unchecked")
            final FlatFieldVectorsWriter<float[]> floatWriter = (FlatFieldVectorsWriter<float[]>) rawVectorDelegate;
            fieldWriters.add(new FieldWriter(fieldInfo, floatWriter));
        }
        return rawVectorDelegate;
    }

    abstract CentroidAssignments calculateCentroids(FieldInfo fieldInfo, FloatVectorValues floatVectorValues, float[] globalCentroid)
        throws IOException;

    abstract void writeCentroids(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        LongValues centroidOffset,
        IndexOutput centroidOutput
    ) throws IOException;

    abstract LongValues buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException;

    abstract LongValues buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        MergeState mergeState,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException;

    abstract CentroidSupplier createCentroidSupplier(
        IndexInput centroidsInput,
        int numCentroids,
        FieldInfo fieldInfo,
        float[] globalCentroid
    ) throws IOException;

    @Override
    public final void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        rawVectorDelegate.flush(maxDoc, sortMap);
        for (FieldWriter fieldWriter : fieldWriters) {
            final float[] globalCentroid = new float[fieldWriter.fieldInfo.getVectorDimension()];
            // build a float vector values with random access
            final FloatVectorValues floatVectorValues = getFloatVectorValues(fieldWriter.fieldInfo, fieldWriter.delegate, maxDoc);
            // build centroids
            final CentroidAssignments centroidAssignments = calculateCentroids(fieldWriter.fieldInfo, floatVectorValues, globalCentroid);
            // wrap centroids with a supplier
            final CentroidSupplier centroidSupplier = new OnHeapCentroidSupplier(centroidAssignments.centroids());
            // write posting lists
            final LongValues offsets = buildAndWritePostingsLists(
                fieldWriter.fieldInfo,
                centroidSupplier,
                floatVectorValues,
                ivfClusters,
                centroidAssignments.assignments(),
                centroidAssignments.overspillAssignments()
            );
            // write centroids
            final long centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
            writeCentroids(fieldWriter.fieldInfo, centroidSupplier, globalCentroid, offsets, ivfCentroids);
            final long centroidLength = ivfCentroids.getFilePointer() - centroidOffset;
            // write meta file
            writeMeta(fieldWriter.fieldInfo, centroidSupplier.size(), centroidOffset, centroidLength, globalCentroid);
        }
    }

    private static FloatVectorValues getFloatVectorValues(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<float[]> fieldVectorsWriter,
        int maxDoc
    ) throws IOException {
        List<float[]> vectors = fieldVectorsWriter.getVectors();
        if (vectors.size() == maxDoc) {
            return FloatVectorValues.fromFloats(vectors, fieldInfo.getVectorDimension());
        }
        final DocIdSetIterator iterator = fieldVectorsWriter.getDocsWithFieldSet().iterator();
        final int[] docIds = new int[vectors.size()];
        for (int i = 0; i < docIds.length; i++) {
            docIds[i] = iterator.nextDoc();
        }
        assert iterator.nextDoc() == NO_MORE_DOCS;
        return new FloatVectorValues() {
            @Override
            public float[] vectorValue(int ord) {
                return vectors.get(ord);
            }

            @Override
            public FloatVectorValues copy() {
                return this;
            }

            @Override
            public int dimension() {
                return fieldInfo.getVectorDimension();
            }

            @Override
            public int size() {
                return vectors.size();
            }

            @Override
            public int ordToDoc(int ord) {
                return docIds[ord];
            }
        };
    }

    @Override
    public final void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            mergeOneFieldIVF(fieldInfo, mergeState);
        }
        // we merge the vectors at the end so we only have two copies of the vectors on disk at the same time.
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
    }

    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    private void mergeOneFieldIVF(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        final int numVectors;
        String tempRawVectorsFileName = null;
        String docsFileName = null;
        boolean success = false;
        // build a float vector values with random access. In order to do that we dump the vectors to
        // a temporary file and if the segment is not dense, the docs to another file/
        try (
            IndexOutput vectorsOut = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfvec_", IOContext.DEFAULT)
        ) {
            tempRawVectorsFileName = vectorsOut.getName();
            FloatVectorValues mergedFloatVectorValues = MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
            // if the segment is dense, we don't need to do anything with docIds.
            boolean dense = mergedFloatVectorValues.size() == mergeState.segmentInfo.maxDoc();
            try (
                IndexOutput docsOut = dense
                    ? null
                    : mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfdoc_", IOContext.DEFAULT)
            ) {
                if (docsOut != null) {
                    docsFileName = docsOut.getName();
                }
                // TODO do this better, we shouldn't have to write to a temp file, we should be able to
                // to just from the merged vector values, the tricky part is the random access.
                numVectors = writeFloatVectorValues(fieldInfo, docsOut, vectorsOut, mergedFloatVectorValues);
                CodecUtil.writeFooter(vectorsOut);
                if (docsOut != null) {
                    CodecUtil.writeFooter(docsOut);
                }
                success = true;
            }
        } finally {
            if (success == false) {
                if (tempRawVectorsFileName != null) {
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, tempRawVectorsFileName);
                }
                if (docsFileName != null) {
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, docsFileName);
                }
            }
        }
        try (
            IndexInput vectors = mergeState.segmentInfo.dir.openInput(tempRawVectorsFileName, IOContext.DEFAULT);
            IndexInput docs = docsFileName == null ? null : mergeState.segmentInfo.dir.openInput(docsFileName, IOContext.DEFAULT)
        ) {
            final FloatVectorValues floatVectorValues = getFloatVectorValues(fieldInfo, docs, vectors, numVectors);

            final long centroidOffset;
            final long centroidLength;
            final int numCentroids;
            final int[] assignments;
            final int[] overspillAssignments;
            final float[] calculatedGlobalCentroid = new float[fieldInfo.getVectorDimension()];
            String centroidTempName = null;
            IndexOutput centroidTemp = null;
            success = false;
            try {
                centroidTemp = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "civf_", IOContext.DEFAULT);
                centroidTempName = centroidTemp.getName();
                CentroidAssignments centroidAssignments = calculateCentroids(
                    fieldInfo,
                    getFloatVectorValues(fieldInfo, docs, vectors, numVectors),
                    calculatedGlobalCentroid
                );
                // write the centroids to a temporary file so we are not holding them on heap
                final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                for (float[] centroid : centroidAssignments.centroids()) {
                    buffer.asFloatBuffer().put(centroid);
                    centroidTemp.writeBytes(buffer.array(), buffer.array().length);
                }
                numCentroids = centroidAssignments.numCentroids();
                assignments = centroidAssignments.assignments();
                overspillAssignments = centroidAssignments.overspillAssignments();
                success = true;
            } finally {
                if (success == false && centroidTempName != null) {
                    IOUtils.closeWhileHandlingException(centroidTemp);
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, centroidTempName);
                }
            }
            try {
                if (numCentroids == 0) {
                    centroidOffset = ivfCentroids.getFilePointer();
                    writeMeta(fieldInfo, 0, centroidOffset, 0, null);
                    CodecUtil.writeFooter(centroidTemp);
                    IOUtils.close(centroidTemp);
                    return;
                }
                CodecUtil.writeFooter(centroidTemp);
                IOUtils.close(centroidTemp);

                try (IndexInput centroidsInput = mergeState.segmentInfo.dir.openInput(centroidTempName, IOContext.DEFAULT)) {
                    CentroidSupplier centroidSupplier = createCentroidSupplier(
                        centroidsInput,
                        numCentroids,
                        fieldInfo,
                        calculatedGlobalCentroid
                    );
                    // write posting lists
                    final LongValues offsets = buildAndWritePostingsLists(
                        fieldInfo,
                        centroidSupplier,
                        floatVectorValues,
                        ivfClusters,
                        mergeState,
                        assignments,
                        overspillAssignments
                    );
                    // write centroids
                    centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
                    writeCentroids(fieldInfo, centroidSupplier, calculatedGlobalCentroid, offsets, ivfCentroids);
                    centroidLength = ivfCentroids.getFilePointer() - centroidOffset;
                    // write meta
                    writeMeta(fieldInfo, centroidSupplier.size(), centroidOffset, centroidLength, calculatedGlobalCentroid);
                }
            } finally {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, centroidTempName);
            }
        } finally {
            if (docsFileName != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(
                    mergeState.segmentInfo.dir,
                    tempRawVectorsFileName,
                    docsFileName
                );
            } else {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, tempRawVectorsFileName);
            }
        }
    }

    private static FloatVectorValues getFloatVectorValues(FieldInfo fieldInfo, IndexInput docs, IndexInput vectors, int numVectors)
        throws IOException {
        if (numVectors == 0) {
            return FloatVectorValues.fromFloats(List.of(), fieldInfo.getVectorDimension());
        }
        final long vectorLength = (long) Float.BYTES * fieldInfo.getVectorDimension();
        final float[] vector = new float[fieldInfo.getVectorDimension()];
        final RandomAccessInput randomDocs = docs == null ? null : docs.randomAccessSlice(0, docs.length());
        return new FloatVectorValues() {
            @Override
            public float[] vectorValue(int ord) throws IOException {
                vectors.seek(ord * vectorLength);
                vectors.readFloats(vector, 0, vector.length);
                return vector;
            }

            @Override
            public FloatVectorValues copy() {
                return this;
            }

            @Override
            public int dimension() {
                return fieldInfo.getVectorDimension();
            }

            @Override
            public int size() {
                return numVectors;
            }

            @Override
            public int ordToDoc(int ord) {
                if (randomDocs == null) {
                    return ord;
                }
                try {
                    return randomDocs.readInt((long) ord * Integer.BYTES);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    private static int writeFloatVectorValues(
        FieldInfo fieldInfo,
        IndexOutput docsOut,
        IndexOutput vectorsOut,
        FloatVectorValues floatVectorValues
    ) throws IOException {
        int numVectors = 0;
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        final KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            numVectors++;
            buffer.asFloatBuffer().put(floatVectorValues.vectorValue(iterator.index()));
            vectorsOut.writeBytes(buffer.array(), buffer.array().length);
            if (docsOut != null) {
                docsOut.writeInt(iterator.docID());
            }
        }
        return numVectors;
    }

    private void writeMeta(FieldInfo field, int numCentroids, long centroidOffset, long centroidLength, float[] globalCentroid)
        throws IOException {
        ivfMeta.writeInt(field.number);
        ivfMeta.writeInt(field.getVectorEncoding().ordinal());
        ivfMeta.writeInt(distFuncToOrd(field.getVectorSimilarityFunction()));
        ivfMeta.writeInt(numCentroids);
        ivfMeta.writeLong(centroidOffset);
        ivfMeta.writeLong(centroidLength);
        if (centroidLength > 0) {
            final ByteBuffer buffer = ByteBuffer.allocate(globalCentroid.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.asFloatBuffer().put(globalCentroid);
            ivfMeta.writeBytes(buffer.array(), buffer.array().length);
            ivfMeta.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(globalCentroid, globalCentroid)));
        }
    }

    private static int distFuncToOrd(VectorSimilarityFunction func) {
        for (int i = 0; i < SIMILARITY_FUNCTIONS.size(); i++) {
            if (SIMILARITY_FUNCTIONS.get(i).equals(func)) {
                return (byte) i;
            }
        }
        throw new IllegalArgumentException("invalid distance function: " + func);
    }

    @Override
    public final void finish() throws IOException {
        rawVectorDelegate.finish();
        if (ivfMeta != null) {
            // write end of fields marker
            ivfMeta.writeInt(-1);
            CodecUtil.writeFooter(ivfMeta);
        }
        if (ivfCentroids != null) {
            CodecUtil.writeFooter(ivfCentroids);
        }
        if (ivfClusters != null) {
            CodecUtil.writeFooter(ivfClusters);
        }
    }

    @Override
    public final void close() throws IOException {
        IOUtils.close(rawVectorDelegate, ivfMeta, ivfCentroids, ivfClusters);
    }

    @Override
    public final long ramBytesUsed() {
        return rawVectorDelegate.ramBytesUsed();
    }

    private record FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> delegate) {}

    interface CentroidSupplier {
        CentroidSupplier EMPTY = new CentroidSupplier() {
            @Override
            public int size() {
                return 0;
            }

            @Override
            public float[] centroid(int centroidOrdinal) {
                throw new IllegalStateException("No centroids");
            }
        };

        int size();

        float[] centroid(int centroidOrdinal) throws IOException;
    }

    // TODO throw away rawCentroids
    static class OnHeapCentroidSupplier implements CentroidSupplier {
        private final float[][] centroids;

        OnHeapCentroidSupplier(float[][] centroids) {
            this.centroids = centroids;
        }

        @Override
        public int size() {
            return centroids.length;
        }

        @Override
        public float[] centroid(int centroidOrdinal) throws IOException {
            return centroids[centroidOrdinal];
        }
    }
}
