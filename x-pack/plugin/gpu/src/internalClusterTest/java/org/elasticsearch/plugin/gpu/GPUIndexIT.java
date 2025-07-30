/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.plugin.gpu;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.gpu.GPUPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@LuceneTestCase.SuppressCodecs("*") // use our custom codec
public class GPUIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GPUPlugin.class);
    }

    public void testBasic() {
        final int dims = randomIntBetween(4, 128);
        final int[] numDocs = new int[] { randomIntBetween(1, 100), 1, 2, randomIntBetween(1, 100) };

        var settings = Settings.builder().put(indexSettings());
        settings.put("index.number_of_shards", 1);
        settings.put("index.vectors.indexing.use_gpu", true);
        assertAcked(prepareCreate("foo-index").setSettings(settings.build()).setMapping(String.format(Locale.ROOT, """
                {
                  "properties": {
                    "my_vector": {
                      "type": "dense_vector",
                      "dims": %d,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "hnsw"
                      }
                    }
                  }
                }
            """, dims)));
        ensureGreen();

        for (int i = 0; i < numDocs.length; i++) {
            BulkRequestBuilder bulkRequest = client().prepareBulk();
            for (int j = 0; j < numDocs[i]; j++) {
                String id = String.valueOf(i * 100 + j);
                bulkRequest.add(prepareIndex("foo-index").setId(id).setSource("my_vector", randomFloatVector(dims)));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            assertFalse("Bulk request failed: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
        }
        refresh();

        float[] queryVector = randomFloatVector(dims);
        int k = randomIntBetween(1, 10);
        int numCandidates = k * 10;
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.knnSearch(List.of(new KnnSearchBuilder("my_vector", queryVector, k, numCandidates, null, null)));
        SearchRequest searchRequest = new SearchRequest("foo-index");
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertEquals("Search should execute successfully on all shards", 0, searchResponse.getFailedShards());
        assertEquals("Expected k hits to be returned", k, searchResponse.getHits().getHits().length);
    }

    private static float[] randomFloatVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
