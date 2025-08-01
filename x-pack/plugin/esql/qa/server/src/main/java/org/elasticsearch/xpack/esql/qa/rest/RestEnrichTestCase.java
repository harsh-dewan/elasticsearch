/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.Mode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public abstract class RestEnrichTestCase extends ESRestTestCase {

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    private static final String sourceIndexName = "countries";
    private static final String policyName = "countries";

    protected final Mode mode;

    @ParametersFactory
    public static List<Object[]> modes() {
        return Arrays.stream(Mode.values()).map(m -> new Object[] { m }).toList();
    }

    protected RestEnrichTestCase(Mode mode) {
        this.mode = mode;
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    @Before
    public void loadTestData() throws IOException {
        Request request = new Request("PUT", "/test1");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "geo.dest": {
                    "type": "keyword"
                  },
                  "number": {
                    "type": "long"
                  }
                }
              }
            }""");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/test1/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity("""
            { "index": {"_id": 1} }
            { "geo.dest": "US", "number": 1000 }
            { "index": {"_id": 2} }
            { "geo.dest": "US", "number": 1000 }
            { "index": {"_id": 3} }
            { "geo.dest": "CN", "number": 5000 }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("PUT", "/test2");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "geo.dest": {
                    "type": "keyword"
                  },
                  "country_number": {
                    "type": "long"
                  }
                }
              }
            }""");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/test2/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity("""
            { "index": {"_id": 1} }
            { "geo.dest": "IN", "country_number": 2 }
            { "index": {"_id": 2} }
            { "geo.dest": "IN", "country_number": 2 }
            { "index": {"_id": 3} }
            { "geo.dest": "US", "country_number": 3 }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("PUT", "/" + sourceIndexName);
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "geo.dest": {
                    "type": "keyword"
                  },
                  "country_name": {
                    "type": "keyword"
                  }
                }
              }
            }""");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/" + sourceIndexName + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity("""
            { "index" : {}}
            { "geo.dest": "US", "country_name": "United States of America" }
            { "index" : {}}
            { "geo.dest": "IN", "country_name": "India" }
            { "index" : {}}
            { "geo.dest": "CN", "country_name": "China" }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("PUT", "/_enrich/policy/" + policyName);
        request.setJsonEntity("""
            {
              "match": {
                "indices": "countries",
                "match_field": "geo.dest",
                "enrich_fields": ["country_name"]
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("PUT", "/_enrich/policy/" + policyName + "/_execute");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    @After
    public void wipeTestData() throws IOException {
        try {
            var response = client().performRequest(new Request("DELETE", "/test1,test2"));
            assertEquals(200, response.getStatusLine().getStatusCode());
            response = client().performRequest(new Request("DELETE", "/" + sourceIndexName));
            assertEquals(200, response.getStatusLine().getStatusCode());
            response = client().performRequest(new Request("DELETE", "/_enrich/policy/" + policyName));
            assertEquals(200, response.getStatusLine().getStatusCode());
        } catch (ResponseException re) {
            assertEquals(404, re.getResponse().getStatusLine().getStatusCode());
        }
    }

    public void testNonExistentEnrichPolicy() throws IOException {
        ResponseException re = expectThrows(ResponseException.class, () -> runEsql("from test1 | enrich countris", null, Mode.SYNC));
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("cannot find enrich policy [countris], did you mean [countries]?")
        );
    }

    public void testNonExistentEnrichPolicy_KeepField() throws IOException {
        ResponseException re = expectThrows(
            ResponseException.class,
            () -> runEsql("from test1 | enrich countris | keep number", null, Mode.SYNC)
        );
        assertThat(
            EntityUtils.toString(re.getResponse().getEntity()),
            containsString("cannot find enrich policy [countris], did you mean [countries]?")
        );
    }

    public void testMatchField_ImplicitFieldsList() throws IOException {
        Map<String, Object> result = runEsql("from test1 | enrich countries | keep number | sort number");
        var columns = List.of(Map.of("name", "number", "type", "long"));
        var values = List.of(List.of(1000), List.of(1000), List.of(5000));
        assertResultMap(result, columns, values);
    }

    public void testMatchField_ImplicitFieldsList_WithStats() throws IOException {
        Map<String, Object> result = runEsql("from test1 | enrich countries | stats s = sum(number) by country_name");
        var columns = List.of(Map.of("name", "s", "type", "long"), Map.of("name", "country_name", "type", "keyword"));
        var values = List.of(List.of(2000, "United States of America"), List.of(5000, "China"));
        assertResultMap(result, columns, values);
    }

    public void testSimpleIndexFilteringWithEnrich() throws IOException {
        // no filter
        Map<String, Object> result = runEsql("""
                from test* metadata _index
                | enrich countries
                | keep *number, geo.dest, _index
                | sort geo.dest, _index
            """);
        var columns = List.of(
            Map.of("name", "country_number", "type", "long"),
            Map.of("name", "number", "type", "long"),
            Map.of("name", "geo.dest", "type", "keyword"),
            Map.of("name", "_index", "type", "keyword")
        );
        var values = List.of(
            Arrays.asList(null, 5000, "CN", "test1"),
            Arrays.asList(2, null, "IN", "test2"),
            Arrays.asList(2, null, "IN", "test2"),
            Arrays.asList(null, 1000, "US", "test1"),
            Arrays.asList(null, 1000, "US", "test1"),
            Arrays.asList(3, null, "US", "test2")
        );
        assertResultMap(result, columns, values);

        // filter something that won't affect the columns
        result = runEsql("""
                from test* metadata _index
                | enrich countries
                | keep *number, geo.dest, _index
                | sort geo.dest, _index
            """, b -> b.startObject("exists").field("field", "foobar").endObject());
        assertResultMap(result, columns, List.of());
    }

    public void testIndexFilteringWithEnrich_RemoveOneIndex() throws IOException {
        // filter out test2 but specifically use one of its fields in the query (country_number)
        Map<String, Object> result = runEsql("""
                from test* metadata _index
                | enrich countries
                | keep country_number, number, geo.dest, _index
                | sort geo.dest, _index
            """, b -> b.startObject("exists").field("field", "number").endObject());

        var columns = List.of(
            Map.of("name", "country_number", "type", "long"),
            Map.of("name", "number", "type", "long"),
            Map.of("name", "geo.dest", "type", "keyword"),
            Map.of("name", "_index", "type", "keyword")
        );
        var values = List.of(
            Arrays.asList(null, 5000, "CN", "test1"),
            Arrays.asList(null, 1000, "US", "test1"),
            Arrays.asList(null, 1000, "US", "test1")
        );

        assertResultMap(result, columns, values);

        // filter out test2 and use a wildcarded field name in the "keep" command
        result = runEsql("""
                from test* metadata _index
                | enrich countries
                | keep *number, geo.dest, _index
                | sort geo.dest, _index
            """, b -> b.startObject("exists").field("field", "number").endObject());

        columns = List.of(
            Map.of("name", "number", "type", "long"),
            Map.of("name", "geo.dest", "type", "keyword"),
            Map.of("name", "_index", "type", "keyword")
        );
        values = List.of(Arrays.asList(5000, "CN", "test1"), Arrays.asList(1000, "US", "test1"), Arrays.asList(1000, "US", "test1"));
        assertResultMap(result, columns, values);
    }

    public void testIndexFilteringWithEnrich_ExpectException() throws IOException {
        // no filter, just a simple query with "enrich" that should throw a valid VerificationException
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("""
                from test* metadata _index
                | enrich countries
                | where foobar == 123
            """));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("Found 1 problem"));
        assertThat(e.getMessage(), containsString("line 3:13: Unknown column [foobar]"));

        // same query, but with a filter this time
        e = expectThrows(ResponseException.class, () -> runEsql("""
                from test* metadata _index
                | enrich countries
                | where foobar == 123
            """, b -> b.startObject("exists").field("field", "number").endObject()));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("Found 1 problem"));
        assertThat(e.getMessage(), containsString("line 3:13: Unknown column [foobar]"));
    }

    public void testIndexFilteringWithEnrich_FilterUnusedIndexFields() throws IOException {
        // filter out "test1". The field that is specific to "test1" ("number") is not actually used in the query
        Map<String, Object> result = runEsql("""
                from test* metadata _index
                | enrich countries
                | keep country_number, geo.dest, _index
                | sort geo.dest, _index
            """, b -> b.startObject("exists").field("field", "country_number").endObject());

        var columns = List.of(
            Map.of("name", "country_number", "type", "long"),
            Map.of("name", "geo.dest", "type", "keyword"),
            Map.of("name", "_index", "type", "keyword")
        );
        var values = List.of(Arrays.asList(2, "IN", "test2"), Arrays.asList(2, "IN", "test2"), Arrays.asList(3, "US", "test2"));
        assertResultMap(result, columns, values);
    }

    private Map<String, Object> runEsql(String query) throws IOException {
        return runEsql(query, null, mode);
    }

    private Map<String, Object> runEsql(String query, CheckedConsumer<XContentBuilder, IOException> filter) throws IOException {
        return runEsql(query, filter, mode);
    }

    private Map<String, Object> runEsql(String query, CheckedConsumer<XContentBuilder, IOException> filter, Mode mode) throws IOException {
        var requestObject = new RestEsqlTestCase.RequestObjectBuilder();
        if (filter != null) {
            requestObject.filter(filter);
        }
        requestObject.query(query);
        return RestEsqlTestCase.runEsql(requestObject, new AssertWarnings.NoWarnings(), profileLogger, mode);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }
}
