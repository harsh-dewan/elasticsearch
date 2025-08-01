/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET_MAP;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.ENRICH_SOURCE_INDICES;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.ENABLE_LOOKUP_JOIN_ON_REMOTE;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.FORK_V9;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINESTATS;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINESTATS_V9;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.JOIN_LOOKUP_V12;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.JOIN_PLANNING_V1;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.METADATA_FIELDS_REMOTE_TEST;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.UNMAPPED_FIELDS;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * This suite loads the data into either the local cluster or the remote cluster, then run spec tests with CCQ.
 * TODO: Some spec tests prevents us from splitting data across multiple shards/indices/clusters
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MultiClusterSpecIT extends EsqlSpecTestCase {

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private static TestFeatureService remoteFeaturesService;
    private static RestClient remoteClusterClient;
    private static DataLocation dataLocation = null;

    @ParametersFactory(argumentFormatting = "%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertTrue("Not enough specs found " + urls, urls.size() > 0);
        return SpecReader.readScriptSpec(urls, specParser());
    }

    public MultiClusterSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, convertToRemoteIndices(testCase), instructions);
    }

    // TODO: think how to handle this better
    public static final Set<String> NO_REMOTE_LOOKUP_JOIN_TESTS = Set.of(
        // Lookup join after STATS is not supported in CCS yet
        "StatsAndLookupIPAndMessageFromIndex",
        "JoinMaskingRegex",
        "StatsAndLookupIPFromIndex",
        "StatsAndLookupMessageFromIndex",
        "MvJoinKeyOnTheLookupIndexAfterStats",
        "MvJoinKeyOnFromAfterStats",
        // Lookup join after SORT is not supported in CCS yet
        "NullifiedJoinKeyToPurgeTheJoin",
        "SortBeforeAndAfterJoin",
        "SortEvalBeforeLookup",
        "SortBeforeAndAfterMultipleJoinAndMvExpand",
        "LookupJoinAfterTopNAndRemoteEnrich",
        // Lookup join after LIMIT is not supported in CCS yet
        "LookupJoinAfterLimitAndRemoteEnrich"
    );

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        boolean remoteMetadata = testCase.requiredCapabilities.contains(METADATA_FIELDS_REMOTE_TEST.capabilityName());
        if (remoteMetadata) {
            // remove the capability from the test to enable it
            testCase.requiredCapabilities = testCase.requiredCapabilities.stream()
                .filter(c -> c.equals("metadata_fields_remote_test") == false)
                .toList();
        }
        super.shouldSkipTest(testName);
        checkCapabilities(remoteClusterClient(), remoteFeaturesService(), testName, testCase);
        // Do not run tests including "METADATA _index" unless marked with metadata_fields_remote_test,
        // because they may produce inconsistent results with multiple clusters.
        assumeFalse("can't test with _index metadata", (remoteMetadata == false) && hasIndexMetadata(testCase.query));
        Version oldVersion = Version.min(Clusters.localClusterVersion(), Clusters.remoteClusterVersion());
        assumeTrue("Test " + testName + " is skipped on " + oldVersion, isEnabled(testName, instructions, oldVersion));
        assumeFalse("INLINESTATS not yet supported in CCS", testCase.requiredCapabilities.contains(INLINESTATS.capabilityName()));
        assumeFalse("INLINESTATS not yet supported in CCS", testCase.requiredCapabilities.contains(JOIN_PLANNING_V1.capabilityName()));
        assumeFalse("INLINESTATS not yet supported in CCS", testCase.requiredCapabilities.contains(INLINESTATS_V9.capabilityName()));
        if (testCase.requiredCapabilities.contains(JOIN_LOOKUP_V12.capabilityName())) {
            assumeTrue(
                "LOOKUP JOIN not yet supported in CCS",
                hasCapabilities(adminClient(), List.of(ENABLE_LOOKUP_JOIN_ON_REMOTE.capabilityName()))
            );
        }
        // Unmapped fields require a coorect capability response from every cluster, which isn't currently implemented.
        assumeFalse("UNMAPPED FIELDS not yet supported in CCS", testCase.requiredCapabilities.contains(UNMAPPED_FIELDS.capabilityName()));
        assumeFalse("FORK not yet supported in CCS", testCase.requiredCapabilities.contains(FORK_V9.capabilityName()));
        // Tests that use capabilities not supported in CCS
        assumeFalse(
            "This syntax is not supported with remote LOOKUP JOIN",
            NO_REMOTE_LOOKUP_JOIN_TESTS.stream().anyMatch(testName::contains)
        );
        // Tests that do SORT before LOOKUP JOIN - not supported in CCS
        assumeFalse("LOOKUP JOIN after SORT not yet supported in CCS", testName.contains("OnTheCoordinator"));

    }

    @Override
    protected boolean supportTimeSeriesCommand() {
        return false;
    }

    private TestFeatureService remoteFeaturesService() throws IOException {
        if (remoteFeaturesService == null) {
            var remoteNodeVersions = readVersionsFromNodesInfo(remoteClusterClient());
            var semanticNodeVersions = remoteNodeVersions.stream()
                .map(ESRestTestCase::parseLegacyVersion)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
            remoteFeaturesService = createTestFeatureService(getClusterStateFeatures(remoteClusterClient()), semanticNodeVersions);
        }
        return remoteFeaturesService;
    }

    private RestClient remoteClusterClient() throws IOException {
        if (remoteClusterClient == null) {
            HttpHost[] remoteHosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
            remoteClusterClient = super.buildClient(restAdminSettings(), remoteHosts);
        }
        return remoteClusterClient;
    }

    @AfterClass
    public static void closeRemoveFeaturesService() throws IOException {
        IOUtils.close(remoteClusterClient);
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] localHosts) throws IOException {
        RestClient localClient = super.buildClient(settings, localHosts);
        HttpHost[] remoteHosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
        RestClient remoteClient = super.buildClient(settings, remoteHosts);
        return twoClients(localClient, remoteClient);
    }

    // These indices are used in metadata tests so we want them on remote only for consistency
    public static final List<String> METADATA_INDICES = List.of("employees", "apps", "ul_logs");

    // These are lookup indices, we want them on both remotes and locals
    public static final Set<String> LOOKUP_INDICES = CSV_DATASET_MAP.values()
        .stream()
        .filter(td -> td.settingFileName() != null && td.settingFileName().equals("lookup-settings.json"))
        .map(CsvTestsDataLoader.TestDataset::indexName)
        .collect(Collectors.toSet());

    public static final Set<String> LOOKUP_ENDPOINTS = LOOKUP_INDICES.stream().map(i -> "/" + i + "/_bulk").collect(Collectors.toSet());

    public static final Set<String> ENRICH_ENDPOINTS = ENRICH_SOURCE_INDICES.stream()
        .map(i -> "/" + i + "/_bulk")
        .collect(Collectors.toSet());

    /**
     * Creates a new mock client that dispatches every request to both the local and remote clusters, excluding _bulk and _query requests.
     * - '_bulk' requests are randomly sent to either the local or remote cluster to populate data. Some spec tests, such as AVG,
     *   prevent the splitting of bulk requests.
     * - '_query' requests are dispatched to the local cluster only, as we are testing cross-cluster queries.
     */
    static RestClient twoClients(RestClient localClient, RestClient remoteClient) throws IOException {
        RestClient twoClients = mock(RestClient.class);
        assertNotNull("data location was set", dataLocation);
        // write to a single cluster for now due to the precision of some functions such as avg and tests related to updates
        final RestClient bulkClient = dataLocation == DataLocation.REMOTE_ONLY ? remoteClient : randomFrom(localClient, remoteClient);
        when(twoClients.performRequest(any())).then(invocation -> {
            Request request = invocation.getArgument(0);
            String endpoint = request.getEndpoint();
            if (endpoint.startsWith("/_query")) {
                return localClient.performRequest(request);
            } else if (endpoint.endsWith("/_bulk") && METADATA_INDICES.stream().anyMatch(i -> endpoint.equals("/" + i + "/_bulk"))) {
                return remoteClient.performRequest(request);
            } else if (endpoint.endsWith("/_bulk")
                && ENRICH_ENDPOINTS.contains(endpoint) == false
                && LOOKUP_ENDPOINTS.contains(endpoint) == false) {
                    return bulkClient.performRequest(request);
                } else {
                    Request[] clones = cloneRequests(request, 2);
                    Response resp1 = remoteClient.performRequest(clones[0]);
                    Response resp2 = localClient.performRequest(clones[1]);
                    assertEquals(resp1.getStatusLine().getStatusCode(), resp2.getStatusLine().getStatusCode());
                    return resp2;
                }
        });
        doAnswer(invocation -> {
            IOUtils.close(localClient, remoteClient);
            return null;
        }).when(twoClients).close();
        return twoClients;
    }

    enum DataLocation {
        REMOTE_ONLY,
        ANY_CLUSTER
    }

    static Request[] cloneRequests(Request orig, int numClones) throws IOException {
        Request[] clones = new Request[numClones];
        for (int i = 0; i < clones.length; i++) {
            clones[i] = new Request(orig.getMethod(), orig.getEndpoint());
            clones[i].addParameters(orig.getParameters());
        }
        HttpEntity entity = orig.getEntity();
        if (entity != null) {
            byte[] bytes = entity.getContent().readAllBytes();
            entity.getContent().close();
            for (Request clone : clones) {
                ByteArrayInputStream cloneInput = new ByteArrayInputStream(bytes);
                HttpEntity cloneEntity = spy(entity);
                when(cloneEntity.getContent()).thenReturn(cloneInput);
                clone.setEntity(cloneEntity);
            }
        }
        return clones;
    }

    /**
     * Convert FROM employees ... => FROM *:employees,employees
     */
    static CsvSpecReader.CsvTestCase convertToRemoteIndices(CsvSpecReader.CsvTestCase testCase) {
        if (dataLocation == null) {
            dataLocation = randomFrom(DataLocation.values());
        }
        String query = testCase.query;
        String[] commands = query.split("\\|");
        String first = commands[0].trim();
        // If true, we're using *:index, otherwise we're using *:index,index
        boolean onlyRemotes = canUseRemoteIndicesOnly() && randomBoolean();
        if (commands[0].toLowerCase(Locale.ROOT).startsWith("from")) {
            String[] parts = commands[0].split("(?i)metadata");
            assert parts.length >= 1 : parts;
            String fromStatement = parts[0];
            String[] localIndices = fromStatement.substring("FROM ".length()).split(",");
            if (Arrays.stream(localIndices).anyMatch(i -> LOOKUP_INDICES.contains(i.trim().toLowerCase(Locale.ROOT)))) {
                // If the query contains lookup indices, use only remotes to avoid duplication
                onlyRemotes = true;
            }
            final boolean onlyRemotesFinal = onlyRemotes;
            final String remoteIndices = Arrays.stream(localIndices)
                .map(index -> unquoteAndRequoteAsRemote(index.trim(), onlyRemotesFinal))
                .collect(Collectors.joining(","));
            var newFrom = "FROM " + remoteIndices + " " + commands[0].substring(fromStatement.length());
            testCase.query = newFrom + query.substring(first.length());
        }
        if (commands[0].toLowerCase(Locale.ROOT).startsWith("ts ")) {
            String[] parts = commands[0].split("\\s+");
            assert parts.length >= 2 : commands[0];
            String[] indices = parts[1].split(",");
            final boolean onlyRemotesFinal = onlyRemotes;
            parts[1] = Arrays.stream(indices)
                .map(index -> unquoteAndRequoteAsRemote(index.trim(), onlyRemotesFinal))
                .collect(Collectors.joining(","));
            String newNewMetrics = String.join(" ", parts);
            testCase.query = newNewMetrics + query.substring(first.length());
        }
        int offset = testCase.query.length() - query.length();
        if (offset != 0) {
            final String pattern = "\\b1:(\\d+)\\b";
            final Pattern regex = Pattern.compile(pattern);
            testCase.adjustExpectedWarnings(warning -> regex.matcher(warning).replaceAll(match -> {
                int position = Integer.parseInt(match.group(1));
                int newPosition = position + offset;
                return "1:" + newPosition;
            }));
        }
        return testCase;
    }

    static boolean canUseRemoteIndicesOnly() {
        // If the data is indexed only into the remote cluster, we can query only the remote indices.
        // However, due to the union types bug in CCS, we must include the local indices in versions without the fix.
        return dataLocation == DataLocation.REMOTE_ONLY && Clusters.bwcVersion().onOrAfter(Version.V_9_1_0);
    }

    static boolean hasIndexMetadata(String query) {
        String[] commands = query.split("\\|");
        if (commands[0].trim().toLowerCase(Locale.ROOT).startsWith("from")) {
            String[] parts = commands[0].split("(?i)metadata");
            return parts.length > 1 && parts[1].contains("_index");
        }
        return false;
    }

    /**
     * Since partial quoting is prohibited, we need to take the index name, unquote it,
     * convert it to a remote index, and then requote it. For example, "employees" is unquoted,
     * turned into the remote index *:employees, and then requoted to get "*:employees".
     * @param index Name of the index.
     * @param asRemoteIndexOnly If the return needs to be in the form of "*:idx,idx" or "*:idx".
     * @return A remote index pattern that's requoted.
     */
    private static String unquoteAndRequoteAsRemote(String index, boolean asRemoteIndexOnly) {
        index = index.trim();

        int numOfQuotes = 0;
        for (; numOfQuotes < index.length(); numOfQuotes++) {
            if (index.charAt(numOfQuotes) != '"') {
                break;
            }
        }

        String unquoted = unquote(index, numOfQuotes);
        if (asRemoteIndexOnly) {
            return quote("*:" + unquoted, numOfQuotes);
        } else {
            return quote("*:" + unquoted + "," + unquoted, numOfQuotes);
        }
    }

    private static String quote(String index, int numOfQuotes) {
        return "\"".repeat(numOfQuotes) + index + "\"".repeat(numOfQuotes);
    }

    private static String unquote(String index, int numOfQuotes) {
        return index.substring(numOfQuotes, index.length() - numOfQuotes);
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected boolean supportsInferenceTestService() {
        return false;
    }

    @Override
    protected boolean supportsIndexModeLookup() throws IOException {
        return hasCapabilities(adminClient(), List.of(JOIN_LOOKUP_V12.capabilityName()));
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return false;
    }

    @Override
    protected boolean supportsTook() throws IOException {
        // We don't read took properly in multi-cluster tests.
        return false;
    }
}
