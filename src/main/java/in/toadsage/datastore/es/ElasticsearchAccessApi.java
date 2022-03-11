package in.toadsage.datastore.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQuery;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.Alias;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


@Slf4j
@RequiredArgsConstructor
public class ElasticsearchAccessApi<E extends ElasticIndex> {

    private static final String CONFIG_ES_USERNAME = "elasticsearch.username";
    private static final String CONFIG_ES_HOST = "elasticsearch.host";
    private static final String CONFIG_ES_PASSWORD = "elasticsearch.password";
    private static final String CONFIG_ES_PORT = "elasticsearch.port";
    private final ElasticsearchClient elasticsearchClient = createEsClient();
    private final Class<E> eClass;

    private static ElasticsearchClient createEsClient() {
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(PropertyHandler.get(CONFIG_ES_USERNAME),
                        PropertyHandler.get(CONFIG_ES_PASSWORD)));
        var restClient = RestClient
                .builder(new HttpHost(PropertyHandler.get(CONFIG_ES_HOST), Integer.parseInt(PropertyHandler.get(CONFIG_ES_PORT))))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder.setMaxConnTotal(500);
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    httpAsyncClientBuilder.setDefaultIOReactorConfig(
                            IOReactorConfig.custom()
                                    .setIoThreadCount(10)
                                    .build());
                    return httpAsyncClientBuilder;
                }).build();
        // Create the transport with a Jackson mapper
        var transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    public void deleteIndex(final String indexName) {
        log.info("Index creation for {} has been initiated", indexName);
        try {
            final boolean exists = isExists(indexName);
            if (exists) {
                var deleteIndexResponse = elasticsearchClient.indices().delete(builder -> builder.index(indexName));
                log.info("response status {} ", deleteIndexResponse.acknowledged());
            }
        } catch (ElasticsearchException | IOException ex) {
            throw new ESAccessException("Index creation failed for " + indexName, ex);
        }
    }

    private boolean isExists(final String indexName) throws IOException {
        return elasticsearchClient.indices().exists(builder -> builder.index(indexName)).value();
    }

    public void saveDocument(final String indexName, final String indexId, final E payload) {
        try {
            final IndexResponse indexResponse = elasticsearchClient.index(builder -> builder.index(indexName).id(indexId).document(payload));
            log.info("response status {} payloadID {}", indexResponse.index(), indexResponse.id());
        } catch (ElasticsearchException ex) {
            if (Objects.equals(ex.response().error().type(), "index_not_found_exception")) {
                createIndex(indexName, Collections.emptySet());
            } else {
                throw new ESAccessException("Index save failed", ex);
            }
        } catch (IOException ex) {
            throw new ESAccessException("Index save failed", ex);
        }
    }

    public void createIndex(final String indexName, final Set<String> alias) {
        log.info("Index creation for {} has been initiated", indexName);
        try {
            final boolean exists = isExists(indexName);
            if (!exists) {
                var createIndexResponse = elasticsearchClient.indices().create(
                        new CreateIndexRequest.Builder()
                                .index(indexName)
                                .aliases(alias.stream().collect(Collectors.toMap(o -> o, s -> new Alias.Builder().isWriteIndex(true).build())))
                                .build());
                log.info("response status {} shard {}", createIndexResponse.acknowledged(),
                        createIndexResponse.shardsAcknowledged());
            }
        } catch (ElasticsearchException | IOException ex) {
            throw new ESAccessException("Index creation failed for " + indexName, ex);
        }
        log.info("{} has already exists", indexName);
    }

    public void deleteDocument(final String indexName, final String indexId) {
        try {
            var indexResponse = elasticsearchClient.delete(builder -> builder.index(indexName).id(indexId));
            log.info("response status {} payloadID {}", indexResponse.index(), indexResponse.id());
        } catch (ElasticsearchException | IOException ex) {
            throw new ESAccessException("Index delete failed", ex);
        }
    }

    public Optional<E> getDocument(final String indexName, final String indexId) {
        try {
            var response = elasticsearchClient.get(new GetRequest.Builder()
                    .id(indexId)
                    .index(indexName).build(), eClass);
            return response.found() ? Optional.ofNullable(response.source()) : Optional.empty();
        } catch (ElasticsearchException | IOException ex) {
            throw new ESAccessException(ex, "GetDOC failed {} {}", indexName, indexId);
        }
    }

    public List<E> filter(final String indexName, final List<Query> queries,
                          final List<SortOptions> sortOptions,
                          final Integer offset,
                          final Integer size) {
        try {
            var search = elasticsearchClient.search(s -> {
                final SearchRequest.Builder req = s
                        .index(indexName)
                        .from(offset)
                        .size(getSize(size));
                if (!sortOptions.isEmpty()) {
                    req.sort(sortOptions);
                }
                if (!queries.isEmpty()) {
                    req.query(new BoolQuery.Builder().filter(queries).build()._toQuery());
                }
                return req;
            }, eClass);
            return search.hits().hits().stream().map(Hit::source).collect(Collectors.toList());

        } catch (ElasticsearchException ex) {
            if (Objects.equals(ex.response().error().type(), "index_not_found_exception")) {
                return Collections.emptyList();
            }
            throw new ESAccessException("filter and search failed " + queries, ex);
        } catch (IOException ex) {
            throw new ESAccessException("filter and search failed " + queries, ex);
        }
    }

    private Integer getSize(final Integer size) {
        var maxPageSize = 10000;
        return size > maxPageSize ? maxPageSize : size;
    }

    public Map<String, Aggregate> aggregate(final String indexName, final Map<String, Aggregation> aggregations) {
        try {
            var search = elasticsearchClient.search(s -> s
                    .index(indexName)
                    .size(0)
                    .aggregations(aggregations), eClass);
            return search.aggregations();
        } catch (ElasticsearchException | IOException ex) {
            throw new ESAccessException("filter and search failed " + aggregations, ex);
        }
    }


    public List<E> findAll(final String indexName, final List<String> ids) {
        try {
            var search = elasticsearchClient.search(s -> s
                    .index(indexName)
                    .query(q -> {
                        q.ids(QueryBuilders.ids().values(ids).build());
                        return q;
                    }), eClass);
            return search.hits().hits().stream().map(Hit::source).collect(Collectors.toList());

        } catch (ElasticsearchException | IOException ex) {
            throw new ESAccessException("find failed for " + ids, ex);
        }
    }

    public Optional<Query> whereIn(final String fieldName, final Set<String> list) {
        final List<FieldValue> values = list.stream()
                .map(FieldValue::of)
                .collect(Collectors.toUnmodifiableList());

        if (!list.isEmpty()) {
            final TermsQuery termsQuery = new TermsQuery.Builder().field(fieldName)
                    .terms(builder -> {
                        builder.value(values);
                        return builder;
                    }).build();

            return Optional.of(termsQuery._toQuery());
        }
        return Optional.empty();
    }

    public Optional<Query> between(final String fieldName, final Object from, final Object to) {
        if (from != null && to != null) {
            return Optional.of(new BoolQuery.Builder()
                    .must(List.of(new RangeQuery.Builder().field(fieldName)
                                    .gt(JsonData.of(from))
                                    .build()._toQuery(),
                            new RangeQuery.Builder().field(fieldName)
                                    .lt(JsonData.of(to))
                                    .build()._toQuery()))
                    .build()._toQuery());
        }
        return Optional.empty();
    }

    public List<E> search(final String indexName, final Map<String, String> queryMap) {
        return searchByOffset(indexName, queryMap, 0, 10000);
    }

    public List<E> searchByOffset(final String indexName,
                                  final Map<String, String> queries,
                                  final Integer offset,
                                  final Integer size) {
        try {
            var search = elasticsearchClient.search(s -> {
                final SearchRequest.Builder builder = s
                        .index(indexName)
                        .from(offset)
                        .size(getSize(size));

                if (!queries.isEmpty()) {
                    return builder.query(createQuery(queries));
                }
                return builder;
            }, eClass);
            return search.hits().hits().stream().map(Hit::source).collect(Collectors.toList());

        } catch (IOException ex) {
            throw new ESAccessException("find failed for " + queries, ex);
        } catch (ElasticsearchException ex) {
            log.error(ex.error().toString());
            throw new ESAccessException("find failed for " + queries, ex);
        }
    }

    private Query createQuery(final Map<String, String> queries) {
        var list = new ArrayList<Query>();
        queries.forEach((key, value) -> list.add(new MatchQuery.Builder()
                .field(key).query(FieldValue.of(value))
                .build()._toQuery()));
        return new BoolQuery.Builder().filter(list).build()._toQuery();
    }

}
