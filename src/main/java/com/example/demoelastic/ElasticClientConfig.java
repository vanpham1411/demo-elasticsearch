package com.example.demoelastic;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;

@Configuration
public class ElasticClientConfig extends AbstractElasticsearchConfiguration {
    private final String host;
    private final Integer port;
    private final String user;
    private final String pass;

    public ElasticClientConfig(@Value("${elasticsearch.hosts}") String host,
                               @Value("${elasticsearch.port}") Integer port,
                               @Value("${elasticsearch.username}") String user,
                               @Value("${elasticsearch.pass}") String pass) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
    }

    @Bean
    @Override
    public RestHighLevelClient elasticsearchClient() {
        HttpHost[] httpPosts = new HttpHost[1];
        httpPosts[0] = new HttpHost(host, port, "http");

        RestClientBuilder clientBuilder = RestClient.builder(httpPosts);
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, pass));
        clientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(clientBuilder);
    }

//    public RestHighLevelClient elasticsearchClient() {
//
//        final ClientConfiguration clientConfiguration =
//                ClientConfiguration
//                        .builder()
//                        .connectedTo("localhost:9200")
//                        .build();
//
//        return RestClients.create(clientConfiguration).rest();
//    }
}
