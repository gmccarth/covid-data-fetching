package com.redhat;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.mongodb.client.MongoClient;

@ApplicationScoped
public class MongoDbPopulator extends RouteBuilder {
    
    @Inject MongoClient mongoClient;

    @ConfigProperty(name = "camel.component.kafka.brokers")
    private String brokers;

    @ConfigProperty(name = "camel.mongodb.database")
    private String database;

    @ConfigProperty(name = "camel.mongodb.collection")
    private String collection;

    
    public void configure() {
        fromF("kafka:jhucsse?brokers=%s",brokers)
        .log("message: ${body}")
        .toF("mongodb:mongoClient?database=%s&collection=%s&operation=insert", database, collection);
    }
}
