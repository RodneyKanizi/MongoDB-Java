package com.mdbu.aggregations;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;
import static java.util.Arrays.asList;

public class Aggregation {

    private final MongoClient client;

    public Aggregation(MongoClient client) {
        this.client = client;
    }

    public AggregateIterable<Document> getAccountTypeSummary() {
        Bson groupStage = Aggregates.group("$account_type", sum("total_balance", "$balance"), avg("average_balance", "$balance"));
        var collection  = client.getDatabase("banking").getCollection("accounts");
        AggregateIterable<Document> result = collection.aggregate(List.of(groupStage));

        result.forEach(document -> System.out.print(document.toJson()));

        return result;
    }

    public AggregateIterable<Document> sortCheckingAccountsDescending() {
        Bson matchStage = Filters.and(gt("balance", 5000), eq("account_type", "checking"));
        Bson sortStage = Aggregates.sort(orderBy(descending("balance")));
        Bson projectStage = Aggregates.project(fields(include("account_id", "account_type", "balance"),excludeId()));

        var collection  = client.getDatabase("banking").getCollection("accounts");
        AggregateIterable<Document> result = collection.aggregate(asList(matchStage, projectStage, sortStage));

        System.out.println("Display aggregation results");
        result.forEach(document -> System.out.print(document.toJson()));
        return result;
    }

    public AggregateIterable<Document> findAccountById(String accountId) {
        Bson matchStage = Aggregates.match(Filters.eq("account_id", accountId));
        Bson projectionStage = Aggregates.project(fields(include("account_holder", "account_type", "balance"), excludeId()));

        var collection  = client.getDatabase("banking").getCollection("accounts");
        AggregateIterable<Document> result = collection.aggregate(asList(matchStage, projectionStage));

        System.out.println("Display aggregation results");
        result.forEach(document -> System.out.print(document.toJson()));
        return result;
    }
}
