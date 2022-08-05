package com.mdbu.crud;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;

import static com.mongodb.client.model.Filters.*;

public class Crud {
    private final MongoCollection<Document> collection;

    public Crud(MongoClient client) {
        this.collection = client.getDatabase("banking").getCollection("accounts");
    }

    public void insertOneDocument() {
        Document inspection = new Document("_id", new ObjectId())
                .append("account_id", "MDB255054629")
                .append("account_holder", "Mai Kalange")
                .append("account_type", "savings")
                .append("balance", 2340);
        System.out.println("Inserting one inspection document");
        InsertOneResult result = collection.insertOne(inspection);

        BsonValue id = result.getInsertedId();
        System.out.println("Inserted document Id:");
        System.out.println("\t" + id);
    }

    public void insertManyDocuments() {
        Document doc1 = new Document().append("account_holder", "John Doe").append("account_id", "MDB99115881").append("balance", 1785).append("account_type", "checking");
        Document doc2 = new Document().append("account_holder", "Jane Doe").append("account_id", "MDB79101843").append("balance", 1468).append("account_type", "checking");

        ArrayList<Document> accounts = new ArrayList<>();
        accounts.add(doc1);
        accounts.add(doc2);

        InsertManyResult result = collection.insertMany(accounts, new InsertManyOptions().ordered(false));
        result.getInsertedIds().forEach((x, y) -> System.out.println(y.asObjectId()));
        System.out.println("\tTotal # of documents: " + result.getInsertedIds().size());
    }

    public void updateOneDocument() {
        Document query = new Document().append("account_holder", "John Doe");
        Bson updates = Updates.combine(Updates.set("account_status", "active"), Updates.inc("balance", 100));
        UpdateResult updateResult = collection.updateOne(query, updates);

        System.out.println("Updated a document:");
        System.out.println("\t" + updateResult.getModifiedCount());
    }

    public void updateManyDocuments() {
        Document query = new Document().append("account_type", "savings");
        Bson updates = Updates.combine(Updates.set("minimum_balance", 100));
        UpdateResult updateResult = collection.updateMany(query, updates);

        System.out.println("Updated a document:");
        System.out.println("\t" + updateResult.getModifiedCount());

    }

    public void findSingleCheckingAccount() {
        Document doc = collection.find(and(gte("balance", 1000), eq("account_type", "checking"))).first();
        System.out.println(doc != null ? doc.toJson() : null);
    }

    public void findAllCheckingAccounts() {
        try (MongoCursor<Document> cursor = collection.find(and(gte("balance", 1000), eq("account_type", "checking")))
                .iterator()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }
    }

    public void deleteDocument() {
        Bson query = eq("account_holder", "John Doe");
        DeleteResult delResult = collection.deleteOne(query);
        System.out.println("Deleted a document:");
        System.out.println("\t" + delResult.getDeletedCount());
    }

    public void deleteMany(String accountStatus) {
        Bson query = eq("account_status", accountStatus);
        DeleteResult delResult = collection.deleteMany(query);
        System.out.println("Deleted a document:");

        System.out.println("\t" + delResult.getDeletedCount());
    }
}
