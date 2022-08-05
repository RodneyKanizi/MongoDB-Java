package com.mdbu.transactions;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;


import static com.mongodb.client.model.Filters.eq;

public class Transaction {
    private final MongoClient client;

    public Transaction(MongoClient client) {
        this.client = client;
    }

    public void transferFunds() {
        final ClientSession clientSession = client.startSession();

        TransactionBody<String> txnBody = () -> {
            MongoCollection<Document> bankingCollection = client.getDatabase("banking").getCollection("accounts");
            MongoCollection<Document> transfersCollection = client.getDatabase("banking").getCollection("transfers");

            Bson fromAccount = eq("account_id", "MDB99115881");
            Bson debit = Updates.inc("balance", -200);

            Bson toAccount = eq("account_id", "MDB79101843");
            Bson credit = Updates.inc("balance", 200);

            transfersCollection.insertOne(new Document("_id", new ObjectId()).append("transfer_id", "TRDEMO242343").append("to_account", "MDB79101843").append("from_account", "MDB574189300").append("amount", 200));
            bankingCollection.updateOne(clientSession, fromAccount, debit);
            bankingCollection.updateOne(clientSession, toAccount, credit);

            return "Funds successfully transferred!";
        };
        try {
            clientSession.withTransaction(txnBody);
        } catch (RuntimeException e) {
            // some error handling
        } finally {
            clientSession.close();
        }
    }
}
