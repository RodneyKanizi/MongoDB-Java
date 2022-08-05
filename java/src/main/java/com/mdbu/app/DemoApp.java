package com.mdbu.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.mdbu.crud.Crud;
import com.mdbu.transactions.Transaction;
import com.mdbu.utils.MongoClientSingleton;
import com.mongodb.client.MongoClient;
import org.slf4j.LoggerFactory;


public class DemoApp {
    public static void main(final String[] args) {
        Logger root = (Logger) LoggerFactory.getLogger("org.mongodb.driver");
        // Available levels are: OFF, ERROR, WARN, INFO, DEBUG, TRACE, ALL
        root.setLevel(Level.WARN);

        MongoClient client = MongoClientSingleton.getClient();

        Crud crud = new Crud(client);
        // Insert a single document
        crud.insertOneDocument();

        // Insert many documents
        crud.insertManyDocuments();

        // Update a single document
        crud.updateOneDocument();

        // Delete a document
        crud.deleteDocument();

        //Transaction
        Transaction tx = new Transaction(client);
        tx.transferFunds();

        //Close the client
        client.close();
    }
}