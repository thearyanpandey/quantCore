package com.quantcore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutionEngine {

    // Configuration
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final String STREAM_KEY = "trade_signals";
    private static final String CONSUMER_GROUP = "execution_group";
    private static final String CONSUMER_NAME = "java_accountant_1";

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/quantcore";
    private static final String DB_USER = "admin";
    private static final String DB_PASS = "secret";

    // In-Memory Wallet (Simplified for Phase 3)
    private static double cashBalance = 10000.00; // Start with $10k
    private static Map<String, Double> cryptoHoldings = new HashMap<>();

    public static void main(String[] args) {
        System.out.println("💰 Java Execution Engine Starting...");
        System.out.println("💵 Initial Balance: $" + cashBalance);

        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        ObjectMapper mapper = new ObjectMapper();

        // 1. Create Consumer Group
        try {
            jedis.xgroupCreate(STREAM_KEY, CONSUMER_GROUP, new StreamEntryID(0, 0), true);
            System.out.println("✅ Consumer Group Created");
        } catch (Exception e) {
            System.out.println("ℹ️  Consumer Group already exists");
        }

        // 2. Connect to Database
        try (Connection dbConn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            System.out.println("✅ Connected to PostgreSQL");

            while (true) {
                // 3. Read Signals from Redis
                // Block for 2 seconds waiting for data
                List<Map.Entry<String, List<StreamEntry>>> streams = jedis.xreadGroup(
                        CONSUMER_GROUP,
                        CONSUMER_NAME,
                        XReadGroupParams.xReadGroupParams().count(1).block(2000),
                        Map.of(STREAM_KEY, StreamEntryID.UNRECEIVED_ENTRY));

                if (streams != null && !streams.isEmpty()) {
                    for (Map.Entry<String, List<StreamEntry>> stream : streams) {
                        for (StreamEntry entry : stream.getValue()) {
                            
                            // 4. Parse JSON Signal
                            String json = entry.getFields().get("json");
                            JsonNode node = mapper.readTree(json);
                            
                            String symbol = node.get("symbol").asText();
                            String action = node.get("action").asText(); // BUY or SELL
                            double price = node.get("price").asDouble();
                            String signalId = node.get("id").asText();
                            long timestamp = node.get("timestamp").asLong();

                            // 5. Execute Business Logic
                            boolean executed = executeTrade(symbol, action, price);

                            if (executed) {
                                // 6. Persist to Postgres
                                String insertSQL = "INSERT INTO trades (signal_id, symbol, action, price, timestamp, execution_status) VALUES (?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement pstmt = dbConn.prepareStatement(insertSQL)) {
                                    pstmt.setString(1, signalId);
                                    pstmt.setString(2, symbol);
                                    pstmt.setString(3, action);
                                    pstmt.setDouble(4, price);
                                    pstmt.setLong(5, timestamp);
                                    pstmt.setString(6, "SUCCESS");
                                    pstmt.executeUpdate();
                                    System.out.println("📝 Trade Logged to DB");
                                }
                            }

                            // 7. Acknowledge message in Redis
                            jedis.xack(STREAM_KEY, CONSUMER_GROUP, entry.getID());
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // The Logic Core
    private static boolean executeTrade(String symbol, String action, double price) {
        double quantityToTrade = 0.01; // Fixed size for simplicity

        if (action.equals("BUY")) {
            double cost = price * quantityToTrade;
            if (cashBalance >= cost) {
                cashBalance -= cost;
                cryptoHoldings.put(symbol, cryptoHoldings.getOrDefault(symbol, 0.0) + quantityToTrade);
                System.out.printf("🟢 BUY EXECUTED: %s @ %.2f | New Balance: $%.2f%n", symbol, price, cashBalance);
                return true;
            } else {
                System.out.println("🔴 BUY REJECTED: Insufficient Funds");
                return false;
            }
        } else if (action.equals("SELL")) {
            double currentHolding = cryptoHoldings.getOrDefault(symbol, 0.0);
            if (currentHolding >= quantityToTrade) {
                double revenue = price * quantityToTrade;
                cashBalance += revenue;
                cryptoHoldings.put(symbol, currentHolding - quantityToTrade);
                System.out.printf("🔴 SELL EXECUTED: %s @ %.2f | New Balance: $%.2f%n", symbol, price, cashBalance);
                return true;
            } else {
                System.out.println("⚠️ SELL REJECTED: No Holdings");
                return false;
            }
        }
        return false;
    }
}