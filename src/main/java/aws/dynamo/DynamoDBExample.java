package aws.dynamo;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.IntStream;

class DynamoDBExample {
    private static final String TABLE_NAME = "Movies";
    private static final long KEY = 1L;
    private static final int MOVIE_YEAR = 2015;
    private static final String MOVIE_TITLE = "The Big New Movie";

    static {
        System.setProperty("aws.accessKeyId", "fakeMyKeyId");
        System.setProperty("aws.secretKey", "fakeSecretAccessKey");
    }

    /*
    * This is a ant + maven mix style project
    * That is because the AWS pom was taking too long to download.
    *
    * So Step 1:
    *   Goto : https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SettingUp.html
    *   Download the dynamodb jar via link
    *       Setting Up DynamoDB Local (Downloadable Version)
    *
    * Extract and add the lib folder to your project classpath
    *
    * run the dynamo db jar using below command
    * java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -inMemory
    *
    * now run the main method
    *
    * */
    public static void main(String[] args) throws Exception {
        new DynamoDBExample().operations();
    }

    private void operations() throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = createTable(dynamoDB);
        addTimeToLiveOnTable(client);
        //loadData(dynamoDB);
        addMovie(table);

        IntStream.range(1, 4).forEach(i -> {
            incrementMovieRatingAtomicCounter(table);
        });
        conditionallySetAValue(table);
        conditionallyDeleteAValue(table);
        Thread.sleep(5000);
        retrieveMovie(table);
    }

    private void addTimeToLiveOnTable(AmazonDynamoDB client) {
        try {
            UpdateTimeToLiveRequest req = new UpdateTimeToLiveRequest();
            req.setTableName(TABLE_NAME);

            TimeToLiveSpecification ttlSpec = new TimeToLiveSpecification();
            ttlSpec.setAttributeName("ttl");
            ttlSpec.setEnabled(true);

            req.withTimeToLiveSpecification(ttlSpec);

            client.updateTimeToLive(req);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void conditionallyDeleteAValue(Table table) {
        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey(new PrimaryKey("ID", KEY))
                .withConditionExpression("info.rating > :val")
                .withValueMap(new ValueMap().withNumber(":val", 5.0));
        try {
            System.out.println("Attempting a conditional delete...");
            table.deleteItem(deleteItemSpec);
            System.out.println("DeleteItem succeeded");
        } catch (Exception e) {
            System.err.println("Unable to delete item: " + MOVIE_YEAR + " " + MOVIE_TITLE);
            System.err.println(e.getMessage());
        }
    }

    private void conditionallySetAValue(Table table) {
        ValueMap valueMap = new ValueMap();
        valueMap.withNumber(":value", 0);
        valueMap.withNumber(":rating", 4);
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("ID", KEY)
                .withUpdateExpression("set info.rating = :value")
                .withConditionExpression("info.rating >= :rating")
                .withValueMap(valueMap)
                .withReturnValues(ReturnValue.UPDATED_NEW);
        try {
            System.out.println("Attempting a conditional update...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Unable to update item: " + MOVIE_YEAR + " " + MOVIE_TITLE);
            System.err.println(e.getMessage());
        }
    }

    private void incrementMovieRatingAtomicCounter(Table table) {
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("ID", KEY)
                .withUpdateExpression("set info.rating = info.rating + :val")
                .withValueMap(new ValueMap().withNumber(":val", 1))
                .withReturnValues(ReturnValue.UPDATED_NEW);
        try {
            System.out.println("Incrementing an atomic counter...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Unable to update item: " + MOVIE_YEAR + " " + MOVIE_TITLE);
            System.err.println(e.getMessage());
        }
    }

    private Item retrieveMovie(Table table) {
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("ID", KEY);
        try {
            System.out.println("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome);
            return outcome;
        } catch (Exception e) {
            System.err.println("Unable to read item: " + MOVIE_YEAR + " " + MOVIE_TITLE);
            System.err.println(e.getMessage());
            return null;
        }
    }

    public void addMovie(Table table) {
        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("MOVIE_YEAR", MOVIE_YEAR);
        infoMap.put("MOVIE_TITLE", MOVIE_TITLE);
        infoMap.put("plot", "Nothing happens at all.");
        infoMap.put("rating", 1);

        try {
            System.out.println("Adding a new item...");
            PutItemOutcome outcome = table
                    .putItem(new Item().withPrimaryKey("ID", KEY).withMap("info", infoMap).withLong("ttl", 1));

            System.out.println("PutItem succeeded:" + outcome);

        } catch (Exception e) {
            System.err.println("Unable to add item: " + MOVIE_YEAR + " " + MOVIE_TITLE);
            System.err.println(e.getMessage());
        }
    }

    private void loadData(Table table) throws Exception {
        JsonParser parser = new JsonFactory().createParser(new File("moviedata.json"));

        JsonNode rootNode = new ObjectMapper().readTree(parser);
        Iterator<JsonNode> iter = rootNode.iterator();

        ObjectNode currentNode;

        while (iter.hasNext()) {
            currentNode = (ObjectNode) iter.next();

            int year = currentNode.path("MOVIE_YEAR").asInt();
            String title = currentNode.path("MOVIE_TITLE").asText();

            try {
                table.putItem(new Item().withPrimaryKey("MOVIE_YEAR", year, "MOVIE_TITLE", title).withJSON("info",
                        currentNode.path("info").toString()));
                System.out.println("PutItem succeeded: " + year + " " + title);

            } catch (Exception e) {
                System.err.println("Unable to add movie: " + year + " " + title);
                System.err.println(e.getMessage());
                break;
            }
        }
        parser.close();

    }

    private Table createTable(DynamoDB dynamoDB) {
        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(TABLE_NAME,
                    Arrays.asList(new KeySchemaElement("ID", KeyType.HASH)),
                    Arrays.asList(new AttributeDefinition("ID", ScalarAttributeType.N)),
                    new ProvisionedThroughput(10L, 10L));
            /*
            * DynamoDB uses the partition key value as input to an internal hash function.
            * The output from the hash function determines the partition
            * (physical storage internal to DynamoDB) in which the item will be stored.
            * All items with the same partition key value are stored together, in sorted order by sort key value.
            * */
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());
            return table;
        } catch (Exception e) {
            if (e instanceof ResourceInUseException) {
                return dynamoDB.getTable(TABLE_NAME);
            } else {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}
