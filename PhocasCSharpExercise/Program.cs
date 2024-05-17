using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

// Connect to local DynamoDB
AWSCredentials credentials = new BasicAWSCredentials("dummy", "dummy");
AmazonDynamoDBConfig clientConfig = new AmazonDynamoDBConfig();
clientConfig.ServiceURL = "http://localhost:8000";
AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials, clientConfig);

// Create a user table if it doesn't exist
var tableName = "User";
var hashKey = "UserId";

var tableResponse = await client.ListTablesAsync();
if (!tableResponse.TableNames.Contains(tableName))
{
    await client.CreateTableAsync(new CreateTableRequest {
        TableName = tableName,
        ProvisionedThroughput = new ProvisionedThroughput {
            ReadCapacityUnits = 3,
            WriteCapacityUnits = 1
        },
        KeySchema = [
            new KeySchemaElement {
                AttributeName = hashKey,
                KeyType = KeyType.HASH
            }
        ],
        AttributeDefinitions = [
            new AttributeDefinition {
                AttributeName = hashKey,
                AttributeType =ScalarAttributeType.S
            }
        ]
    });
}

// Wait for table to become available
bool isTableAvailable;
do {
    var tableStatus = await client.DescribeTableAsync(tableName);
    isTableAvailable = tableStatus.Table.TableStatus == "ACTIVE";
    if (!isTableAvailable) {
        Thread.Sleep(5000);
    }
} while (!isTableAvailable);

// Save a user
var context = new DynamoDBContext(client);
var currentUser = new User() { UserId = "123", Name = "ABC"};
await context.SaveAsync<User>(currentUser);

// Load it back
List<ScanCondition> conditions = [new ScanCondition("UserId", ScanOperator.Equal, "123")];
var allDocs = await context.ScanAsync<User>(conditions).GetRemainingAsync();
var savedUser = allDocs.FirstOrDefault();