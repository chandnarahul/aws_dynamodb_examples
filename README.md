# aws_dynamodb_examples

 This is a ant + maven mix style project

 That is because the AWS pom was taking too long to download.

 So

 Step 1:

    Goto : https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SettingUp.html

    Click on the link with below description

       Setting Up DynamoDB Local (Downloadable Version)

 Step 2:

    Extract and add the lib folder to your project classpath

 Step 3:

    run the dynamo db jar using below command

    java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -inMemory

 Step 4:

    now run the main method
