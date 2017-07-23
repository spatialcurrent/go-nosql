package nosql

import (
  "errors"
  "log"
  "strings"
)

import (
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/awserr"
  "github.com/aws/aws-sdk-go/aws/credentials"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/dynamodb"
  "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type BackendDynamoDB struct {
	dynamodb_client *dynamodb.DynamoDB
}

func (b *BackendDynamoDB) Type() string {
	return "dynamodb"
}

func (b *BackendDynamoDB) Connect(options map[string]string) error {

  aws_access_key_id := options["AWSAccessKeyId"]
  aws_secret_access_key := options["AWSSecretAccessKey"]
  aws_session_token := options["AWSSessionToken"]
  aws_region := options["AWSDefaultRegion"]
  dynamodb_url := options["StorefrontDynamoDBUrl"]

  if strings.Contains(dynamodb_url, "localhost") {
    aws_access_key_id = "localhost"
    aws_secret_access_key = "localhost"
    aws_session_token = "localhost"
  }

  aws_session := session.Must(session.NewSessionWithOptions(session.Options{
    Config: aws.Config{
      Credentials: credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, aws_session_token),
      Endpoint: aws.String(dynamodb_url),
      MaxRetries: aws.Int(3),
      Region: aws.String(aws_region),
    },
  }))

  b.dynamodb_client = dynamodb.New(aws_session)
	return nil
}

func (b *BackendDynamoDB) GetItemById(table_name string, id string, item interface{}) error {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(table_name),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
		},
	}

	result, err := b.dynamodb_client.GetItem(input)
	if err != nil {
		return err
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, item)
	if err != nil {
		return err
	}

	return nil
}

func (b *BackendDynamoDB) GetItemsByIds(table_name string, ids []string, sort_fields []string, items interface{}) error {
	input := &dynamodb.QueryInput{
		TableName: aws.String(table_name),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			"id": {
				SS: aws.StringSlice(ids),
			},
		},
		KeyConditionExpression: aws.String("id IN :id"),
	}

	result, err := b.dynamodb_client.Query(input)
	if err != nil {
		return err
	}

	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, items)
	if err != nil {
		return err
	}

	return nil
}


func (b *BackendDynamoDB) GetItemByAttributeValue(table_name string, attribute_name string, attribute_value string, item interface{}) error {
  eav := map[string]*dynamodb.AttributeValue{}
	eav[":v"] = &dynamodb.AttributeValue{
		S: aws.String(attribute_value),
	}

  input := &dynamodb.QueryInput{
    TableName: aws.String(table_name),
    IndexName: aws.String(attribute_name+"_index"),
    KeyConditionExpression: aws.String(attribute_name+" = :v"),
    ExpressionAttributeValues: eav,
  }

  result, err := b.dynamodb_client.Query(input)
	if err != nil {
		return err
	}

  err = dynamodbattribute.UnmarshalMap(result.Items[0], item)
	if err != nil {
		return err
	}

	return nil
}

func (b *BackendDynamoDB) GetItemsByAttributeValue(table_name string, attribute_name string, attribute_value string, sort_fields []string, items interface{}) error {
	eav := map[string]*dynamodb.AttributeValue{}
	eav[":v"] = &dynamodb.AttributeValue{
		S: aws.String(attribute_value),
	}

  input := &dynamodb.QueryInput{
    TableName: aws.String(table_name),
    IndexName: aws.String(attribute_name+"_index"),
    KeyConditionExpression: aws.String(attribute_name+" = :v"),
    ExpressionAttributeValues: eav,
  }

  result, err := b.dynamodb_client.Query(input)
	if err != nil {
		return err
	}

  err = dynamodbattribute.UnmarshalListOfMaps(result.Items, items)
	if err != nil {
		return err
	}

	return nil
}


func (b *BackendDynamoDB) GetItems(table_name string, sort_fields []string, items interface{}) (error) {
  input := &dynamodb.ScanInput{
		TableName: aws.String(table_name),
    //KeyConditionExpression: aws.String(""),
	}

	result, err := b.dynamodb_client.Scan(input)
	if err != nil {
		return err
	}

  export := items.(*[]interface{})

  for _, item := range result.Items {
		*export = append(*export, item)
	}

	return nil
}

func (b *BackendDynamoDB) RemoveItemById(table_name string, id string) error {
	input := &dynamodb.DeleteItemInput{
    TableName: aws.String(table_name),
    Key: map[string]*dynamodb.AttributeValue{
      "id": {
        S: aws.String(id),
      },
    },
  }

  _, err := b.dynamodb_client.DeleteItem(input)
  if err != nil {
    return err
  }

  return nil
}

func (b *BackendDynamoDB) RemoveItemByAttributeValue(table_name string, attribute_name string, attribute_value string) error {

	key := map[string]*dynamodb.AttributeValue{}
	key[attribute_name] = &dynamodb.AttributeValue{
		S: aws.String(attribute_value),
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(table_name),
		Key: key,
	}

	_, err := b.dynamodb_client.DeleteItem(input)
	if err != nil {
		return err
	}

	return nil
}

func (b *BackendDynamoDB) RemoveItemsByAttributeValue(table_name string, attribute_name string, attribute_value string) error {

  eav := map[string]*dynamodb.AttributeValue{}
	eav[attribute_name] = &dynamodb.AttributeValue{
		S: aws.String(attribute_value),
	}

  input := &dynamodb.QueryInput{
    TableName: aws.String(table_name),
    ExpressionAttributeValues: eav,
    KeyConditionExpression: aws.String(attribute_name+" = :"+attribute_name),
  }

	result, err := b.dynamodb_client.Query(input)
	if err != nil {
		return err
	}

  for _, item := range result.Items {
		b.RemoveItemById(table_name, *item["id"].S)
	}

	return nil
}

func (b *BackendDynamoDB) RemoveAll(table_name string) error {
	input := &dynamodb.QueryInput{
		TableName: aws.String(table_name),
	}

	result, err := b.dynamodb_client.Query(input)
	if err != nil {
		return err
	}

  for _, item := range result.Items {
		b.RemoveItemById(table_name, *item["id"].S)
	}

	return nil

}

func (b *BackendDynamoDB) InsertItem(table_name string, item interface{}) error {

	av, err := dynamodbattribute.MarshalMap(item)
  if err != nil {
    return errors.New("Error: Could not marshal DynamoDB item")
  }

  _, err = b.dynamodb_client.PutItem(&dynamodb.PutItemInput{
    TableName: aws.String(table_name),
    Item: av,
  })

  return err
}

func (b *BackendDynamoDB) UpdateItemById(table_name string, id string, values map[string]interface{}) error {
  return errors.New("Not implemented yet!")
}



func (b *BackendDynamoDB) CreateTables(tables []Table) error {
  var err error
	for _, t := range tables {
		err = b.CreateTable(t.Name, t.Indexes)
    if err != nil {
      break
    }
	}
	return err
}

func (b *BackendDynamoDB) CreateTable(table_name string, indexes []string) error {

  pt := &dynamodb.ProvisionedThroughput{
    ReadCapacityUnits: aws.Int64(400),
    WriteCapacityUnits: aws.Int64(400),
  }

  ad := []*dynamodb.AttributeDefinition{
    &dynamodb.AttributeDefinition{AttributeName: aws.String("id"), AttributeType: aws.String("S")},
  }

  gsi := []*dynamodb.GlobalSecondaryIndex{}
  if len(indexes) > 0 {
    for _, index := range indexes {
      ad = append(ad, &dynamodb.AttributeDefinition{
        AttributeName: aws.String(index),
        AttributeType: aws.String("S"),
      })
      gsi = append(gsi, &dynamodb.GlobalSecondaryIndex{
        IndexName: aws.String(index+"_index"),
        KeySchema: []*dynamodb.KeySchemaElement{
          &dynamodb.KeySchemaElement{AttributeName: aws.String(index), KeyType: aws.String("HASH")},
        },
        Projection: &dynamodb.Projection{
          NonKeyAttributes: nil,
          ProjectionType: aws.String("ALL"),
        },
        ProvisionedThroughput: pt,
      })
    }
  }

  input := &dynamodb.CreateTableInput{
    TableName: aws.String(table_name),
    AttributeDefinitions: ad,
    KeySchema: []*dynamodb.KeySchemaElement{
      &dynamodb.KeySchemaElement{AttributeName: aws.String("id"), KeyType: aws.String("HASH")},
    },
    ProvisionedThroughput: pt,
  }
  if len(gsi) > 0 {
    input.SetGlobalSecondaryIndexes(gsi)
  }

  _, err := b.dynamodb_client.CreateTable(input)
  if err != nil {
    return err
  }

  return nil

}

func (b *BackendDynamoDB) DeleteTables(table_names []string) error {
  var err error
	for _, table_name := range table_names {
		err = b.DeleteTable(table_name)
    if err != nil {
      if aerr, ok := err.(awserr.Error); ok {
        switch aerr.Code() {
        case dynamodb.ErrCodeResourceInUseException:
           break
        case dynamodb.ErrCodeResourceNotFoundException:
          // If it doesn't exist, that's fine.  Just log and continue.
          log.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
          err = nil
        case dynamodb.ErrCodeLimitExceededException:
           break
        case dynamodb.ErrCodeInternalServerError:
          break
        default:
          break
        }
      } else {
        break
      }
    }
	}
	return err
}

func (b *BackendDynamoDB) DeleteTable(table_name string) error {
  input := &dynamodb.DeleteTableInput{
    TableName: aws.String(table_name),
  }

  _, err := b.dynamodb_client.DeleteTable(input)
  if err != nil {
    return err
  }

  return nil

}