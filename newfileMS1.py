import boto3
import csv
import os

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    bucket_name = 'your-bucket-name'  # Replace with your own bucket name
    file_name = 'your-file-name.csv'  # Replace with your own file name
    table_name = 'your-table-name'  # Replace with your own table name

    # Download the CSV file from S3
    s3_client.download_file(bucket_name, file_name, '/tmp/' + file_name)

    # Open the CSV file and parse the data
    with open('/tmp/' + file_name, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row
        for row in csv_reader:
            item = {
                'id': row[0],
                'name': row[1],
                'description': row[2],
                'price': row[3]
            }
            
            # Add the item to the DynamoDB table
            table = dynamodb.Table(table_name)
            table.put_item(Item=item)

    # Clean up the temporary file
    os.remove('/tmp/' + file_name)

    return {
        'statusCode': 200,
        'body': 'Data loaded successfully to DynamoDB table'
    }
