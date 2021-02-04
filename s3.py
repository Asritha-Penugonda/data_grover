
import boto3
import pandas as pd
 
# helper function to print s3 buckets
 
def status_df(response):
    return pd.DataFrame({
    'ts': [(x['CreationDate']) for x in response['Buckets']],
    'bucket name': [x['Name'] for x in response['Buckets']]
    
})
# create client object
 
s3 = boto3.client('s3')
 
# list all s3 buckets
 
response = s3.list_buckets()
print(response)
 
print(status_df(response))
 
# create S3 bucket
s3.create_bucket(Bucket='testbuckethp3py')
response = s3.list_buckets()
status_df(response)
 
# upload file
 
with open('testfile.txt', 'r') as f:
    content = f.read()
 
print(content)
s3.upload_file('testfile.txt', 'testbuckethp3py', 'testfile_s3.txt')
 
 
# create directory structure
 
s3.put_object(Body = content, Bucket = 'testbuckethp3py', Key = 'testdir/testfile.txt')
 
 
# upload a csv file
 
 
with open('data/New York/New York_population.csv', 'r') as f:
    content = f.read()
 
s3.put_object(Body = content, Bucket = 'testbuckethp3py', Key = 'NewYork/population.csv')
 
 
# access file from s3
s3_resource = boto3.resource('s3')
 
s3_object = s3_resource.Object(bucket_name='testbuckethp3py', key='NewYork/population.csv')
 
from io import StringIO
 
s3_data = StringIO(s3_object.get()['Body'].read().decode('utf-8'))
 
 
data = pd.read_csv(s3_data)
print(data.head())