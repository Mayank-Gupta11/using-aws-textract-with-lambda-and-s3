import json
import time
import boto3
client = boto3.client('textract')
client_s3 = boto3.client('s3')


key='output3/'

def lambda_handler(event, context):
    # TODO implement
    
    #print(event)
    
    key_name = event['Records'][0]['s3']['object']['key']
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    
    
    full_name=key_name.split('/')[-1]
    #print(full_name)
    name=full_name.split('.')[0]
    #print(name)
    
    response = client.start_document_text_detection(
    DocumentLocation={
        'S3Object': {
            'Bucket': bucket_name,
            #'Name': name_file,
            'Name':key_name
        }
    }
    )
    #print(response)
    
    time.sleep(120)
    
    jobid=response['JobId']
    print(jobid)
    #
    response2 = client.get_document_text_detection(
    JobId=jobid
    )
    
    l=response2['Blocks']
    s_line=''
    s_word=''
    for i in l:
        if i['BlockType']=='LINE':
            s_line=s_line+i['Text']+';'
        elif i['BlockType']=='WORD':
            s_word=s_word+i['Text']+';'
    
    
    key_line=key+name+'_linewise'+'.txt'
    key_word=key+name+'_wordwise'+'.txt'
    
    client_s3.put_object(Body=s_line, Bucket=bucket_name, Key=key_line)
    client_s3.put_object(Body=s_word, Bucket=bucket_name, Key=key_word)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
