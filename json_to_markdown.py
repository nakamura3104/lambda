import json
import boto3
from botocore.exceptions import NoCredentialsError

s3 = boto3.client('s3')

def get_nested_keys(data, name=''):
    if type(data) is dict:
        for k, v in data.items():
            if type(v) in [list, dict]:
                yield from get_nested_keys(v, name + k + '.')
            else:
                yield name + k, v
    elif type(data) is list:
        for i in data:
            yield name[:-1], i

def lambda_handler(event, context):
    # JSONファイルをS3から取得
    try:
        file_obj = s3.get_object(Bucket='<bucket-name>', Key='<json-file-key>')
        file_content = file_obj["Body"].read().decode('utf-8')
        data = json.loads(file_content)
    except NoCredentialsError:
        return {
            'statusCode': 400,
            'body': json.dumps('Credentials not available')
        }

    # Markdown形式のテーブル作成
    md_table = "| Key | Value |\n| --- | --- |\n"
    for k, v in get_nested_keys(data):
        if "." in k:
            pads = k.count(".")
            k = ("*" + ".") * pads + k.split(".")[-1]
        if isinstance(v, list):
            v = "<br/>".join(f"- {i}" for i in v)
        md_table += f"| {k} | {v} |\n"

    # MarkdownテキストをS3に保存
    try:
        s3.put_object(Body=md_table, Bucket='<bucket-name>', Key='<markdown-file-key>')
    except NoCredentialsError:
        return {
            'statusCode': 400,
            'body': json.dumps('Credentials not available')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Markdown table successfully generated and saved to S3')
    }
