import json
import boto3
import csv
import operator
from datetime import datetime, date, timedelta

today = datetime.today()
yesterday = today - timedelta(days=1)
dayBeforeYesterday = today - timedelta(days=2)

folder_title = datetime.strftime(yesterday, '%Y-%m-%d')
endTime = datetime.strftime(yesterday, '%Y-%m-%dT15:00:00Z')
startTime = datetime.strftime(dayBeforeYesterday, '%Y-%m-%dT15:00:00Z')

def getMetricStatistics(target_dict):
    cloudwatch = boto3.client('cloudwatch', region_name='ap-northeast-1')

    logs = cloudwatch.get_metric_statistics(Namespace=target_dict["NameSpaceHeader"] + target_dict["NameSpace"],
                                MetricName=target_dict["MetricName"],
                                Dimensions=target_dict["Dimensions"],
                                StartTime=startTime,
                                EndTime=endTime,
                                Period=300,
                                Statistics=[target_dict["Statistics"]]
                                )
    return logs


def convertCSV(datapoints):
    with open('/tmp/tmp.txt','w') as f:
        if len(datapoints) == 0:
            print("Empty")
            pass
        else:
            csv.register_dialect('dialect1', doublequote=True, quoting=csv.QUOTE_ALL)
            writer = csv.DictWriter(f, fieldnames=datapoints[0].keys(), dialect="dialect1")
            for row in datapoints:
                writer.writerow(row)


def sortCSV():
    with open('/tmp/tmp.txt', 'r') as f:
        reader = csv.reader(f)
        result = sorted(reader, key=operator.itemgetter(0))

    with open('/tmp/tmp.txt', 'w') as f:
        data = csv.writer(f,delimiter=',')
        for row in result:
            data.writerow(row)

def generateFileName(target_dict):
    directory_name = folder_title
    file_name = target_dict['NameSpace'] + "-" + target_dict['MetricName'] + "-" + folder_title + ".csv"
    print(directory_name + "/" + file_name)
    return directory_name + "/" + file_name


def uploadToS3(fileName):
    s3 = boto3.client('s3')

    with open('/tmp/tmp.txt', 'r') as f:
        data = f.read()
        result = s3.put_object(
            ACL='private',
            Body=data,
            Bucket=os.environ['BUCKET_NAME'],
            Key=fileName
            )
        print(result)


def lambda_handler(event, context):
    all_metrics_list = [
        {
        'NameSpaceHeader' : 'AWS/',
        'NameSpace' : 'EC2',
        'MetricName':'CPUUtilization',
        'Dimensions':[{"Name" : "InstanceId","Value" : "i-xxxxxxxxxxxxxxxxx"}],
        'Statistics' : 'Average'
        }
    ]

    for target in all_metrics_list:
        #メトリクス取得
        logs = getMetricStatistics(target)

        #CSVへ変換・ソート
        convertCSV(logs['Datapoints'])
        sortCSV()

        #適当なフォルダ名/ファイル名を生成して、s3へアップ
        uploadToS3(generateFileName(target))

    return {
        'statusCode': 200,
        'body': json.dumps('finish lambda')
    }