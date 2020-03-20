from google.cloud import bigquery
import os
from datetime import datetime,timezone
import docker
import json
import requests
import re
import io
import ipinfo
from time import sleep

#--------------
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
bq_project_id = ''
dataset_id = ''
#-------
table_id = os.environ["TABLE_ID"]
containerName = os.environ['CONTAINER_NAME']
ipinfo_token = os.environ['IPINFO_TOKEN']
serverName = os.environ['SERVER_NAME']


#nginx_log_format = '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" "$http_x_forwarded_for"'


def get_country_region(ips):

    print('api call')
    
    for ip in ips:
        retries = 0
        while True:
            try:

                access_token = ipinfo_token
                handler = ipinfo.getHandler(access_token)
                details = handler.getDetails(ip)
                ipDetail = {
                    "country" : details.country,
                    "region" : details.region,
                    "ip" : ip
                }
                return ipDetail

            except AttributeError as e:
                print(f"{serverName} - Export Attribute error : {repr(e)}\nIP : {ip}",alert=True)
                sleep(5)
                break

            except Exception as e:
                if (retries := retries+1) == 4:
                    break

                print('{} - Export Exception : {}'.format(serverName,repr(e)),alert=True)
                sleep(5)



def get_logs():

    logs_details = []

    client = docker.from_env()
    container = client.containers.get(containerName)

    match_string = '([\d.]+) - - \[(.*)\] \"(.*)\" ([\d]+) ([\d]+) \"(.*)\" \"(.*)\" \"(.*)\"'

    with open('last_timestamp.json') as f:
        lastTimestamp = json.loads(f.read())
    
    f.close()

    if "timestamp" in lastTimestamp:
        last_timestamp = datetime.strptime(lastTimestamp["timestamp"],'%Y-%m-%d %H:%M:%S.%f')
        lines = container.logs(since=last_timestamp).decode('utf-8').splitlines()
    else:
        lines = container.logs().decode('utf-8').splitlines()

    with open('ipList.json') as f:
        ipList = json.loads(f.read())
        if ipList["init"]:
            ipList["init"] = False
            ip_deets = {}
        else:
            ip_deets = ipList["details"]
    f.close()

    for line in lines:
        if match := re.match(match_string,line):

            remote_addr = match.group(1)

            timestamp = match.group(2)
            timestamp = str(datetime.strptime(timestamp,'%d/%b/%Y:%H:%M:%S %z'))

            request_uri = match.group(3)
            status = match.group(4)
            body_bytes_sent = match.group(5)
            http_referer = match.group(6)
            http_user_agent = match.group(7)

            http_x_forwarded_for = match.group(8)
            http_x_forwarded_for_list = http_x_forwarded_for.split(",")

            logDetail = {}
            logDetail["remote_addr"] = remote_addr
            logDetail["request"] = request_uri
            logDetail["status"] = status
            logDetail["body_bytes_sent"] = body_bytes_sent
            logDetail["http_referer"] = http_referer
            logDetail["http_user_agent"] = http_user_agent
            logDetail["timestamp"] = timestamp

            ipFound = False
            for ip in http_x_forwarded_for_list:
                if ip in ip_deets:
                    logDetail["country"] = ip_deets[ip]["country"]
                    logDetail["region"] = ip_deets[ip]["region"]
                    logDetail["http_x_forwarded_for"] = ip
                    ipFound = True
                    break
            
            if not ipFound:
                ipDetail = get_country_region(http_x_forwarded_for_list)

                if ipDetail:
                    logDetail["country"] = ipDetail["country"]
                    logDetail["region"] = ipDetail["region"]
                    logDetail["http_x_forwarded_for"] = ipDetail["ip"]

                    ipDetail.pop("ip")
                    ip_deets[logDetail["http_x_forwarded_for"]] = ipDetail

                else:
                    logDetail["http_x_forwarded_for"] = http_x_forwarded_for_list[-1]


            logs_details.append(logDetail)

    ipList["details"] = ip_deets
    f = open("ipList.json","w")
    f.write(json.dumps(ipList))
    f.close()

    
    return logs_details


def prepare_rows(logs_details):

    #---------
    

    rows = []

    for log in logs_details:
        row = {}
        row["REMOTE_ADDR"] = log["remote_addr"]
        row["REQUEST"] = log["request"]
        row["STATUS"] = log["status"]
        row["BODY_BYTES_SENT"] = log["body_bytes_sent"]
        row["HTTP_REFERER"] = log["http_referer"]
        row["HTTP_USER_AGENT"] = log["http_user_agent"]
        row["HTTP_X_FORWARDED_FOR"] = log["http_x_forwarded_for"]
        if "country" in log:
            row["COUNTRY"] = log["country"]
        if "region" in log:
            row["REGION"] = log["region"]
        row["TIMESTAMP"] = log["timestamp"]
        rows.append(row)

    rows_file = io.StringIO()
    for row in rows:
        rows_file.write(json.dumps(row))
        rows_file.write("\n")
    rows_file.seek(0)

    return rows_file 


def bigquery_write(outputFile,table_id,writeTruncateFlag):

    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id, project=bq_project_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    if writeTruncateFlag:
        job_config.write_disposition = 'WRITE_TRUNCATE'

    job = client.load_table_from_file(outputFile, table_ref, job_config=job_config)
    job.result()
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
    print("{} - Loaded {} rows into {}:{}.".format(serverName,job.output_rows, dataset_id, table_id))




def main_flow():

    try:
        print("getting logs..")
        logs_details = get_logs()
        print("prepating bq rows..")
        outputFile = prepare_rows(logs_details)
        print("writing to bq..")
        bigquery_write(outputFile,table_id,False)
        timestamp = str(datetime.now(tz=timezone.utc).replace(tzinfo=None))
        f = open("last_timestamp.json","w")
        f.write(json.dumps({"timestamp" : timestamp}))
        f.close()

    except Exception as e:
        print('{} - {} : {}'.format(serverName,table_id,repr(e)),True)


main_flow()

