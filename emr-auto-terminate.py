import json
import os
from pprint import pprint
import boto3
from datetime import datetime, timezone


# cluster up time threshold in minutes
# passing time into env
#CLUSTER_UP_TIME_THRESHOLD = int(os.environ['CLUSTER_UP_TIME_THRESHOLD'])
# passing value 
CLUSTER_UP_TIME_THRESHOLD = 60


def lambda_handler(event, context):
    lambda_start_time = datetime.now(timezone.utc)
    session = boto3.session.Session()
    secret_client = session.client(service_name='secretsmanager',region_name='us-east-1')

# passing aws secret-key and aws-access-key
    get_secret_value_access_key = secret_client.get_secret_value(SecretId='<aws-access-key>')
    get_secret_value_secret_key = secret_client.get_secret_value(SecretId='<aws-secret-key>')


    # connecting to emr
    emr = boto3.client('emr',aws_access_key_id=get_secret_value_access_key['SecretString'],aws_secret_access_key= get_secret_value_secret_key['SecretString'])

    clustersToShutDown = []

    # fetching list of running clusters
    runningCluster = emr.list_clusters(
        ClusterStates=[
            'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
        ]
    )

    # check any steps running still else shutdown the cluster if the last step got terminated before 30 miutes
    for cluster in runningCluster['Clusters']:
        tags = emr.describe_cluster(ClusterId=cluster['Id'])['Cluster']['Tags']
        for tag in tags:
            if tag['Key'] == 'CLUSTERTYPE' and tag['Value'] == 'async':
                runningSteps = emr.list_steps(ClusterId=cluster['Id'], StepStates=[
                    'PENDING', 'CANCEL_PENDING', 'RUNNING'
               ])
                if len(runningSteps['Steps']) == 0:
                    allSteps = emr.list_steps(ClusterId=cluster['Id'])
                    if len(allSteps['Steps']) != 0:
                        datetime_end = allSteps['Steps'][0]['Status']['Timeline']['EndDateTime']
                        datetime_now = datetime.now(timezone.utc)
                        minutes_diff = (datetime_now - datetime_end).total_seconds() / 60
                        pprint(minutes_diff)
                        if minutes_diff > CLUSTER_UP_TIME_THRESHOLD:
                            pprint("Production cluster " + cluster['Id'] + " is running idle for more than threshold time of " + str(CLUSTER_UP_TIME_THRESHOLD) + " minutes")
                            clustersToShutDown.append(cluster['Id'])
                    else:
                        cluster_start_time = cluster['Status']['Timeline']['CreationDateTime']
                        datetime_now = datetime.now(timezone.utc)
                        minutes_diff = (datetime_now - cluster_start_time).total_seconds() / 60
                        pprint("minutes" + str(minutes_diff))
                        if minutes_diff > CLUSTER_UP_TIME_THRESHOLD:
                            clustersToShutDown.append(cluster['Id'])
                break

    if len(clustersToShutDown) != 0:
        pprint("Production Clusters to be shut down: " + str(clustersToShutDown))
        response = emr.terminate_job_flows(JobFlowIds=clustersToShutDown)
        pprint("Production Clusters shutdown response: " + str(response))
    else:
        pprint("No need for any production cluster termination. Exiting gracefully...")
