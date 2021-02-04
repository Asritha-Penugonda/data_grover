import boto3

AWS_ACCESS_KEY_ID = "YOUR_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_SECRET"

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

ec2 = session.resource('ec2', region_name='us-west-2')

instances = ec2.instances.filter(
    Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
for instance in instances:
    print(instance.id, instance.instance_type, instance.key_name, instance.private_ip_address, instance.public_dns_name ,instance.tags[0].get("Value"))
