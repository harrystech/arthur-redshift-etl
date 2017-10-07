# Overview

This directory contains the descriptions and tools to use AWS CloudFormation to setup
a VPC (with subnets, security groups, etc.)
and Redshift cluster (with parameter groups etc.) to run the ETL.

# Prerequisites

## Key pairs

A key pair has to be created beforehand.
The name of the keypair must be `dw-{ENV}-keypair` where the `ENV` is the environment that you choose for the stacks, such as `prod` or `dev`.

## S3 buckets

There are two different bucket that we expect to get used.
Both buckets must be setup before you can start running the ETL and are not part of the CloudFormation setup.
* The `object-store` is used for temporary data around schemas, data files and configuration.
* The `data-lake` maybe used to load static data from or to unload cluster data into.

# Resources

Note:
All resources created will have a tag of `user:project` with value `data-warehouse` to easily build a Resource Group and to track costs in Billing.
Also, names will start with `dw-` to make them easy to find in the AWS Console, which is to say that the `Name` tag is set as much as possible.

## Data Warehouse VPC (`dw_vpc.yaml`)

Default IP address range
* IPv4 CIDR: `10.10.0.0/16` (configurable, but make sure the subnets are within the VPC's IP range)
* Network ACL: Allow all traffic
* DNS resolution: yes
* DNS hostnames: yes

### DHCP

DHCP options:
* `domain-name = ec2.internal`
* `domain-name-servers = AmazonProvidedDNS`

### Internet gateway

Yes

### NAT gateway

Yes, will be used by private subnet.  Needs to be associated with an elastic IP (which is a requirement
that doesn't come in until we run Lambdas to load data into tables in Redshift).

### Service endpoint

Endpoint to S3 with read access to anything and write access only to some buckets:

Example:
```JSON
{
    "Version": "2012-10-17",
    "Id": "Policy1472827094035",
    "Statement": [
        {
            "Sid": "ReadOnlyEverything",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:Describe*",
                "s3:Get*",
                "s3:List*"
            ],
            "Resource": "*"
        },
        {
            "Sid": "WriteSelective",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:DeleteObject*",
                "s3:PutObject*"
            ],
            "Resource": [
                "arn:aws:s3:::<object store>/*",
                "arn:aws:s3:::<data lake>/*"
            ]
        }
    ]
}
```

(And read-access to other buckets is important, e.g. for starting up EMR.)

### Subnets

#### Public

The data warehouse and EMR cluster will be in the "public" subnet.

* Region: `us-east-1`
* IPv4 CIDR: `10.10.0.0/22`
* Routing Table:
    * local: `10.10.0.0/16`
    * all: internet gateway
    * service endpoint for S3

(The EMR cluster is "public" to make it easy to login from the office and debug if that's ever necessary.)

#### Private

Any Lambda instances will be running in the "private" subnet.

* Region: `us-east-1` (should always match public subnet)
* IPv4 CIDR: `10.10.8.0/22`
* Routing Table:
    * local: `10.10.0.0/16`
    * all: NAT
    * service endpoint for S3

### Security groups

There are multiple security groups that define who may talk to whom. Note that the "managed" security
groups are used by EMR and will be automatically updated when clusters are running.

#### Access to master and core nodes: "managed" security groups

* Name prefixes: `dw-sg-master` and `dw-sg-core`
* They need to allow traffic from each other.  Mostly, they must exist so that EMR can configure them.
* Should also allow ssh access from other EC2 nodes

#### Access from Lambda (Tallboy)

* Name prefix: `dw-sg-lambda`
* There are no inbound or outbound rules. This security group is only used by reference.

#### Access to EC2 instances

* Name prefix: `dw-sg-ec2`
* Access using SSH from within the same security group and to the master node and the Redshift cluster.

#### Access from our office IP addresses

* Name prefix: `dw-sg-office`
* Access using SSH from whitelisted IP address ranges
* Note that parameters for whitelisting are ignored if the IP range is set to `0.0.0.0/0` so as to avoid
a "from anywhere" access

#### Access to data warehouse

This references all the prior security groups

* Name prefix: `dw-sg-redshift-public`
* Access to Redshift (5439) from:
    * IP addresses from Looker, see list at [Enabling secure DB](https://docs.looker.com/setup-and-management/enabling-secure-db)
    * IP addresses from Heap, see [SQL connection requirements](https://docs.heapanalytics.com/docs/heap-sql-connection-requirements)
    * Security group for "managed master"
    * Security group for "lambda (Tallboy)"
    * Security group for EC2 instances
    * IP addresses from our office
    * IP addresses from other applications
    
### Roles and instance profiles (for EMR)

* Instance role for EMR cluster
* Instance profile for EMR cluster nodes

## Data Warehouse Redshift cluster (`dw_cluster.yaml`)

### Subnet group

Use the public subnet setup for the VPC.

### Parameter group

Uses all defaults except:
* Set `require_ssl` to `true`

(TODO: Set `query_concurrency` to value matching the hardware.)

### Cluster

* Use 2 `dc1.8xlarge` nodes in production, 2 `ds2.xlarge` nodes in development, and 2 `dc1.large` for experiments.
* Use the subnet group and parameter group from above.

# Installation

The commands below assume that your role has the necessary privileges for CloudFormation.

## Creating and updating the VPC using CloudFormation

### Creating the stack

```bash
cloudformation/create_dw_vpc.sh dev ObjectStore=your-object-store DataLake=data-lake
```

If you want to specify a specific IP address:
```bash
cloudformation/create_dw_vpc.sh dev ObjectStore=your-object-store DataLake=data-lake WhitelistCIDR1=192.168.1.1/32
```

### Updating the stack

Examples for updating the stack:

```bash
cloudformation/update_dw_vpc.sh dev ObjectStore=UsePreviousValue DataLake=UsePreviousValue
```

Be careful here if you specified one of the optional parameters: you have to specify it here again!

### Deleting the stack

```bash
cloudformation/delete_dw_vpc.sh dev
```

Note that you have to manually dis-associate the deleted VPC from a private hosted zone
if you've previously used `update_dns.py` to store the public and private IP addresses
in hosted zones.

## Creating and updating the Redshift Cluster using CloudFormation

The cluster expects the stack for the VPC to export its values.
If you are curious, you should check them:
```bash
aws cloudformation list-exports
```

### Create, update, and delete

Commands are similar to setting up the VPC but expect now the VPC stack as parameter.
Remember that the name of the stack is `dw-vpc-{ENV}` and the full name, not just `{ENV}`
is expected in the `VpcStackName` parameter.
```bash
cloudformation/create_dw_cluster.sh dev VpcStackName=dw-vpc-dev MasterUsername=admin MasterUserPassword=secure-pwd

cloudformation/update_dw_cluster.sh dev VpcStackName=UsePreviousValue \
    MasterUsername=UsePreviousValue MasterUserPassword=UsePreviousValue \
    NumberOfNodes=3

cloudformation/delete_dw_cluster.sh dev
```

### Adding enhanced VPC routing

Since this cannot be specified (at the moment) as part of CloudFormation, run this manually:
```bash
aws redshift modify-cluster --enhanced-vpc-routing --cluster-identifier "<your cluster identifier>"
```

## Publicizing the new cluster

Run the update script to create resource records in DNS to point to the new cluster.
You will need the `cluster-identifier` from the list of exported values from the cluster stack.
```bash
cloudformation/update_dns.py "<cluster-identifier>" "<hosted-zone-name>" hostname
```

# Next steps

## Creating default roles

Until we move the roles and profiles for EMR and EC2 into the settings files, some default
roles have to be created using the CLI:
```
aws datapipeline create-default-roles
aws emr create-default-roles
```

## Setting up the cluster with a database and users

Since the cluster was just brought up using CloudFormation,
it is missing users and a "database" to use for the ETL.

Make sure that passwords exist in your `~.pgpass` file,
write a `config/credentials.sh` file,
then run
```
arthur.py initialize --force --with-user-creation
```

# Future improvements

* Add CloudFormation configuration for SNS topics used in data pipeline
* Create a role to have permission to run CloudFormation (bootstrap role)
* Use notifications during CloudFormation ("ETL News")
* Automatically associate the VPC with the private hosted zone
* Grant access to DynamoDB table to Redshift copy role
