# Overview

This directory contains the descriptions and tools to use AWS CloudFormation
to setup a VPC (with subnets, security groups, etc.) and Redshift cluster (with
parameter groups etc.) to run the ETL.

# Prerequisites

A key pair has to be created beforehand.

# Resources

Note: All resources created will have a tag of `user:project` with value `data-warehouse` to easily
build a Resource Group and to track costs in Billing. Also, names will start with `dw-` to make them
easy to find in the AWS Console, which is to say that the `Name` tag is set as much as possible.

## VPC (vpc.yaml)

Default IP address range
* IPv4 CIDR: `10.10.0.0/16` (configurable, might need alternate)
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

Endpoint to S3 with read access to anything but write access only to some buckets:

Example:
```JSON
{
    "Version": "2012-10-17",
    "Id": "Policy1472827094035",
    "Statement": [
        {
            "Sid": "S3ReadOnlyEverything",
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
            "Sid": "S3ReadWrite",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::<object store>/*"
            ]
        }
    ]
}
```

This could be improved further since we only need write permissions to `data` folders.

(And read-access to other buckets is important, e.g. for starting up EMR.)

### Subnets

#### Public

The data warehouse will be in the "public" subnet.

Region: `us-east-1b`
IPv4 CIDR: `10.10.0.0/22`
Routing Table:
* local: `10.10.0.0/16`
* all: internet gateway
* service endpoint for S3

#### Private

All instances will be running in the "private" subnet.

Region: `us-east-1b`
IPv4 CIDR: `10.10.8.0/22`
Routing Table:
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

#### Access to data warehouse

This references all the prior security groups

* Name prefix: `dw-sg-redshift-public`
* Access to Redshift (5439) from:
    * IP addresses from Looker, see list at https://docs.looker.com/setup-and-management/enabling-secure-db
    * IP addresses from Heap, see https://docs.heapanalytics.com/docs/heap-sql-connection-requirements
    * Security group for "managed master"
    * Security group for "lambda (Tallboy)"
    * Security group for EC2 instances
    * IP addresses from our office
    * IP addresses from other applications
    
### Roles and instance profiles (for EMR)

* Instance role for EMR cluster
* Instance profile for EMR cluster nodes

## Redshift (cluster.yaml)

### Subnet group

Use the public subnet setup for the VPC

### Parameter group

Uses all defaults except:
* Set `require_ssl` to `true`

(TODO: Set `query_concurrency` to value matching the hardware.)

### Cluster

* Use 2 `dc1.8xlarge` nodes in production, 2 `ds2.xlarge` nodes in development, and 2 `dc1.large` for experiments.
* Use the subnet group and parameter group from above.

# Installation

The commands below assume that your role has the necessary privileges for CloudFormation.

## Creating the VPC using CloudFormation

### Creating the stack

```bash
cloudformation/create_dw_vpc.sh dev your-object-store-dev
```

If you want to specify a specific IP address:
```bash
cloudformation/create_dw_vpc.sh dev your-object-store-dev ParameterKey=WhitelistCIDR1,ParameterValue=192.168.1.1/32
```

### Updating the stack

```bash
cloudformation/update_dw_vpc.sh dev your-object-store-dev
```

### Deleting the stack

```bash
cloudformation/delete_dw_vpc.sh dev
```

## Creating the Redshift Cluster using CloudFormation

The cluster expects the stack for the VPC to export its values.
```bash
aws cloudformation list-exports
```

### Create, update, delete

Commands are similar to setting up the VPC:
```bash
cloudformation/create_dw_cluster.sh dev your-object-store-dev
cloudformation/update_dw_cluster.sh dev your-object-store-dev
cloudformation/delete_dw_cluster.sh dev
```

## Publicizing the new cluster

Run the update script to create resource records in DNS to point to the new cluster
```bash
cloudformation/update_dns.py cluster-identifier hosted-zone-name hostname
```

# Future improvements

* Add CloudFormation configuration for SNS topics used in data pipeline
* Create a role to have permission to run cloudformation (bootstrap role)
* Use notifications during CloudFormation ("ETL News")