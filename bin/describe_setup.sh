#! /bin/bash

# Gather all information using aws CLI

VPC_ID=${1?"Missing: VPC ID"}

aws ec2 describe-vpcs --vpc-ids "$VPC_ID"

aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC_ID"

aws ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=$VPC_ID"

aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID"

aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID"

# Describe addresses (elastic IPs)?
# Describe key pairs?

# aws redshift describe-cluster-parameter-groups
# aws redshift describe-cluster-parameters
# aws redshift describe-cluster-subnet-groups
# aws redshift describe-clusters
