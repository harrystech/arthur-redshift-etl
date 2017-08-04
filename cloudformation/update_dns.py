#! /usr/bin/env python3

"""
Usage: update_dns.py cluster-identifier hosted-zone-name hostname

Update public hosted zone with public IP address and private hosted zone with private IP address
of the Redshift cluster.
"""

import sys

import boto3
import jmespath


def get_redshift_leader_info(cluster_identifier):
    """Return dict with 'PrivateIPAddress' and 'PublicIPAddress' for the leader node"""
    client = boto3.client("redshift")
    response = client.describe_clusters(ClusterIdentifier=cluster_identifier)
    values = jmespath.search("""Clusters[0].ClusterNodes[?NodeRole == 'LEADER'] | [0]""", response)
    return values


def get_hosted_zones(hosted_zone_name):
    """Return list of dicts with 'HostedZoneId' value and 'PrivateZone' flag"""
    client = boto3.client("route53")
    zones = client.list_hosted_zones_by_name(DNSName=hosted_zone_name, MaxItems="2")
    names = ["HostedZoneId", "PrivateZone"]
    values = jmespath.search("""HostedZones[].[Id, Config.PrivateZone]""", zones)
    return [dict(zip(names, zone_values)) for zone_values in values]


def update_dns_records(cluster_identifier, hosted_zone_name, hostname):
    """
    Add A records for hostname in private and public hosted zones to point to the Redshift cluster.
    """
    leader_node = get_redshift_leader_info(cluster_identifier)
    zones = get_hosted_zones(hosted_zone_name)
    fqdn = '.'.join((hostname, hosted_zone_name))

    client = boto3.client("route53")

    for hosted_zone in zones:
        hosted_zone_id = hosted_zone["HostedZoneId"]
        if hosted_zone["PrivateZone"]:
            ip_address = leader_node["PrivateIPAddress"]
        else:
            ip_address = leader_node["PublicIPAddress"]
        print("Updating {} with A record for {} to {}".format(hosted_zone_id, fqdn, ip_address))
        response = client.change_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            ChangeBatch={
                'Changes': [
                    {
                        'Action': 'UPSERT',
                        'ResourceRecordSet': {
                            'Name': fqdn,
                            'Type': 'A',
                            'TTL': 900,
                            'ResourceRecords': [
                                {
                                    'Value': ip_address
                                },
                            ]
                        }
                    }
                ]
            }
        )
        print(response["ChangeInfo"])


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    update_dns_records(sys.argv[1], sys.argv[2], sys.argv[3])
