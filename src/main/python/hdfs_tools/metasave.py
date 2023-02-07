"""
Class used to get information on Namenode's primary data structures
"""
import re
import socket

class Metasave():
    """Class used to parse metasave file."""
    def __init__(
            self,
            metasave_path,
            replica,
            datanode=None):
        """
        Instantiate the Metasave

        Parameters
        ----------
        metasave_path:
            the path to the metasave file
        """
        self.metasave_path = metasave_path
        self.replica = {
            'live': replica['live'] if 'live' in replica else r'\d+',
            'decommissioned': replica['decommissioned'] if 'decommissioned' in replica else r'\d+',
            'corrupt': replica['corrupt'] if 'corrupt' in replica else r'\d+',
            'excess': replica['excess'] if 'excess' in replica else r'\d+'
        }
        self.datanode = datanode
        self.replica_count_by_host = {}
        self.blocks = []

    @classmethod
    def from_block(cls, metasave_path, args):
        """
        Instantiate the Metasave from the block sub-command arguments

        Parameters
        ----------
        metasave_path:
            the path to the metasave file
        args:
            the script command line arguments
        """
        if args.no_live_replicas is not None:
            return cls(metasave_path, {'live':'0', 'decommissioned':'1'}, args.no_live_replicas)
        return cls(metasave_path, {'live':'[12]', 'decommissioned':'1'}, args.under_replicated)

    @classmethod
    def from_count(cls, metasave_path, args):
        """
        Instantiate the Metasave from the count sub-command arguments

        Parameters
        ----------
        metasave_path:
            the path to the metasave file
        args:
            the script command line arguments
        """
        replica = {}
        if args.live is not None:
            replica['live'] = str(args.live)
        if args.decommissioned is not None:
            replica['decommissioned'] = str(args.decommissioned)
        if args.corrupt is not None:
            replica['corrupt'] = str(args.corrupt)
        if args.excess is not None:
            replica['excess'] = str(args.excess)
        return cls(metasave_path, replica)

    def parse(self):
        """
        Parse metasave file
        """
        ip_addr_to_host = {}
        replica_regexp = re.compile(
                r'^([^:]+):\s+\S+\s+\(replicas: l: '
            + self.replica['live']
            + r' d: '
            + self.replica['decommissioned']
            + r' c: '
            + self.replica['corrupt']
            + r' e: '
            + self.replica['excess']
            + r'\)(.+)$'
        )
        datanodes_regexp = re.compile(r'\s+(\S+):50010(?:\(decommissioned\))?\s+:')
        with open(self.metasave_path, encoding='utf-8') as f_ms:
            for line in f_ms:
                match_replica = replica_regexp.search(line)
                if match_replica:
                    datanodes_part = match_replica[2]
                    datanodes_ip_addr = datanodes_regexp.findall(datanodes_part)
                    for ip_addr in datanodes_ip_addr:
                        if ip_addr in ip_addr_to_host:
                            host = ip_addr_to_host[ip_addr]
                            self.replica_count_by_host[host] += 1
                        else:
                            host = socket.gethostbyaddr(ip_addr)[0]
                            ip_addr_to_host[ip_addr] = host
                            self.replica_count_by_host[host] = 1

                        if self.datanode == host:
                            block_hdfs_path = match_replica[1]
                            self.blocks.append(block_hdfs_path)

    def display_replica_count_by_host(self):
        """
        Display the number of HDFS blocks which match the defined replica by host
        """
        for host, count in sorted(self.replica_count_by_host.items(), key=lambda kv: kv[0]):
            print(f'{host} --> {count}')

    def display_host_replica_status(self):
        """
        Display the HDFS blocks replica status for the datanode in parameter
        """
        for block in self.blocks:
            print(block)
