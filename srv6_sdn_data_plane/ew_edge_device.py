#!/usr/bin/python

from __future__ import print_function

# General imports
import time
import os
import sys
import json
import threading
import logging
from filelock import FileLock
from argparse import ArgumentParser
from threading import Thread
from threading import Lock
# ipaddress dependencies
from ipaddress import IPv6Interface, IPv4Address
# NetworkX dependencies
import networkx as nx
from networkx.readwrite import json_graph
# SRv6 dependencies
from srv6_sdn_data_plane.southbound.grpc import sb_grpc_server
# pymerang dependencies
from pymerang.pymerang_client import PymerangDevice

# Global variables

# In our experiment we use srv6 as default password
DEFAULT_QUAGGA_PASSWORD = 'srv6'
# Port of zebra daemon
DEFAULT_ZEBRA_PORT = 2601
# Port of ospf6d daemon
DEFAULT_OSPF6D_PORT = 2606
# Supported southbound interfaces
SUPPORTED_SB_INTERFACES = ['gRPC']
# Logger reference
logger = logging.getLogger(__name__)
# Server ip and port
DEFAULT_GRPC_SERVER_IP = '::'
DEFAULT_GRPC_SERVER_PORT = 12345
# Debug option
SERVER_DEBUG = False
# Secure option
DEFAULT_SECURE = False
# Server certificate
DEFAULT_CERTIFICATE = 'cert_server.pem'
# Server key
DEFAULT_KEY = 'key_server.pem'
# Default southbound interface
DEFAULT_SB_INTERFACE = 'gRPC'
# Default verbose mode
DEFAULT_VERBOSE = False
# Default server IP
DEFAULT_PYMERANG_SERVER_IP = '::'
# Port of the gRPC server executing on the controller
DEFAULT_PYMERANG_SERVER_PORT = 50061
# Loopback IP address of the device
DEFAULT_PYMERANG_CLIENT_IP = 'fcff:1::1'
# Souce IP address of the NAT discovery
DEFAULT_NAT_DISCOVERY_CLIENT_IP = '0.0.0.0'
# Source port of the NAT discovery
DEFAULT_NAT_DISCOVERY_CLIENT_PORT = 0
# IP address of the NAT discovery
DEFAULT_NAT_DISCOVERY_SERVER_IP = '2000::1'
# Port number of the NAT discovery
DEFAULT_NAT_DISCOVERY_SERVER_PORT = 3478
# Config file
DEFAULT_CONFIG_FILE = '/tmp/config.json'
# Default interval between two keep alive messages
DEFAULT_KEEP_ALIVE_INTERVAL = 30
# Source port of the NAT discovery
DEFAULT_VXLAN_PORT = 4789
# File containing the token
DEFAULT_TOKEN_FILE = 'token'


class EWEdgeDevice(object):

    def __init__(self, sb_interface=DEFAULT_SB_INTERFACE,
                 secure=DEFAULT_SECURE,
                 key=DEFAULT_KEY, certificate=DEFAULT_CERTIFICATE,
                 grpc_server_ip=DEFAULT_GRPC_SERVER_IP,
                 grpc_server_port=DEFAULT_GRPC_SERVER_PORT,
                 quagga_password=DEFAULT_QUAGGA_PASSWORD,
                 ospf6d_port=DEFAULT_OSPF6D_PORT,
                 zebra_port=DEFAULT_ZEBRA_PORT,
                 pymerang_server_ip=DEFAULT_PYMERANG_SERVER_IP,
                 pymerang_server_port=DEFAULT_PYMERANG_SERVER_PORT,
                 nat_discovery_server_ip=DEFAULT_NAT_DISCOVERY_SERVER_IP,
                 nat_discovery_server_port=DEFAULT_NAT_DISCOVERY_SERVER_PORT,
                 nat_discovery_client_ip=DEFAULT_NAT_DISCOVERY_CLIENT_IP,
                 nat_discovery_client_port=DEFAULT_NAT_DISCOVERY_CLIENT_PORT,
                 config_file=DEFAULT_CONFIG_FILE,
                 token_file=DEFAULT_TOKEN_FILE,
                 keep_alive_interval=DEFAULT_KEEP_ALIVE_INTERVAL,
                 verbose=DEFAULT_VERBOSE):
        # Verbose mode
        self.VERBOSE = verbose
        if self.VERBOSE:
            print('*** Initializing controller variables')
        # Initialize variables
        #
        # Southbound interface
        self.sb_interface = sb_interface
        # Secure mode
        self.secure = secure
        # Server key
        self.key = key
        # Server certificate
        self.certificate = certificate
        # IP of the gRPC server
        self.grpc_server_ip = grpc_server_ip
        # Port of the gRPC server
        self.grpc_server_port = grpc_server_port
        # IP of the pymerang server
        self.pymerang_server_ip = pymerang_server_ip
        # Port of the pymerang server
        self.pymerang_server_port = pymerang_server_port
        # Quagga password
        self.quagga_password = quagga_password
        # ospf6d port
        self.ospf6d_port = ospf6d_port
        # zebra port
        self.zebra_port = zebra_port
        # IP address of the gRPC pymerang server
        self.pymerang_server_ip = pymerang_server_ip
        # Port on which the gRPC pymerang server is listening
        self.pymerang_server_port = pymerang_server_port
        # IP address of the NAT discovery server
        self.nat_discovery_server_ip = nat_discovery_server_ip
        # Port of the NAT discovery server
        self.nat_discovery_server_port = nat_discovery_server_port
        # IP address used by the NAT discovery client
        self.nat_discovery_client_ip = nat_discovery_client_ip
        # Port used by the NAT discovery client
        self.nat_discovery_client_port = nat_discovery_client_port
        # Config file
        self.config_file = config_file
        # Token file
        self.token_file = token_file
        # Keep alive interval
        self.keep_alive_interval = keep_alive_interval
        # Print configuration
        if self.VERBOSE:
            print()
            print('Configuration')
            # print('*** Quagga password: %s' % self.quagga_password)
            print('*** Zebra port: %s' % self.zebra_port)
            print('*** OSPF6D port: %s' % self.ospf6d_port)
            if self.secure:
                print('*** Secure mode: enabled')
                print('*** Key: %s' % self.key)
                print('*** Certificate: %s' % self.certificate)
            else:
                print('*** Secure mode: disabled')
            print('*** Selected southbound interface: %s' % self.sb_interface)
            print('*** gRPC server IP: %s' % self.grpc_server_ip)
            print('*** gRPC server port: %s' % self.grpc_server_port)
            print('*** pymerang server IP: %s' % self.pymerang_server_ip)
            print('*** pymerang server port: %s' % self.pymerang_server_port)
            print()

    # Start registration client
    def start_registration_client(self):
        logging.info('*** Starting registration client')
        registration_client = PymerangDevice(
            server_ip=self.pymerang_server_ip,
            server_port=self.pymerang_server_port,
            nat_discovery_server_ip=self.nat_discovery_server_ip,
            nat_discovery_server_port=self.nat_discovery_server_port,
            nat_discovery_client_ip=self.nat_discovery_client_ip,
            nat_discovery_client_port=self.nat_discovery_client_port,
            config_file=self.config_file,
            token_file=self.token_file,
            keep_alive_interval=self.keep_alive_interval,
            debug=self.VERBOSE)
        # Run registration client
        registration_client.run()

    # Run the EveryWAN Edge Device

    def run(self):
        if self.VERBOSE:
            print('*** Starting the EveryWAN Edge Device')
        # Start registration server
        thread = Thread(
            target=self.start_registration_client
        )
        # thread.daemon = True
        thread.start()
        if self.VERBOSE:
            print('*** Starting gRPC Southbound server')
        # Start southbound gRPC server
        # This will block until the server is terminated
        # because of an exception or a request coming from a
        # southbound client
        sb_grpc_server.start_server(
            grpc_ip=self.grpc_server_ip,
            grpc_port=self.grpc_server_port,
            secure=self.secure,
            key=self.key,
            certificate=self.certificate,
            quagga_password=self.quagga_password,
            zebra_port=self.zebra_port,
            ospf6d_port=self.ospf6d_port,
        )


# Parse arguments
def parseArguments():
    # Get parser
    parser = ArgumentParser(description='SRv6 Controller')
    # Enable debug logs
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Activate debug logs')
    # Verbose mode
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest='verbose', default=False,
                        help='Enable verbose mode')
    # Southbound interface
    parser.add_argument('--sb-interface', action='store',
                        dest='sb_interface', default=DEFAULT_SB_INTERFACE,
                        help='Select a southbound interface '
                        'from this list: %s' % SUPPORTED_SB_INTERFACES)
    # IP address of the southbound gRPC server
    parser.add_argument('--grpc-server-ip', dest='grpc_server_ip',
                        action='store', default=DEFAULT_GRPC_SERVER_IP,
                        help='IP of the southbound gRPC server')
    # Port of the southbound gRPC server
    parser.add_argument('--grpc-server-port', dest='grpc_server_port',
                        action='store', default=DEFAULT_GRPC_SERVER_PORT,
                        help='Port of the southbound gRPC server')
    # Enable secure mode
    parser.add_argument('-s', '--secure', action='store_true',
                        default=DEFAULT_SECURE, help='Activate secure mode')
    # Server certificate
    parser.add_argument('--server-cert', dest='server_cert',
                        action='store', default=DEFAULT_CERTIFICATE,
                        help='Server certificate file')
    # Server key
    parser.add_argument('--server-key', dest='server_key',
                        action='store', default=DEFAULT_KEY,
                        help='Server key file')
    # Password used to log in to ospf6d and zebra daemons
    parser.add_argument('--quagga-password', action='store',
                        dest='quagga_password', default=DEFAULT_QUAGGA_PASSWORD,
                        help='Password used to log in to ospf6d and zebra daemons')
    # Port used to log in to zebra daemon
    parser.add_argument('--zebra-port', action='store',
                        dest='zebra_port', default=DEFAULT_ZEBRA_PORT,
                        help='Port used to log in to zebra daemon')
    # Port used to log in to ospf6d daemon
    parser.add_argument('--ospf6d-port', action='store',
                        dest='ospf6d_port', default=DEFAULT_OSPF6D_PORT,
                        help='Port used to log in to ospf6d daemon')
    # IP address of the gRPC registration server
    parser.add_argument(
        '-i', '--pymerang-server-ip', dest='pymerang_server_ip',
        default=DEFAULT_PYMERANG_SERVER_IP, help='Pymerang server IP address'
    )
    # Port of the gRPC server
    parser.add_argument(
        '-p', '--pymerang-server-port', dest='pymerang_server_port',
        default=DEFAULT_PYMERANG_SERVER_PORT, help='Pymerang server port'
    )
    # IP address of the NAT discovery server
    parser.add_argument(
        '-n', '--nat-discovery-server-ip', dest='nat_discovery_server_ip',
        default=DEFAULT_NAT_DISCOVERY_SERVER_IP, help='NAT discovery server IP'
    )
    # Port of the NAT discovery server
    parser.add_argument(
        '-m', '--nat-discovery-server-port', type=int,
        dest='nat_discovery_server_port',
        default=DEFAULT_NAT_DISCOVERY_SERVER_PORT,
        help='NAT discovery server port'
    )
    # IP address used by the NAT discoery client
    parser.add_argument(
        '-l', '--nat-discovery-client-ip', dest='nat_discovery_client_ip',
        default=DEFAULT_NAT_DISCOVERY_CLIENT_IP, help='NAT discovery client IP'
    )
    # Port used by the NAT discovery client
    parser.add_argument(
        '-o', '--nat-discovery-client-port', type=int,
        dest='nat_discovery_client_port',
        default=DEFAULT_NAT_DISCOVERY_CLIENT_PORT,
        help='NAT discovery client port'
    )
    # File containing the configuration of the device
    parser.add_argument(
        '-c', '--config-file', dest='config_file',
        default=DEFAULT_CONFIG_FILE, help='Config file'
    )
    # Interval between two consecutive keep alive messages
    parser.add_argument(
        '-k', '--keep-alive-interval', dest='keep_alive_interval',
        default=DEFAULT_KEEP_ALIVE_INTERVAL,
        help='Interval between two consecutive keep alive'
    )
    # Interval between two consecutive keep alive messages
    parser.add_argument(
        '-t', '--token-file', dest='token_file',
        default=DEFAULT_TOKEN_FILE,
        help='File containing the token used for the authentication'
    )
    # Parse input parameters
    args = parser.parse_args()
    # Done, return
    return args


def _main():
    # Let's parse input parameters
    args = parseArguments()
    # Verbose mode
    verbose = args.verbose
    # Southbound interface
    sb_interface = args.sb_interface
    # Setup properly the logger
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    # Setup properly the secure mode
    if args.secure:
        secure = True
    else:
        secure = False
    # gRPC server IP
    grpc_server_ip = args.grpc_server_ip
    # gRPC server port
    grpc_server_port = args.grpc_server_port
    # pymerang server IP
    pymerang_server_ip = args.pymerang_server_ip
    # pymerang server port
    pymerang_server_port = args.pymerang_server_port
    # Server certificate
    certificate = args.server_cert
    # Quagga password
    quagga_password = args.quagga_password
    # ospf6d port
    ospf6d_port = args.ospf6d_port
    # zebra port
    zebra_port = args.zebra_port
    # Server key
    key = args.server_key
    # Pymerang server IP
    pymerang_server_ip = args.pymerang_server_ip
    # Pymerang server port
    pymerang_server_port = args.pymerang_server_port
    # NAT discovery server IP
    nat_discovery_server_ip = args.nat_discovery_server_ip
    # NAT discovery server port
    nat_discovery_server_port = args.nat_discovery_server_port
    # NAT discovery client IP
    nat_discovery_client_ip = args.nat_discovery_client_ip
    # NAT discovery client port
    nat_discovery_client_port = args.nat_discovery_client_port
    # Config file
    config_file = args.config_file
    # Interval between two consecutive keep alive messages
    keep_alive_interval = args.keep_alive_interval
    # File containing the token used for the authentication
    token_file = args.token_file
    #
    # Check debug level
    SERVER_DEBUG = logger.getEffectiveLevel() == logging.DEBUG
    logger.info('SERVER_DEBUG:' + str(SERVER_DEBUG))
    # Check interfaces file, dataplane and gRPC client paths
    if sb_interface not in SUPPORTED_SB_INTERFACES:
        utils.print_and_die('Error: %s interface not yet supported or invalid\n'
                            'Supported southbound interfaces: %s' % (sb_interface, SUPPORTED_SB_INTERFACES))
    # Create a new EveryWAN Edge Device
    ew_edge_device = EWEdgeDevice(
        sb_interface=sb_interface,
        secure=secure,
        key=key,
        certificate=certificate,
        grpc_server_ip=grpc_server_ip,
        grpc_server_port=grpc_server_port,
        quagga_password=quagga_password,
        ospf6d_port=ospf6d_port,
        zebra_port=zebra_port,
        pymerang_server_ip=pymerang_server_ip,
        pymerang_server_port=pymerang_server_port,
        nat_discovery_server_ip=nat_discovery_server_ip,
        nat_discovery_server_port=nat_discovery_server_port,
        nat_discovery_client_ip=nat_discovery_client_ip,
        nat_discovery_client_port=nat_discovery_client_port,
        config_file=config_file,
        token_file=token_file,
        keep_alive_interval=keep_alive_interval,
        verbose=verbose
    )
    # Start the edge device
    ew_edge_device.run()


if __name__ == '__main__':
    _main()