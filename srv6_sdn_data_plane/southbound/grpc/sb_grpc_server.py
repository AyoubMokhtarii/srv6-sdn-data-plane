#!/usr/bin/python


from __future__ import absolute_import, division, print_function

# General imports
from argparse import ArgumentParser
import socket
import logging
import grpc
import telnetlib
import sys
from concurrent import futures
from pyroute2 import IPRoute
from pyroute2 import IPDB
from socket import AF_INET
from socket import AF_INET6
from socket import AF_UNSPEC
from pyroute2.netlink.exceptions import NetlinkError
from pyroute2.netlink.rtnl import ndmsg

if sys.version_info >= (3, 0):
    from pyroute2.netlink.nlsocket import Stats  # noqa F401


# ################## Setup these variables ##################

# Path of the proto files
# PROTO_FOLDER = '../../../srv6-sdn-proto/'

# ###########################################################

# Adjust relative paths
# script_path = os.path.dirname(os.path.abspath(__file__))
# PROTO_FOLDER = os.path.join(script_path, PROTO_FOLDER)

# Check paths
# if PROTO_FOLDER == '':
#    print('Error: Set PROTO_FOLDER variable '
#          'in sb_grpc_server.py')
#    sys.exit(-2)
# if not os.path.exists(PROTO_FOLDER):
#    print('Error: PROTO_FOLDER variable in sb_grpc_server.py '
#          'points to a non existing folder\n')
#    sys.exit(-2)

# Add path of proto files
# sys.path.append(PROTO_FOLDER)

# SRv6 dependencies
from srv6_sdn_proto import srv6_manager_pb2_grpc
from srv6_sdn_proto import srv6_manager_pb2
from srv6_sdn_proto import status_codes_pb2
from srv6_sdn_proto import network_events_listener_pb2
from srv6_sdn_proto import network_events_listener_pb2_grpc
from srv6_sdn_proto import gre_interface_pb2
# from srv6_sdn_proto import ip_tunnel_interface_pb2
from srv6_sdn_proto.ip_tunnel_interface_pb2 import IPTunnelType
# from .sb_grpc_utils import InvalidAddressFamilyError
from .sb_grpc_utils import InvalidAddressFamilyError, getAddressFamily

# STAMP Support
ENABLE_STAMP_SUPPORT = True

# Import modules required by STAMP
if ENABLE_STAMP_SUPPORT:
    from srv6_delay_measurement import sender as stamp_sender_module
    from srv6_delay_measurement import reflector as stamp_reflector_module

# Global variables definition

# Quagga configuration params
DEFAULT_ZEBRA_PORT = 2601
DEFAULT_OSPF6D_PORT = 2606
DEFAULT_QUAGGA_PASSWORD = 'srv6'

# RT_SCOPES represents the scope of the area where an address is valid
# The scopes available are defined in /etc/iproute2/RT_SCOPES
RT_SCOPES = {
    'global': 0,
    'nowhere': 255,
    'host': 254,
    'link': 253
}

EVENT_TYPES = {
    'CONNECTION_ESTABLISHED': (
        network_events_listener_pb2.NetworkEvent.CONNECTION_ESTABLISHED
    ),
    'INTF_UP': network_events_listener_pb2.NetworkEvent.INTF_UP,
    'INTF_DOWN': network_events_listener_pb2.NetworkEvent.INTF_DOWN,
    'INTF_DEL': network_events_listener_pb2.NetworkEvent.INTF_DEL,
    'NEW_ADDR': network_events_listener_pb2.NetworkEvent.NEW_ADDR,
    'DEL_ADDR': network_events_listener_pb2.NetworkEvent.DEL_ADDR
}


class RTM_TYPES:
    RTN_UNSPEC = 0
    RTN_UNICAST = 1       # Gateway or direct route
    RTN_LOCAL = 2         # Accept locally
    RTN_BROADCAST = 3     # Accept locally as broadcast, send as broadcast
    RTN_ANYCAST = 4       # Accept locally as broadcast, but send as unicast
    RTN_MULTICAST = 5     # Multicast route
    RTN_BLACKHOLE = 6     # Drop
    RTN_UNREACHABLE = 7   # Destination is unreachable
    RTN_PROHIBIT = 8      # Administratively prohibited
    RTN_THROW = 9         # Not in this table
    RTN_NAT = 10          # Translate this address
    RTN_XRESOLVE = 11     # Use external resolver


ROUTE_TYPES = {
    'unicast': RTM_TYPES.RTN_UNICAST,
    'local': RTM_TYPES.RTN_LOCAL,
    'broadcast': RTM_TYPES.RTN_BROADCAST,
    'multicast': RTM_TYPES.RTN_MULTICAST,
    'throw': RTM_TYPES.RTN_THROW,
    'unreachable': RTM_TYPES.RTN_UNREACHABLE,
    'prohibit': RTM_TYPES.RTN_PROHIBIT,
    'blackhole': RTM_TYPES.RTN_BLACKHOLE,
    'nat': RTM_TYPES.RTN_NAT
}

# Whether to use Zebra or not for address configuration
USE_ZEBRA = False

# Server reference
grpc_server = None
# Netlink socket
ip_route = None
ipdb = None
# Non-loopback interfaces
interfaces = []
# Mapping interface to ids
idxs = {}
# Logger reference
logger = logging.getLogger(__name__)
# Server ip and port
DEFAULT_GRPC_IP = '::'
DEFAULT_GRPC_PORT = 12345
# Debug option
SERVER_DEBUG = False
# Secure option
DEFAULT_SECURE = False
# Server certificate
DEFAULT_CERTIFICATE = 'cert_server.pem'
# Server key
DEFAULT_KEY = 'key_server.pem'

# Netlink error codes
NETLINK_ERROR_NO_SUCH_PROCESS = 3
NETLINK_ERROR_FILE_EXISTS = 17
NETLINK_ERROR_NO_SUCH_DEVICE = 19
NETLINK_ERROR_OPERATION_NOT_SUPPORTED = 95


class SRv6Manager(srv6_manager_pb2_grpc.SRv6ManagerServicer):
    '''gRPC request handler'''

    def __init__(
        self,
        quagga_password=DEFAULT_QUAGGA_PASSWORD,
        zebra_port=DEFAULT_ZEBRA_PORT,
        ospf6d_port=DEFAULT_OSPF6D_PORT,
        stop_event=None,
        reboot_required=None
    ):
        self.quagga_password = quagga_password
        self.zebra_port = zebra_port
        self.ospf6d_port = ospf6d_port
        self.stop_event = stop_event
        self.reboot_required = reboot_required

    def parse_netlink_error(self, e):
        if e.code == NETLINK_ERROR_FILE_EXISTS:
            logging.warning('Netlink error: File exists')
            return status_codes_pb2.STATUS_FILE_EXISTS
        elif e.code == NETLINK_ERROR_NO_SUCH_PROCESS:
            logging.warning('Netlink error: No such process')
            return status_codes_pb2.STATUS_NO_SUCH_PROCESS
        elif e.code == NETLINK_ERROR_NO_SUCH_DEVICE:
            logging.warning('Netlink error: No such device')
            return status_codes_pb2.STATUS_NO_SUCH_DEVICE
        elif e.code == NETLINK_ERROR_OPERATION_NOT_SUPPORTED:
            logging.warning('Netlink error: Operation not supported')
            return status_codes_pb2.STATUS_OPERATION_NOT_SUPPORTED
        else:
            logging.warning('Generic internal error: %s', e)
            status_codes_pb2.STATUS_INTERNAL_ERROR

    def ShutdownDevice(self, request, context):
        logging.info('\n\nShutdownDevice command received')
        # Set the stop flag to trigger the server shutdown
        self.stop_event.set()
        # Require a reboot
        if self.reboot_required is not None:
            self.reboot_required.set()
        return srv6_manager_pb2.SRv6ManagerReply(
            status=status_codes_pb2.STATUS_SUCCESS
        )

    def HandleSRv6ExplicitPathRequest(self, op, request, context):
        logging.debug('config received:\n%s', request)
        # Perform operation
        try:
            if op == 'add' or 'del':
                # Let's push the routes
                for path in request.paths:
                    # Rebuild segments
                    segments = []
                    for srv6_segment in path.sr_path:
                        segments.append(srv6_segment.segment)
                    table = path.table
                    if path.table == -1:
                        table = None
                    if segments == []:
                        segments = ['::']
                    if path.device != '':
                        oif = idxs[path.device]
                    else:
                        oif = None
                    ip_route.route(
                        op,
                        dst=path.destination,
                        oif=oif,
                        table=table,
                        encap={
                            'type': 'seg6',
                            'mode': path.encapmode,
                            'segs': segments
                        }
                    )
            else:
                # Operation unknown: this is a bug
                logging.error('Unrecognized operation: %s', op)
                exit(-1)
            # and create the response
            logging.debug('Send response: OK')
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_SUCCESS
            )
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleSRv6LocalProcessingFunctionRequest(self, op, request, context):
        logging.debug('config received:\n%s', request)
        # Let's process the request
        try:
            for function in request.functions:
                # Extract params from request
                segment = function.segment
                action = function.action
                nexthop = function.nexthop
                table = function.table
                interface = function.interface
                device = function.device
                localsid_table = function.localsid_table
                # Check optional params
                nexthop = nexthop if nexthop != '' else None
                table = table if table != -1 else None
                interface = interface if interface != '' else None
                # Perform operation
                if op == 'del':
                    # Delete a route
                    ip_route.route(
                        op, family=AF_INET6, dst=segment, table=localsid_table
                    )
                elif op == 'add':
                    # Add a new route
                    if action == 'End':
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End'
                            }
                        )
                    elif action == 'End.X':
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.X',
                                'nh6': nexthop
                            }
                        )
                    elif action == 'End.T':
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.T',
                                'table': table
                            }
                        )
                    elif action == 'End.DX2':
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.DX2',
                                'oif': interface
                            }
                        )
                    elif action == 'End.DX6':
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.DX6',
                                'nh6': nexthop
                            }
                        )
                    elif action == 'End.DX4':
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.DX4',
                                'nh4': nexthop
                            }
                        )
                    elif action == 'End.DT6':
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.DT6',
                                'table': table
                            }
                        )
                    elif action == 'End.DT4':
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.DT4',
                                'vrf_table': table
                            }
                        )
                    elif action == 'End.DT46':
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.DT46',
                                'vrf_table': table
                            }
                        )
                    elif action == 'End.B6':
                        # Rebuild segments
                        segments = []
                        for srv6_segment in function.segs:
                            segments.append(srv6_segment.segment)
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.B6',
                                'srh': {'segs': segments}
                            }
                        )
                    elif action == 'End.B6.Encaps':
                        # Rebuild segments
                        segments = []
                        for srv6_segment in function.segs:
                            segments.append(srv6_segment.segment)
                        ip_route.route(
                            op,
                            family=AF_INET6,
                            dst=segment,
                            oif=idxs[device],
                            table=localsid_table,
                            encap={
                                'type': 'seg6local',
                                'action': 'End.B6.Encaps',
                                'srh': {'segs': segments}
                            }
                        )
                    else:
                        logging.debug('Error: Unrecognized action')
                        return srv6_manager_pb2.SRv6ManagerReply(
                            status=status_codes_pb2.STATUS_INVALID_ACTION
                        )
                else:
                    # Operation unknown: this is a bug
                    logging.error('Unrecognized operation: %s', op)
            # and create the response
            logging.debug('Send response: OK')
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_SUCCESS
            )
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleIPRuleRequest(self, op, request, context):
        logging.debug('config received:\n%s', request)

        # Let's process the request
        try:
            if op == 'add' or op == 'del':
                for rule in request.rules:
                    # Extract params from the request
                    family = rule.family
                    table = rule.table
                    priority = rule.priority
                    action = rule.action
                    scope = rule.scope
                    destination = rule.destination
                    dst_len = rule.dst_len
                    source = rule.source
                    src_len = rule.src_len
                    in_interface = rule.in_interface
                    out_interface = rule.out_interface
                    # Check optional fields
                    table = table if table != -1 else None
                    priority = priority if priority != -1 else (self._get_lowest_priority_rule() -1)
                    action = action if action != '' else None
                    scope = scope if scope != -1 else None
                    destination = destination if destination != '' else None
                    dst_len = dst_len if dst_len != -1 else None
                    source = source if source != '' else None
                    src_len = src_len if src_len != -1 else None
                    in_interface = in_interface if in_interface != '' else None
                    out_interface = (
                        out_interface if out_interface != '' else None
                    )

                    # # FIXME remmove this just for debug -------------------------------------------------------
                    # logging.info('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
                    # logging.info('op: %s', op)
                    # logging.info('family: %s', family)
                    # logging.info('table: %s', table)
                    # logging.info('priority: %s', priority)
                    # logging.info('action: %s', action)
                    # logging.info('scope: %s', scope)
                    # logging.info('destination: %s', destination)
                    # logging.info('dst_len: %s', dst_len)
                    # logging.info('source: %s', source)
                    # logging.info('src_len: %s', src_len)
                    # logging.info('in_interface: %s', in_interface)
                    # logging.info('out_interface: %s', out_interface)
                    # logging.info('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
                    # # --------------------------------------------------------------------------------------------


                    # Create or delete the rule
                    ip_route.rule(
                        op,
                        family=family,
                        table=table,
                        priority=priority,
                        action=action,
                        rtscope=scope,
                        dst=destination,
                        dst_len=dst_len,
                        src=source,
                        src_len=src_len,
                        iifname=in_interface,
                        oifname=out_interface
                    )
            else:
                # Operation unknown: this is a bug
                logging.error('Unrecognized operation: %s', op)
            # and create the response
            logging.debug('Send response: OK')
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_SUCCESS
            )
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleIPRouteRequest(self, op, request, context):
        logging.debug('config received:\n%s', request)
        # Let's process the request
        try:
            if op == 'add' or op == 'del':
                for route in request.routes:
                    # Extract params from the request
                    family = route.family
                    tos = route.tos
                    type = route.type
                    table = route.table
                    scope = route.scope
                    proto = route.proto
                    destination = route.destination
                    dst_len = route.dst_len
                    preferred_source = route.preferred_source
                    src_len = route.src_len
                    in_interface = route.in_interface
                    out_interface = route.out_interface
                    gateway = route.gateway
                    # Check optional params
                    family = family if family != -1 else None
                    tos = tos if tos != '' else None
                    type = ROUTE_TYPES[type] if type != '' else None
                    table = table if table != -1 else None
                    scope = scope if scope != -1 else None
                    proto = proto if proto != -1 else None
                    destination = destination if destination != '' else None
                    dst_len = dst_len if dst_len != -1 else None
                    preferred_source = (
                        preferred_source if preferred_source != '' else None
                    )
                    src_len = src_len if src_len != -1 else None
                    in_interface = (
                        ip_route.link_lookup(ifname=in_interface)[0]
                        if in_interface != ''
                        else None
                    )
                    out_interface = (
                        ip_route.link_lookup(ifname=out_interface)[0]
                        if out_interface != ''
                        else None
                    )
                    gateway = gateway if gateway != '' else None
                    
                    # Let's push the route
                    if destination is None and op == 'del':
                        # Destination not specified, delete all the routes
                        ip_route.flush_routes(
                            table=table,
                            tos=tos,
                            scope=scope,
                            type=type,
                            proto=proto,
                            prefsrc=preferred_source,
                            src_len=src_len,
                            iif=in_interface,
                            oif=out_interface,
                            gateway=gateway,
                            family=family
                        )
                    else:
                        # Create or delete the route
                        ip_route.route(
                            op,
                            table=table,
                            tos=tos,
                            scope=scope,
                            type=type,
                            proto=proto,
                            dst=destination,
                            prefsrc=preferred_source,
                            src_len=src_len,
                            dst_len=dst_len,
                            iif=in_interface,
                            oif=out_interface,
                            gateway=gateway,
                            family=family
                        )
            else:
                # Operation unknown: this is a bug
                logging.error('Unrecognized operation: %s', op)
            # and create the response
            logging.debug('Send response: OK')
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_SUCCESS
            )
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleIPAddrPyroute2Request(self, op, request, context):
        logging.debug('config received:\n%s', request)
        # Let's process the request
        try:
            if op == 'add' or op == 'del':
                # Interface configuration
                for addr in request.addrs:
                    # Extract the interface from the request
                    device = str(addr.device)
                    # Extract address family
                    family = addr.family
                    if family == -1:
                        if op == 'del':
                            family = AF_UNSPEC
                        else:
                            family = AF_INET
                    # Get IP address
                    ip = str(addr.ip_addr)
                    # Get network prefix
                    # prefix = str(addr.net) if addr.net != '' else None
                    # Interface configuration
                    if family in [AF_INET, AF_INET6]:
                        # Add or Remove IPv6 address
                        try:
                            ip_route.addr(
                                op,
                                index=ip_route.link_lookup(ifname=device)[0],
                                address=ip.split('/')[0],
                                mask=int(ip.split('/')[1]),
                                family=family
                            )
                        except NetlinkError as err:
                            if not request.ignore_errors:
                                raise NetlinkError(err.code)
                    elif family == AF_UNSPEC:
                        if op == 'del':
                            # Remove IPv4/IPv6 address
                            ip_route.addr(
                                op,
                                index=ip_route.link_lookup(ifname=device)[0],
                                address=ip.split('/')[0],
                                mask=int(ip.split('/')[1])
                            )
                        else:
                            raise InvalidAddressFamilyError
                    else:
                        raise InvalidAddressFamilyError
                    logging.debug('Send response: OK')
                    return srv6_manager_pb2.SRv6ManagerReply(
                        status=status_codes_pb2.STATUS_SUCCESS
                    )
            else:
                # Operation unknown: this is a bug
                logging.error('Unrecognized operation: %s', op)
            # and create the response
            logging.debug('Send response: OK')
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_SUCCESS
            )
        except InvalidAddressFamilyError:
            logging.debug('Send response: Invalid address family')
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_INVALID_ADDRESS
            )
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleIPAddrRequest(self, op, request, context):
        logging.debug('config received:\n%s', request)
        # Let's process the request
        try:
            if op == 'add' or op == 'del':
                # Log to zebra daemon and add prefix
                # and ip address to the interface
                port = self.zebra_port
                password = self.quagga_password
                try:
                    # Init telnet
                    tn = telnetlib.Telnet('localhost', port)
                    # Password
                    tn.read_until(b'Password: ')
                    tn.write(('%s\r\n' % password).encode('latin-1'))
                    # Terminal length set to 0 to not have interruptions
                    tn.write(b'terminal length 0\r\n')
                    # Enable
                    tn.write(b'enable\r\n')
                    # Password
                    tn.read_until(b'Password: ')
                    tn.write(('%s\r\n' % password).encode('latin-1'))
                    # Configure terminal
                    tn.write(b'configure terminal\r\n')
                    # Interface configuration
                    for addr in request.addrs:
                        # Extract the interface from the request
                        device = str(addr.device)
                        # Extract address family
                        family = addr.family
                        if family == -1:
                            if op == 'del':
                                family = AF_UNSPEC
                            else:
                                family = AF_INET
                        # Get IP address
                        ip = str(addr.ip_addr)
                        # Get network prefix
                        prefix = str(addr.net) if addr.net != '' else None
                        # Interface configuration
                        tn.write(
                            ('interface %s\r\n' % device).encode('latin-1')
                        )
                        if family == AF_INET6:
                            if op == 'del':
                                # Remove IPv6 address
                                tn.write(
                                    (
                                        'no ipv6 address %s\r\n' % ip
                                    ).encode('latin-1')
                                )
                                if prefix is not None:
                                    tn.write(
                                        (
                                            'no ipv6 nd prefix %s\r\n' % prefix
                                        ).encode('latin-1')
                                    )
                            else:
                                # Add IPv6 address
                                tn.write(
                                    (
                                        'ipv6 address %s\r\n' % ip
                                    ).encode('latin-1')
                                )
                                if prefix is not None:
                                    tn.write(
                                        (
                                            'ipv6 nd prefix %s\r\n' % prefix
                                        ).encode('latin-1')
                                    )
                        elif family == AF_INET:
                            if op == 'del':
                                # Remove IPv4 address
                                tn.write(
                                    (
                                        'no ip address %s\r\n' % ip
                                    ).encode('latin-1')
                                )
                            else:
                                # Add IPv4 address
                                tn.write(
                                    (
                                        'ip address %s\r\n' % ip
                                    ).encode('latin-1')
                                )
                        elif family == AF_UNSPEC:
                            if op == 'del':
                                # Remove IPv6 address
                                tn.write(
                                    (
                                        'no ipv6 address %s\r\n' % ip
                                    ).encode('latin-1')
                                )
                                if prefix is not None:
                                    tn.write(
                                        (
                                            'no ipv6 nd prefix %s\r\n' % prefix
                                        ).encode('latin-1')
                                    )
                                # Remove IPv4 address
                                tn.write(
                                    (
                                        'no ip address %s\r\n' % ip
                                    ).encode('latin-1')
                                )
                            else:
                                raise InvalidAddressFamilyError
                        else:
                            raise InvalidAddressFamilyError
                        # Close interface configuration
                        tn.write(b'q\r\n')
                    # Close configuration mode
                    tn.write(b'q\r\n')
                    # Close privileged mode
                    tn.write(b'q\r\n')
                    # Read all
                    tn.read_all()
                    # Close telnet
                    tn.close()
                    logging.debug('Send response: OK')
                    return srv6_manager_pb2.SRv6ManagerReply(
                        status=status_codes_pb2.STATUS_SUCCESS
                    )
                except socket.error:
                    logging.debug('Send response: Unreachable zebra daemon')
                    return srv6_manager_pb2.SRv6ManagerReply(
                        status=status_codes_pb2.STATUS_UNREACHABLE_ZEBRA
                    )
                except InvalidAddressFamilyError:
                    logging.debug('Send response: Invalid address family')
                    return srv6_manager_pb2.SRv6ManagerReply(
                        status=status_codes_pb2.STATUS_INVALID_ADDRESS
                    )
            else:
                # Operation unknown: this is a bug
                logging.error('Unrecognized operation: %s', op)
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleVRFDeviceRequest(self, op, request, context):

        # FIXME remove this just logging ----------------------------------------------------------------------
        logging.info("\n&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& HandleVRFDeviceRequest - op: %s\n", op)
        logging.info("request : ")
        logging.info(request) 
        logging.info("\n&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n\n\n")
        # ------------------------------------------------------------------------------------------------------


        logging.debug('config received:\n%s', request)
        # Let's process the request
        try:
            if op == 'add' or op == 'del':
                for device in request.devices:
                    ip_route.link(
                        op,
                        ifname=device.name,
                        kind='vrf',
                        vrf_table=device.table
                    )
                    if op == 'add':
                        # Enable the new VRF
                        vrfindex = ip_route.link_lookup(ifname=device.name)[0]
                        ip_route.link('set', index=vrfindex, state='up')
                        '''
                        # Set the default route for the table
                        # (and hence default route for the VRF)
                        ip_route.route('add', table=device.table,
                                        type='unreachable', dst='default',
                                        priority=4278198272, family=AF_INET)
                        ip_route.route('add', table=device.table,
                                        type='unreachable', dst='default',
                                        priority=4278198272, family=AF_INET6)
                        '''
                    '''
                    elif op == 'del':
                        ip_route.route('del', table=device.table,
                                        type='unreachable',
                                        dst='default', family=AF_INET)
                        ip_route.route('del', table=device.table,
                                        type='unreachable',
                                        dst='default', family=AF_INET6)
                    '''
                # and create the response
                if op == 'add':
                    return self.HandleVRFDeviceRequest(
                        'change', request, context
                    )
                else:
                    logging.debug('Send response: OK')
                    return srv6_manager_pb2.SRv6ManagerReply(
                        status=status_codes_pb2.STATUS_SUCCESS
                    )
            elif op == 'change':
                # FIXME remove this just logging ----------------------------------------------------------------------
                logging.info("\n\######################33333####### op == change : n")
                logging.info("request : ")
                logging.info(request)

                # --------------------------------------------------------------------------------------------------
                for device in request.devices:
                    if device.op == 'add_interfaces':
                        # Get the VRF index
                        vrfindex = ip_route.link_lookup(ifname=device.name)[0]
                        # Add the remaining links to the VRF
                        for interface in device.interfaces:
                            ifindex = ip_route.link_lookup(ifname=interface)[0]
                            ip_route.link(
                                'set', index=ifindex, master=vrfindex
                            )
                        return srv6_manager_pb2.SRv6ManagerReply(
                            status=status_codes_pb2.STATUS_SUCCESS
                        )
                    elif device.op == 'del_interfaces':
                        # Get the VRF index
                        vrfindex = ip_route.link_lookup(ifname=device.name)[0]
                        # For each link in the VRF
                        interfaces_in_vrf = set()
                        for link in ip_route.get_links():
                            if link.get_attr('IFLA_MASTER') == vrfindex:
                                interfaces_in_vrf.add(
                                    link.get_attr('IFLA_IFNAME')
                                )
                        # Add the remaining links to the VRF
                        for interface in device.interfaces:
                            if interface not in interfaces_in_vrf:
                                logging.warning(
                                    'Interface does not belong to the VRF'
                                )
                                return srv6_manager_pb2.SRv6ManagerReply(
                                    status=(
                                        status_codes_pb2.STATUS_NO_SUCH_DEVICE
                                    )
                                )
                            ifindex = ip_route.link_lookup(ifname=interface)[0]
                            ip_route.link('set', index=ifindex, master=0)
                        return srv6_manager_pb2.SRv6ManagerReply(
                            status=status_codes_pb2.STATUS_SUCCESS
                        )
                    else:
                        # Get the interfaces to be added to the VRF
                        interfaces = []
                        for interface in device.interfaces:
                            interfaces.append(interface)
                        # Get the VRF index
                        vrfindex = ip_route.link_lookup(ifname=device.name)[0]
                        # For each link in the VRF
                        for link in ip_route.get_links():
                            if link.get_attr('IFLA_MASTER') == vrfindex:
                                if link.get_attr('IFLA_IFNAME') in interfaces:
                                    # The link belongs to the VRF
                                    interfaces.remove(
                                        link.get_attr('IFLA_IFNAME')
                                    )
                                else:
                                    # The link has to be removed from the VRF
                                    ifindex = link.get('index')
                                    ip_route.link(
                                        'set', index=ifindex, master=0
                                    )
                        # Add the remaining links to the VRF
                        for interface in interfaces:
                            ifindex = ip_route.link_lookup(ifname=interface)[0]
                            ip_route.link(
                                'set', index=ifindex, master=vrfindex
                            )
                        return srv6_manager_pb2.SRv6ManagerReply(
                            status=status_codes_pb2.STATUS_SUCCESS
                        )
            else:
                # Operation unknown: this is a bug
                logging.error('Unrecognized operation: %s', op)
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleInterfaceRequest(self, op, request, context):
        logging.debug('config received:\n%s', request)
        # Let's process the request
        try:
            if op == 'change':
                # Handle change operation
                # Log to ospf6d daemon and remove the interface
                # from the ospf advertisements. The subnet of a VPN site
                # is a private subnet, so we don't advertise it
                port = self.ospf6d_port
                password = self.quagga_password
                try:
                    # Init telnet
                    tn = telnetlib.Telnet('localhost', port)
                    # Password
                    tn.read_until(b'Password: ')
                    tn.write(('%s\r\n' % password).encode('latin-1'))
                    # Terminal length set to 0 to not have interruptions
                    tn.write(b'terminal length 0\r\n')
                    # Enable
                    tn.write(b'enable\r\n')
                    # Configure terminal
                    tn.write(b'configure terminal\r\n')
                    # OSPF6 configuration
                    tn.write(b'router ospf6\r\n')
                    # Interface advertisements
                    for device in request.interfaces:
                        if device.ospf_adv:
                            # Add the interface to the link state messages
                            tn.write(
                                (
                                    'interface %s area 0.0.0.0\r\n'
                                    % str(device.name)
                                ).encode('latin-1')
                            )
                        else:
                            # Remove the interface from the link state messages
                            tn.write(
                                (
                                    'no interface %s area 0.0.0.0\r\n'
                                    % str(device.name)
                                ).encode('latin-1')
                            )
                    # Close interface configuration
                    tn.write(b'q\r\n')
                    # Close configuration mode
                    tn.write(b'q\r\n')
                    # Close privileged mode
                    tn.write(b'q\r\n')
                    # Read all
                    tn.read_all()
                    # Close telnet
                    tn.close()
                    logging.debug('Send response: OK')
                    return srv6_manager_pb2.SRv6ManagerReply(
                        status=status_codes_pb2.STATUS_SUCCESS
                    )
                except socket.error:
                    logging.debug('Send response: Unreachable ospf6d daemon')
                    return srv6_manager_pb2.SRv6ManagerReply(
                        status=status_codes_pb2.STATUS_UNREACHABLE_OSPF6D
                    )
            elif op == 'get':
                # Handle get operation
                # Get the interfaces
                interfaces = []
                for interface in request.interfaces:
                    ifindex = ip_route.link_lookup(ifname=interface.name)[0]
                    interfaces.append(ifindex)
                links = dict()
                for link in ip_route.get_links(*interfaces):
                    if (
                        link.get_attr('IFLA_LINKINFO')
                        and (
                            link.get_attr('IFLA_LINKINFO')
                            .get_attr('IFLA_INFO_KIND') != 'vrf'
                        )
                    ):
                        # Skip the VRFs
                        # Get the index of the interface
                        ifindex = link.get('index')
                        # Get the name of the interface
                        ifname = link.get_attr('IFLA_IFNAME')
                        # Get the MAC address of the interface
                        macaddr = link.get_attr('IFLA_ADDRESS')
                        # Get the state of the interface
                        state = link.get_attr('IFLA_OPERSTATE')
                        # Save the interface
                        links[ifindex] = (ifname, macaddr, state)
                # Get the addresses assigned to the interfaces
                addrs = dict()
                for addr in ip_route.get_addr():
                    # Get the index of the interface
                    ifindex = addr.get('index')
                    # Get the IP address of the interface
                    ipaddr = addr.get_attr('IFA_ADDRESS')
                    # Get prefix length
                    prefixlen = addr.get('prefixlen')
                    # IP/mask
                    ipaddr = '%s/%s' % (ipaddr, prefixlen)
                    # Save the address
                    if addrs.get(ifindex) is None:
                        addrs[ifindex] = list()
                    addrs[ifindex].append(ipaddr)
                # Mapping interface name to MAC address and IP address
                interfaces = dict()
                for ifindex in links:
                    ifname = links[ifindex][0]
                    macaddr = links[ifindex][1]
                    state = links[ifindex][2]
                    ipaddr = addrs[ifindex]
                    interfaces[ifname] = (macaddr, ipaddr, state, ifindex)
                # Create the response
                response = srv6_manager_pb2.SRv6ManagerReply(
                    status=status_codes_pb2.STATUS_SUCCESS
                )
                for ifname in interfaces:
                    interface = response.interfaces.add()
                    interface.index = int(interfaces[ifname][3])
                    interface.name = ifname
                    interface.macaddr = interfaces[ifname][0]
                    for addr in interfaces[ifname][1]:
                        interface.ipaddrs.append(addr)
                    interface.state = interfaces[ifname][2]
                logging.debug('Send response:\n%s', response)
                return response
            else:
                # Operation unknown: this is a bug
                logging.error('Unrecognized operation: %s', op)
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleIPNeighRequest(self, op, request, context):
        logging.debug('config received:\n%s', request)
        # Let's process the request
        try:
            if op == 'add' or op == 'del':
                for neigh in request.neighs:
                    # Extract params from the request
                    family = neigh.family
                    addr = neigh.addr
                    lladdr = None
                    if neigh.lladdr:
                        lladdr = neigh.lladdr
                    device = neigh.device
                    flags = 0
                    if neigh.proxy:
                        flags |= ndmsg.NTF_PROXY
                    # Create or delete the neigh
                    device = ip_route.link_lookup(ifname=device)[0]
                    ip_route.neigh(
                        op,
                        family=family,
                        dst=addr,
                        lladdr=lladdr,
                        ifindex=device,
                        flags=flags,
                        state=ndmsg.states['permanent']
                    )
            else:
                # Operation unknown: this is a bug
                logging.error('Unrecognized operation: %s', op)
            # and create the response
            logging.debug('Send response: OK')
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_SUCCESS
            )
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleGREInterfaceRequest(self, op, request, context):
        logging.debug('config received:\n%s', request)
        # Let's process the request
        try:
            if op == 'add' or op == 'del':
                for gre_interface in request.gre_interfaces:
                    # Extract params from the request
                    name = gre_interface.name
                    local = gre_interface.local
                    remote = gre_interface.remote
                    key = gre_interface.key
                    type = gre_interface.type
                    # Check optional params
                    local = local if local != '' else None
                    remote = remote if remote != '' else None
                    key = key if key != -1 else None
                    if type == gre_interface_pb2.IP6GRE:
                        # Create or delete the gre interface
                        ip_route.link(
                            op,
                            ifname=name,
                            kind='ip6gre',
                            ip6gre_local=local,
                            ip6gre_remote=remote,
                            ip6gre_key=key
                        )
                    if type == gre_interface_pb2.GRE:
                        # Create or delete the gre interface
                        ip_route.link(
                            op,
                            ifname=name,
                            kind='gre',
                            gre_local=local,
                            gre_remote=remote,
                            gre_key=key
                        )
                    else:
                        logging.warning('Unrecognized GRE type: %s', type)
                        return srv6_manager_pb2.SRv6ManagerReply(
                            status=status_codes_pb2.STATUS_INTERNAL_ERROR
                        )
                    # Enable the new GRE interface
                    greindex = ip_route.link_lookup(ifname=name)[0]
                    ip_route.link('set', index=greindex, state='up')
            else:
                # Operation unknown: this is a bug
                logging.error('Unrecognized operation: %s', op)
            # and create the response
            logging.debug('Send response: OK')
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_SUCCESS
            )
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def HandleIPVxLANRequest(self, op, request, context):
        logging.debug("config received:\n%s", request)
        # Let's process the request
        for vxlan in request.vxlan:
            # Extract params from the request
            ifname = vxlan.ifname
            vxlan_link = vxlan.vxlan_link
            vxlan_id = vxlan.vxlan_id
            vxlan_port = vxlan.vxlan_port
            vxlan_group = None
            if vxlan_group != '':
                vxlan_group = vxlan.vxlan_group
            # Let's push the vxlan command
            if op == 'add':
                # Create VTEP

                # FIXME remove this just logging  ---------------------------------------------------------
                logging.info("\n\n\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n")
                logging.info(ifname)
                logging.info(ip_route.link_lookup(ifname=vxlan_link)[0])
                logging.info(vxlan_id)
                logging.info(vxlan_port)
                logging.info(vxlan_group)
                logging.info("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n\n\n")
                logging.info("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n\n")
                logging.info(request)
                logging.info("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n\n")


                # FIXME -----------------------------------------------------------------------------------


                if getAddressFamily(vxlan_group) == AF_INET:
                    ip_route.link(
                        op,
                        ifname=ifname,
                        kind="vxlan",
                        vxlan_link=ip_route.link_lookup(ifname=vxlan_link)[0],
                        vxlan_id=vxlan_id,
                        vxlan_port=vxlan_port,
                        vxlan_port_range={
                            'low': vxlan_port,
                            'high': vxlan_port+1
                        },
                        vxlan_group=vxlan_group
                    )
                elif getAddressFamily(vxlan_group) == AF_INET6:
                    ip_route.link(
                        op,
                        ifname=ifname,
                        kind="vxlan",
                        vxlan_link=ip_route.link_lookup(ifname=vxlan_link)[0],
                        vxlan_id=vxlan_id,
                        vxlan_port=vxlan_port,
                        vxlan_port_range={
                            'low': vxlan_port,
                            'high': vxlan_port+1
                        },
                        vxlan_group6=vxlan_group
                    )
                else:
                    return srv6_manager_pb2.SRv6ManagerReply(
                        status=status_codes_pb2.STATUS_INVALID_ADDRESS
                    )
                # Set UP VTEP
                ip_route.link(
                    'set',
                    index=ip_route.link_lookup(ifname=ifname)[0],
                    state='up'
                )
            # Delete VTEP interface
            elif op == 'del':
                ip_route.link(
                    'del',
                    index=ip_route.link_lookup(ifname=ifname)[0]
                )
            else:
                # Operation unknown: this is a bug
                print('Unrecognized operation')
                exit(-1)
        # and create the response
        return srv6_manager_pb2.SRv6ManagerReply(
            status=status_codes_pb2.STATUS_SUCCESS
        )

    def HandleIPfdbentriesRequest(self, op, request, context):
        logging.debug("config received:\n%s", request)
        # Let's process the request
        for fdbentries in request.fdbentries:
            # Extract params from the request
            ifindex = fdbentries.ifindex
            dst = fdbentries.dst
            # Let's push the fdb append command
            if op == 'add':
                ip_route.fdb(
                    'append',
                    ifindex=ip_route.link_lookup(ifname=ifindex)[0],
                    lladdr='00:00:00:00:00:00',
                    dst=dst
                )

            elif op == 'del':
                ip_route.fdb(
                    'del',
                    ifindex=ip_route.link_lookup(ifname=ifindex)[0],
                    lladdr='00:00:00:00:00:00',
                    dst=dst
                )
            else:
                # Operation unknown: this is a bug
                print('Unrecognized operation')
                exit(-1)
        # and create the response
        return srv6_manager_pb2.SRv6ManagerReply(
            status=status_codes_pb2.STATUS_SUCCESS
        )

    def HandleIPTunnelRequest(self, op, request, context):
        logging.debug('config received:\n%s', request)
        # Let's process the request
        try:
            for ip_tunnel in request.ip_tunnels:
                if op == 'add':
                    # Extract the tunnel type
                    if ip_tunnel.tunnel_type == IPTunnelType.IP4IP4:
                        ip_route.link(
                            op,
                            ifname=ip_tunnel.ifname,
                            kind='sit',
                            ip6tnl_local=ip_tunnel.local_addr,
                            ip6tnl_remote=ip_tunnel.remote_addr,
                            ip6tnl_mode='ipip'
                        )
                    elif ip_tunnel.tunnel_type == IPTunnelType.IP4IP6:
                        ip_route.link(
                            op,
                            ifname=ip_tunnel.ifname,
                            kind='ip6tnl',
                            ip6tnl_local=ip_tunnel.local_addr,
                            ip6tnl_remote=ip_tunnel.remote_addr,
                            ip6tnl_mode='ipip6'
                        )
                    elif ip_tunnel.tunnel_type == IPTunnelType.IP6IP4:
                        ip_route.link(
                            op,
                            ifname=ip_tunnel.ifname,
                            kind='sit',
                            ip6tnl_local=ip_tunnel.local_addr,
                            ip6tnl_remote=ip_tunnel.remote_addr,
                            ip6tnl_mode='ip6ip'
                        )
                    elif ip_tunnel.tunnel_type == IPTunnelType.IP6IP6:
                        ip_route.link(
                            op,
                            ifname=ip_tunnel.ifname,
                            kind='ip6tnl',
                            ip6tnl_local=ip_tunnel.local_addr,
                            ip6tnl_remote=ip_tunnel.remote_addr,
                            ip6tnl_mode='ip6ip6'
                        )
                    else:
                        logging.error(
                            'Invalid tunnel type: %s', ip_tunnel.tunnel_type
                        )
                        return srv6_manager_pb2.SRv6ManagerReply(
                            status=status_codes_pb2.STATUS_INTERNAL_ERROR
                        )
                    # Enable the new interface
                    ifindex = ip_route.link_lookup(ifname=ip_tunnel.ifname)[0]
                    ip_route.link('set', index=ifindex, state='up')
                elif op == 'del':
                    ip_route.link(op, ifname=ip_tunnel.ifname)
                else:
                    # Operation unknown: this is a bug
                    logging.error('Unrecognized operation: %s', op)
            logging.debug('Send response: OK')
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_SUCCESS
            )
        except NetlinkError as e:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=self.parse_netlink_error(e)
            )

    def Execute(self, op, request, context):

        logging.info('============= operation: %s', op)

        entity_type = request.entity_type
        # Handle operation
        # The operation to be executed depends on
        # the entity carried by the request message
        if entity_type == srv6_manager_pb2.SRv6ExplicitPath:
            request = request.srv6_ep_request
            return self.HandleSRv6ExplicitPathRequest(op, request, context)
        elif entity_type == srv6_manager_pb2.SRv6LocalProcessingFunction:
            request = request.srv6_lpf_request
            return self.HandleSRv6LocalProcessingFunctionRequest(
                op, request, context
            )
        elif entity_type == srv6_manager_pb2.IPAddr:
            request = request.ipaddr_request
            if USE_ZEBRA:
                return self.HandleIPAddrRequest(op, request, context)
            else:
                return self.HandleIPAddrPyroute2Request(op, request, context)
        elif entity_type == srv6_manager_pb2.IPRule:
            request = request.iprule_request
            return self.HandleIPRuleRequest(op, request, context)
        elif entity_type == srv6_manager_pb2.IPRoute:
            request = request.iproute_request
            return self.HandleIPRouteRequest(op, request, context)
        elif entity_type == srv6_manager_pb2.VRFDevice:
            request = request.vrf_device_request
            return self.HandleVRFDeviceRequest(op, request, context)
        elif entity_type == srv6_manager_pb2.Interface:
            request = request.interface_request
            return self.HandleInterfaceRequest(op, request, context)
        elif entity_type == srv6_manager_pb2.IPNeigh:
            request = request.ipneigh_request
            return self.HandleIPNeighRequest(op, request, context)
        elif entity_type == srv6_manager_pb2.GREInterface:
            request = request.gre_interface_request
            return self.HandleGREInterfaceRequest(op, request, context)
        elif entity_type == srv6_manager_pb2.IPVxlan:
            request = request.ipvxlan_request
            return self.HandleIPVxLANRequest(op, request, context)
        elif entity_type == srv6_manager_pb2.IPfdbentries:
            request = request.fdbentries_request
            return self.HandleIPfdbentriesRequest(op, request, context)
        elif entity_type == srv6_manager_pb2.IPTunnel:
            request = request.iptunnel_request
            return self.HandleIPTunnelRequest(op, request, context)
        else:
            return srv6_manager_pb2.SRv6ManagerReply(
                status=status_codes_pb2.STATUS_INVALID_GRPC_REQUEST
            )

    def Create(self, request, context):
        # Handle Create operation
        return self.Execute('add', request, context)

    def Get(self, request, context):
        # Handle Create operation
        return self.Execute('get', request, context)

    def Update(self, request, context):
        # Handle Remove operation
        return self.Execute('change', request, context)

    def Remove(self, request, context):
        # Handle Remove operation
        return self.Execute('del', request, context)
    
    def _get_lowest_priority_rule(self):
        rules = list(ip_route.get_rules())
        prio = None
        lowest_priority = None
        for rule in rules:
            for attr in rule['attrs']:
                if attr[0] == 'FRA_PRIORITY':
                    prio = attr[1]
                    if lowest_priority is None or prio < lowest_priority:
                        lowest_priority = prio
        return lowest_priority


class NetworkEventsListener(
    network_events_listener_pb2_grpc.NetworkEventsListenerServicer
):

    def Listen(self, request, context):
        logging.debug('config received:\n%s', request)
        # Send an ACK message to the client
        message = network_events_listener_pb2.NetworkEvent()
        message.type = EVENT_TYPES['CONNECTION_ESTABLISHED']
        yield message
        # Inizialize IPDB
        ipdb = IPDB()
        # Process event queue
        with ipdb.eventqueue() as evq:
            # Process messages
            for msg in evq:
                if not context.is_active():
                    logging.info('The client has been disconnected')
                    break
                ifindex = None
                ifname = None
                macaddr = None
                ipaddr = None
                prefixlen = None
                state = None
                if (
                    msg.get_attr('IFLA_LINKINFO') is not None
                    and (
                        msg.get_attr('IFLA_LINKINFO')
                        .get_attr('IFLA_INFO_KIND') == 'vrf'
                    )
                ):
                    # Skip VRF devices
                    continue
                # Convert the message to a dictionary representation
                nlmsg = eval(str(msg))
                if nlmsg['event'] == 'RTM_NEWLINK':
                    # New link message
                    # Extract attributes from the Netlink message
                    attrs = dict(nlmsg['attrs'])
                    # Extract the state of the interface
                    state = attrs.get('IFLA_OPERSTATE')
                    if state == 'UP':
                        type = 'INTF_UP'
                    elif state == 'DOWN':
                        type = 'INTF_DOWN'
                    else:
                        # Skip other events
                        continue
                    # Extract the interface index
                    ifindex = nlmsg['index']
                    # Extract the interface name
                    ifname = attrs.get('IFLA_IFNAME')
                    # Extract the MAC address of the interface
                    macaddr = attrs.get('IFLA_ADDRESS')
                elif nlmsg['event'] == 'RTM_DELLINK':
                    # Deleted link message
                    # Extract attributes from the Netlink message
                    attrs = dict(nlmsg['attrs'])
                    # Extract the state of the interface
                    type = 'INTF_DEL'
                    # Extract the index of the interface
                    ifindex = nlmsg['index']
                    # Extract the name of the interface
                    ifname = attrs.get('IFLA_IFNAME')
                    # Extract the MAC address of the interface
                    macaddr = attrs.get('IFLA_ADDRESS')
                elif nlmsg['event'] == 'RTM_NEWADDR':
                    # Deleted link message
                    # Extract attributes from the Netlink message
                    attrs = dict(nlmsg['attrs'])
                    # Extract the state of the interface
                    type = 'NEW_ADDR'
                    # Extract the index of the interface
                    ifindex = nlmsg['index']
                    # Extract the name of the interface
                    ifname = attrs.get('IFLA_IFNAME')
                    # Extract the IP address of the interface
                    ipaddr = attrs.get('IFA_ADDRESS')
                    # Extract the prefix length
                    prefixlen = nlmsg['prefixlen']
                elif nlmsg['event'] == 'RTM_DELADDR':
                    # Deleted link message
                    # Extract attributes from the Netlink message
                    attrs = dict(nlmsg['attrs'])
                    # Extract the state of the interface
                    type = 'DEL_ADDR'
                    # Extract the index of the interface
                    ifindex = nlmsg['index']
                    # Extract the IP address of the interface
                    ipaddr = attrs.get('IFA_ADDRESS')
                    # Extract the prefix length
                    prefixlen = nlmsg['prefixlen']
                else:
                    # Skip other events
                    continue
                # Create the response
                response = network_events_listener_pb2.NetworkEvent()
                response.interface.index = int(ifindex)
                if ifname is not None:
                    response.interface.name = ifname
                if macaddr is not None:
                    response.interface.macaddr = macaddr
                if ipaddr is not None:
                    response.interface.ipaddr = '%s/%s' % (ipaddr, prefixlen)
                response.type = EVENT_TYPES[type]
                # and send the response to the client
                logging.debug('Send response:\n%s', response)
                yield response
        logging.info('Exiting from Listen()')


# Start gRPC server
def start_server(
    grpc_ip=DEFAULT_GRPC_IP,
    grpc_port=DEFAULT_GRPC_PORT,
    quagga_password=DEFAULT_QUAGGA_PASSWORD,
    zebra_port=DEFAULT_ZEBRA_PORT,
    ospf6d_port=DEFAULT_OSPF6D_PORT,
    secure=DEFAULT_SECURE,
    certificate=DEFAULT_CERTIFICATE,
    key=DEFAULT_KEY,
    stop_event=None,
    reboot_required=None
):
    # Configure gRPC server listener and ip route
    global grpc_server, ip_route, ipdb
    # Setup gRPC server
    if grpc_server is not None:
        logging.error('gRPC Server is already up and running')
    else:
        # Create the server and add the handlers
        grpc_server = grpc.server(futures.ThreadPoolExecutor())
        # Add the STAMP handlers
        if ENABLE_STAMP_SUPPORT:
            stamp_sender_module.run_grpc_server(
                server=grpc_server, stop_event=stop_event
            )
            stamp_reflector_module.run_grpc_server(
                server=grpc_server, stop_event=stop_event
            )
        srv6_manager_pb2_grpc.add_SRv6ManagerServicer_to_server(
            SRv6Manager(
                quagga_password,
                zebra_port,
                ospf6d_port,
                stop_event,
                reboot_required
            ),
            grpc_server
        )
        (
            network_events_listener_pb2_grpc
            .add_NetworkEventsListenerServicer_to_server(
                NetworkEventsListener(), grpc_server
            )
        )
        # If secure we need to create a secure endpoint
        if secure:
            # Read key and certificate
            with open(key, 'rb') as f:
                key = f.read()
            with open(certificate, 'rb') as f:
                certificate = f.read()
            # Create server ssl credentials
            grpc_server_credentials = (
                grpc.ssl_server_credentials(((key, certificate),))
            )
            # Create a secure endpoint
            grpc_server.add_secure_port(
                '[%s]:%s' % (grpc_ip, grpc_port), grpc_server_credentials
            )
        else:
            # Create an insecure endpoint
            grpc_server.add_insecure_port('[%s]:%s' % (grpc_ip, grpc_port))
    # Setup ip route
    if ip_route is not None:
        logging.error('IP Route is already setup')
    else:
        ip_route = IPRoute()
    # Setup ipdb
    if ipdb is not None:
        logging.error('IPDB is already setup')
    else:
        ipdb = IPDB()
    # Resolve the interfaces
    for link in ip_route.get_links():
        if link.get_attr('IFLA_IFNAME') != 'lo':
            interfaces.append(link.get_attr('IFLA_IFNAME'))
    for interface in interfaces:
        idxs[interface] = ip_route.link_lookup(ifname=interface)[0]
    # Start the loop for gRPC
    logging.info('*** Listening gRPC')
    grpc_server.start()
    stop_event.wait()
    logging.info('*** Terminating gRPC server')
    grpc_server.stop(10).wait()
    logging.info('*** Server terminated')
    # while True:
    #    time.sleep(5)


# Parse options
def parse_arguments():
    # Get parser
    parser = ArgumentParser(
        description='gRPC Southbound APIs for SRv6 Controller'
    )
    parser.add_argument(
        '-d', '--debug', action='store_true', help='Activate debug logs'
    )
    parser.add_argument(
        '-s', '--secure', action='store_true', help='Activate secure mode'
    )
    parser.add_argument(
        '-o',
        '--ospf6d-port',
        dest='ospf6d_port',
        action='store',
        default=DEFAULT_OSPF6D_PORT,
        help='The port that the ospf6d VTY is listening on'
    )
    parser.add_argument(
        '-z',
        '--zebra-port',
        dest='zebra_port',
        action='store',
        default=DEFAULT_ZEBRA_PORT,
        help='The port that the zebra VTY is listening on'
    )
    parser.add_argument(
        '-p',
        '--quagga-pwd',
        dest='quagga_pwd',
        action='store',
        default=DEFAULT_QUAGGA_PASSWORD,
        help='Password of zebra/ospf6d quagga daemons'
    )
    parser.add_argument(
        '-g',
        '--grpc-ip',
        dest='grpc_ip',
        action='store',
        default=DEFAULT_GRPC_IP,
        help='IP of the gRPC server'
    )
    parser.add_argument(
        '-r',
        '--grpc-port',
        dest='grpc_port',
        action='store',
        default=DEFAULT_GRPC_PORT,
        help='Port of the gRPC server'
    )
    parser.add_argument(
        '-c',
        '--server-cert',
        dest='server_cert',
        action='store',
        default=DEFAULT_CERTIFICATE,
        help='Server certificate file'
    )
    parser.add_argument(
        '-k',
        '--server-key',
        dest='server_key',
        action='store',
        default=DEFAULT_KEY,
        help='Server key file'
    )
    # Parse input parameters
    args = parser.parse_args()
    # Return the arguments
    return args


if __name__ == '__main__':
    args = parse_arguments()
    # Setup properly the secure mode
    if args.secure:
        secure = True
    else:
        secure = False
    # gRPC IP
    grpc_ip = args.grpc_ip
    # gRPC port
    grpc_port = args.grpc_port
    # Password used to connect to quagga daemons
    quagga_password = args.quagga_pwd
    # Zebra port
    zebra_port = args.zebra_port
    # ospf6d port
    ospf6d_port = args.ospf6d_port
    # Server certificate
    certificate = args.server_cert
    # Server key
    key = args.server_key
    # Setup properly the logger
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger().setLevel(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger().setLevel(level=logging.INFO)
    # Debug settings
    server_debug = logger.getEffectiveLevel() == logging.DEBUG
    logging.info('&&&SERVER_DEBUG:' + str(server_debug))
    # Start the server
    start_server(
        grpc_ip,
        grpc_port,
        quagga_password,
        zebra_port,
        ospf6d_port,
        secure,
        certificate,
        key
    )
