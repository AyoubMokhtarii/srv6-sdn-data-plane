#!/usr/bin/python

# Copyright (C) 2018 Carmine Scarpitta, Pier Luigi Ventre, Stefano Salsano -
# (CNIT and University of Rome "Tor Vergata")
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Utils for SRv6 gRPC Southbound
#
# @author Carmine Scarpitta <carmine.scarpitta.94@gmail.com>
# @author Pier Luigi Ventre <pier.luigi.ventre@uniroma2.it>
# @author Stefano Salsano <stefano.salsano@uniroma2.it>
#

from __future__ import absolute_import, division, print_function

from ipaddress import IPv4Interface, IPv6Interface
from ipaddress import AddressValueError
from socket import AF_INET, AF_INET6

MIN_TABLE_ID = 2
MAX_TABLE_ID = 255


# Utiliy function to check if the provided table ID is valid
def validateTableId(tableid):
    return tableid >= MIN_TABLE_ID and tableid <= MAX_TABLE_ID


# Utiliy function to check if the IP
# is a valid IPv4 address
def isValidIPv6Address(ip):
    if ip is None:
        return False
    try:
        IPv6Interface(ip)
        return True
    except AddressValueError:
        return False


# Utiliy function to check if the IP
# is a valid IPv4 address
def isValidIPv4Address(ip):
    if ip is None:
        return False
    try:
        IPv4Interface(ip)
        return True
    except AddressValueError:
        return False


# Utiliy function to get the IP address family
def getAddressFamily(ip):
    if isValidIPv6Address(ip):
        # IPv6 address
        return AF_INET6
    elif isValidIPv4Address(ip):
        # IPv4 address
        return AF_INET
    else:
        # Invalid address
        return None


class SouthboundGRPCError(Exception):
    pass


class InvalidAddressFamilyError(SouthboundGRPCError):
    pass


class InvalidIPTablesRequestError(SouthboundGRPCError):
    pass