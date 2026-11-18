# ---------------------------------------------------------------------------
# gpudb_address_resolution.py - Turns a cluster's system properties into the
# addresses a client will use.
#
# Copyright (c) 2026 Kinetica DB Inc.
# ---------------------------------------------------------------------------
"""Address resolution for the GPUdb client: from a cluster's system properties
to the addresses its ranks are reached at.

Resolution has three phases:

1. Parse.  Each rank may be reached at several addresses: those advertised in
   ``conf.worker_http_server_urls``, the private ones in
   ``conf.worker_http_server_urls_private``, and ones built from the host's
   interface addresses (``conf.worker_http_server_ips``) and the rank's port
   (``conf.worker_http_server_ports``).  They are gathered per rank, configured
   addresses ahead of discovered ones (:func:`extract_rank_candidate_urls`).

2. Select.  The hostname regex, if any, keeps the matching candidates; an
   address that names the machine this client runs on is set aside where the
   server was reached elsewhere, since it cannot be that server.

3. Probe.  Each rank's candidates are tried in order and the first that answers
   is used (:func:`probe_rank_candidate_urls`); once an address answers for one
   rank, the other ranks on its host try it first.

Everything up to the probe is a pure function of its arguments.  The probe is
given the checks it makes -- whether a rank answers, and what properties an
address reports -- as callables, so this module never holds a connection.
"""

import ipaddress
import logging
import re
import socket
from urllib.parse import urlsplit

try:
    from gpudb.gpudb import GPUdb, GPUdbException, GPUdbHostnameRegexFailureException
except ImportError:
    from gpudb import GPUdb, GPUdbException, GPUdbHostnameRegexFailureException


_log = logging.getLogger( "gpudb.AddressResolution" )


# ---------------------------------------------------------------------------
# The property names read here
# ---------------------------------------------------------------------------

RING_NAME           = "conf.ring_name"
CLUSTER_NAME        = "conf.cluster_name"
ENABLE_MULTI_HEAD   = "conf.enable_worker_http_servers"
ENABLE_HTTPD        = "conf.enable_httpd_proxy"
NUM_HOSTS           = "conf.number_of_hosts"

# The address an operator has published for a host's host manager: all or
# none across hosts, and unique per host, so where it is set at all it is a
# complete table; and the port the host managers listen on
HM_PUBLIC_URL       = "conf.host{}_host_manager_public_url"
HM_PORT             = "conf.hm_http_port"

# Each rank's addresses: advertised (operator-configured, or the host's
# interfaces), private (from the host's configured internal address), and the
# host's interface addresses and the rank's port, to build addresses from
SERVER_URLS         = "conf.worker_http_server_urls"
SERVER_URLS_PRIVATE = "conf.worker_http_server_urls_private"
SERVER_IPS          = "conf.worker_http_server_ips"
SERVER_PORTS        = "conf.worker_http_server_ports"

# The scheme of a rank's own listener
USE_HTTPS           = "conf.use_https"

HOST_ADDRESS        = "conf.host{}_address"
RANK_ADDRESS        = "conf.rank{}_ip_address"

TRUE                = "TRUE"


# ---------------------------------------------------------------------------
# Host manager addresses
# ---------------------------------------------------------------------------

def get_is_httpd_enabled( system_properties ):
    """Whether the cluster reports an httpd proxy in front of its ranks."""
    return str( (system_properties or {}).get( ENABLE_HTTPD, "" ) ).upper() == TRUE
# end get_is_httpd_enabled


def _int_property( system_properties, key, fallback ):
    """An integer property, or `fallback` where it is absent or not one."""
    try:
        return int( str( system_properties[ key ] ).strip() )
    except Exception:
        return fallback
# end _int_property


def host_manager_path_for( path ):
    """The host manager's path beside a head node reached through httpd: the
    head node's path with its last segment replaced by gpudb-host-manager.
    """
    last_slash = path.rfind( "/" )
    if last_slash < 0:
        return "/gpudb-host-manager"
    return path[ : last_slash + 1 ] + "gpudb-host-manager"
# end host_manager_path_for


def published_host_manager_url( system_properties, head_node_url ):
    """The host manager address the deployment publishes for the host
    `head_node_url` names, or None where none is published for it.  Matched by
    host rather than by index, since the property is indexed by host number
    and what is held here is an address.  A malformed entry is passed over.
    """
    if (not system_properties) or (head_node_url is None):
        return None

    for host in range( _int_property( system_properties, NUM_HOSTS, 0 ) ):
        value = str( system_properties.get( HM_PUBLIC_URL.format( host ) ) or "" ).strip()
        if not value:
            continue
        try:
            candidate = GPUdb.URL( value )
        except Exception as ex:
            _log.debug( "Ignoring malformed published host manager URL <{}>: {}".format( value, ex ) )
            continue
        if candidate.host == head_node_url.host:
            return candidate
    return None
# end published_host_manager_url


def published_host_manager_port( system_properties, fallback ):
    """The host manager port the cluster reports, or `fallback`."""
    return _int_property( system_properties or {}, HM_PORT, fallback )
# end published_host_manager_port


def build_host_manager_url( head_node_url, is_httpd_enabled, system_properties,
                            host_manager_port, default_host_manager_port ):
    """The host manager address for the host a connection is addressed by:

    1. the address the deployment publishes for that host, where it does;
    2. otherwise, with httpd enabled and a path in the address, the head
       node's own address -- its port too -- with gpudb-host-manager as the
       last path segment: httpd proxies the host manager beside the ranks;
    3. otherwise, the head node's host at the host manager port, with no path:
       the port the cluster reports (conf.hm_http_port), unless the caller set
       one other than the default, which is theirs to keep.

    Given no system properties, as for a cluster that was down when the
    connection was made, httpd is told from the address having a path.
    """
    published = published_host_manager_url( system_properties, head_node_url )
    if published is not None:
        _log.debug( "Using the published host manager URL: {}".format( published ) )
        return published

    if is_httpd_enabled and head_node_url.path:
        address = head_node_url.url[ : len( head_node_url.url ) - len( head_node_url.path ) ]
        return GPUdb.URL( address + host_manager_path_for( head_node_url.path ) )

    port = ( published_host_manager_port( system_properties, host_manager_port )
             if (host_manager_port == default_host_manager_port) else host_manager_port )
    return GPUdb.URL( "{}://{}:{}".format( head_node_url.protocol.lower(), head_node_url.host, port ) )
# end build_host_manager_url


# ---------------------------------------------------------------------------
# Single addresses
# ---------------------------------------------------------------------------

def get_is_multi_head_enabled( system_properties ):
    """Whether the cluster has multi-head I/O (worker HTTP servers) enabled."""
    return str( system_properties.get( ENABLE_MULTI_HEAD, "" ) ).upper() == TRUE
# end get_is_multi_head_enabled


def rank_label( rank_index ):
    """Names a rank for a message; rank 0 is the head rank."""
    return "the head rank" if (rank_index == 0) else "worker rank {}".format( rank_index )
# end rank_label


def has_usable_host( url ):
    """Whether a parsed address names a host that can be connected to."""
    return (url is not None) and bool( url.host )
# end has_usable_host


def _names_a_host( text ):
    """Whether address text names a host.  Checked before the text is parsed
    into a :class:`GPUdb.URL`, which fills in the local host for an address
    naming none (``http://:9191``) -- the one thing an advertised address must
    never be turned into.
    """
    try:
        return bool( urlsplit( text if ("://" in text) else ("http://" + text) ).hostname )
    except ValueError:
        return False
# end _names_a_host


def parse_usable_alternate_urls( alternate_list, what ):
    """The usable addresses in one comma-separated list of a rank's (or a
    cluster's) alternatives, in the order given.  An unusable one is skipped,
    not fatal, so that it cannot mask a good one after it.
    """
    usable = []
    for text in alternate_list.split( "," ):
        if not text:
            continue

        if not _names_a_host( text ):
            _log.debug( "Skipping {} naming no host <{}>".format( what, text ) )
            continue

        try:
            url = GPUdb.URL( text )
        except Exception as ex:
            _log.debug( "Skipping unusable {} <{}>: {}".format( what, text, ex ) )
            continue

        usable.append( url )
    # end for

    return usable
# end parse_usable_alternate_urls


def _regex_given( hostname_regex ):
    if hostname_regex is None:
        return False
    pattern = getattr( hostname_regex, "pattern", hostname_regex )
    return bool( pattern )
# end _regex_given


def hostname_regex_matches( hostname_regex, candidate ):
    """Whether the hostname regex selects an address's host.  The match is
    anchored at the start only, so a pattern is not implicitly
    exact: end it with ``$`` to name one address.
    """
    return re.match( hostname_regex, candidate ) is not None
# end hostname_regex_matches


def filter_urls_by_hostname_regex( candidates, hostname_regex, what ):
    """The candidates whose host the regex selects, in the order given; all of
    them if no regex is given.
    """
    if not _regex_given( hostname_regex ):
        return candidates

    retained = []
    for url in candidates:
        if hostname_regex_matches( hostname_regex, url.host ):
            _log.debug( "Keeping matching {}: {}".format( what, url ) )
            retained.append( url )
        else:
            _log.debug( "Skipping non-matching {}: {}".format( what, url ) )
    return retained
# end filter_urls_by_hostname_regex


# ---------------------------------------------------------------------------
# Locality: an address naming this machine cannot name a server reached
# elsewhere
# ---------------------------------------------------------------------------

def _is_local_interface_address( family, address ):
    """Whether an address is assigned to an interface on this machine: binding
    to one succeeds only if it is.
    """
    try:
        with socket.socket( family, socket.SOCK_DGRAM ) as probe:
            probe.bind( (address, 0) )
        return True
    except OSError:
        return False
# end _is_local_interface_address


def is_local_address( host ):
    """Whether a host name or address names this machine: whether any address
    it resolves to is loopback or sits on a local interface.  False if it
    cannot be resolved.
    """
    if not host:
        return False

    try:
        infos = socket.getaddrinfo( host.strip( "[]" ), None )
    except (OSError, UnicodeError) as ex:
        _log.debug( "Could not classify <{}> as local: {}".format( host, ex ) )
        return False

    for family, _, _, _, sockaddr in infos:
        address = sockaddr[0]
        try:
            ip = ipaddress.ip_address( address.split( "%" )[0] )
        except ValueError:
            continue

        if ip.is_loopback:
            return True
        if (not ip.is_unspecified) and _is_local_interface_address( family, address ):
            return True
    # end for

    return False
# end is_local_address


def _is_credible_address( url, server_is_local, locality_cache ):
    """Whether an advertised address may be used: always where the server is
    on this machine; otherwise unless it names this machine, which cannot be
    the server.  An address a probe sent there would reach whatever is
    listening locally, and that answers just as the right server would.
    """
    if server_is_local or (url is None):
        return True

    local = locality_cache.get( url.host )
    if local is None:
        local = is_local_address( url.host )
        locality_cache[ url.host ] = local
    return not local
# end _is_credible_address


def rank_urls_if_all_credible( urls, server_is_local, what ):
    """The rank addresses, or an empty list where any of them names this
    machine and the server is elsewhere.

    All or nothing: the list is rank-indexed (``None`` being a removed rank),
    so dropping one entry would shift every rank above it and misroute sharded
    records.  One incredible address withdraws the list, and multi-head with it.
    """
    if server_is_local or not urls:
        return urls

    locality_cache = {}
    for url in urls:
        if not _is_credible_address( url, server_is_local, locality_cache ):
            # Not a warning: the worker probe re-derives the addresses, setting
            # aside only the ones naming this machine, and warns if multi-head
            # is then lost
            _log.debug( "Not using the {}es advertised for this cluster: it advertises <{}> "
                        "for a rank, which names this machine and so cannot name a server "
                        "reached elsewhere; the list is rank-indexed, so the rest cannot be "
                        "used without it".format( what, url ) )
            return []
    return urls
# end rank_urls_if_all_credible


def retain_credible_urls( urls, server_is_local, what ):
    """The candidates, out of one rank's alternatives, that could name the
    server, in their order.  Safe to filter, unlike the rank list: removing an
    alternative shifts nothing.
    """
    if server_is_local or (urls is None):
        return urls

    locality_cache = {}
    retained = []
    for url in urls:
        if _is_credible_address( url, server_is_local, locality_cache ):
            retained.append( url )
        else:
            _log.debug( "Skipping {} <{}>: it names this machine, not a server reached "
                        "elsewhere".format( what, url ) )
    return retained
# end retain_credible_urls


# ---------------------------------------------------------------------------
# Phase 1: each rank's candidate addresses
# ---------------------------------------------------------------------------

def extract_rank_slot_fields( system_properties, key ):
    """A ';'-separated per-rank property split into its rank slots; a removed
    rank is an empty field.  Empty if the property is absent.
    """
    value = system_properties.get( key )
    return value.split( ";" ) if value else []
# end extract_rank_slot_fields


def get_rank_slot_field( fields, slot ):
    """The field for a rank slot, or "" where the property does not reach it."""
    return fields[ slot ] if (slot < len( fields )) else ""
# end get_rank_slot_field


def build_enumerated_candidate_urls( ips_field, port_field, use_https ):
    """One rank's addresses built from its host's interface addresses and the
    rank's port.  The scheme is the rank's own listener's (``conf.use_https``),
    not the advertised addresses', which may be an httpd proxy's.  An IPv6
    literal is skipped rather than emitted unbracketed.
    """
    built = []
    if (not ips_field) or (not port_field):
        return built

    scheme = "https" if use_https else "http"
    for address in ips_field.split( "," ):
        if not address:
            continue
        if ":" in address:
            _log.debug( "Skipping enumerated address needing IPv6 bracketing <{}>".format( address ) )
            continue

        text = "{}://{}:{}".format( scheme, address, port_field )
        try:
            url = GPUdb.URL( text )
        except Exception as ex:
            _log.debug( "Skipping unbuildable enumerated address <{}>: {}".format( text, ex ) )
            continue
        if has_usable_host( url ):
            built.append( url )
    # end for

    return built
# end build_enumerated_candidate_urls


def order_rank_candidate_urls( advertised, private_urls, enumerated ):
    """One rank's candidates, configured addresses ahead of discovered ones,
    without duplicates.

    An advertised address on a host the interfaces enumerate is itself
    discovered (with no public address configured, the advertised list is the
    enumeration); otherwise it is configured.  So the order is: configured
    advertised, private, discovered advertised, enumerated.
    """
    enumerated_hosts = { url.host for url in enumerated }

    configured_advertised = [ url for url in advertised if url.host not in enumerated_hosts ]
    discovered_advertised = [ url for url in advertised if url.host in enumerated_hosts ]

    ordered = []
    seen = set()
    for tier in (configured_advertised, private_urls, discovered_advertised, enumerated):
        for url in tier:
            if str( url ) not in seen:
                seen.add( str( url ) )
                ordered.append( url )
    return ordered
# end order_rank_candidate_urls


def extract_rank_candidate_urls( system_properties ):
    """Each rank slot's candidate addresses, rank 0 (the head) first: ``None``
    for a removed rank, so that indices stay rank numbers; an empty list for a
    rank that advertised nothing usable.  Empty if multi-head is disabled or no
    rank addresses are advertised.
    """
    candidates = []

    if not get_is_multi_head_enabled( system_properties ):
        _log.debug( "Distributed I/O not enabled on server; skipping rank URL retrieval" )
        return candidates

    advertised_value = system_properties.get( SERVER_URLS )
    if not advertised_value:
        _log.debug( "No entry for <{}> in system properties".format( SERVER_URLS ) )
        return candidates

    advertised_fields = advertised_value.split( ";" )
    private_fields    = extract_rank_slot_fields( system_properties, SERVER_URLS_PRIVATE )
    ips_fields        = extract_rank_slot_fields( system_properties, SERVER_IPS )
    port_fields       = extract_rank_slot_fields( system_properties, SERVER_PORTS )

    use_https = str( system_properties.get( USE_HTTPS, "" ) ).upper() == TRUE

    for i, advertised_field in enumerate( advertised_fields ):
        # A removed rank keeps its slot; the server empties all of its fields
        if not advertised_field:
            candidates.append( None )
            continue

        advertised   = parse_usable_alternate_urls( advertised_field, "rank URL" )
        private_urls = parse_usable_alternate_urls( get_rank_slot_field( private_fields, i ),
                                                    "private rank URL" )
        enumerated   = build_enumerated_candidate_urls( get_rank_slot_field( ips_fields, i ),
                                                        get_rank_slot_field( port_fields, i ),
                                                        use_https )

        candidates.append( order_rank_candidate_urls( advertised, private_urls, enumerated ) )
    # end for

    return candidates
# end extract_rank_candidate_urls


# ---------------------------------------------------------------------------
# Phase 2: keep what the hostname regex selects, and pick one per rank
# ---------------------------------------------------------------------------

def retain_matching_rank_candidate_urls( candidates, hostname_regex ):
    """Each rank's candidates that the hostname regex selects (all of them
    without one), ``None`` kept for a removed rank.

    Raises:
        GPUdbException  if a rank advertised nothing usable
        GPUdbHostnameRegexFailureException  if the regex rejected all of a
            rank's usable addresses
    """
    retained_by_rank = []

    for i, rank_candidates in enumerate( candidates ):
        if rank_candidates is None:
            retained_by_rank.append( None )
            continue

        if not rank_candidates:
            raise GPUdbException( "No valid IP/hostname found for {}".format( rank_label( i ) ) )

        retained = filter_urls_by_hostname_regex( rank_candidates, hostname_regex, "rank URL" )
        if not retained:
            raise GPUdbHostnameRegexFailureException(
                "No valid matching IP/hostname found for {}".format( rank_label( i ) ) )

        retained_by_rank.append( retained )
    # end for

    return retained_by_rank
# end retain_matching_rank_candidate_urls


def select_rank_urls( candidates, hostname_regex ):
    """One address per rank slot, the first the regex keeps, before any probe
    has run; ``None`` for a removed rank.
    """
    return [ (None if (retained is None) else retained[0])
             for retained in retain_matching_rank_candidate_urls( candidates, hostname_regex ) ]
# end select_rank_urls


def extract_rank_urls( system_properties, hostname_regex ):
    """The head and worker rank addresses selected from the system properties,
    the head first, ``None`` for a removed rank.  Empty if multi-head is
    disabled or no rank addresses are advertised.
    """
    return select_rank_urls( extract_rank_candidate_urls( system_properties ), hostname_regex )
# end extract_rank_urls


# ---------------------------------------------------------------------------
# Ranks by host
# ---------------------------------------------------------------------------

def extract_rank_host_mapping( system_properties, rank_slots ):
    """Each rank slot's host index, joining ``conf.rank<r>_ip_address`` with
    ``conf.host<h>_address`` on the raw configured address; -1 where the rank
    is removed or its host cannot be identified.
    """
    rank_host = [ -1 ] * rank_slots

    try:
        host_count = int( system_properties.get( NUM_HOSTS ) )
    except (TypeError, ValueError):
        _log.debug( "No usable host count; ranks will not be grouped by host" )
        return rank_host

    # A host with no address contributes nothing, rather than matching every
    # rank that also has none
    host_index_by_address = {}
    for h in range( host_count ):
        address = system_properties.get( HOST_ADDRESS.format( h ) )
        if address:
            host_index_by_address[ address ] = h

    for r in range( rank_slots ):
        address = system_properties.get( RANK_ADDRESS.format( r ) )
        if not address:
            continue    # a removed rank
        host = host_index_by_address.get( address )
        if host is None:
            _log.debug( "No host matches the address of {} <{}>; it will not be "
                        "grouped".format( rank_label( r ), address ) )
            continue
        rank_host[ r ] = host
    # end for

    return rank_host
# end extract_rank_host_mapping


def get_non_identifying_hosts( candidate_urls, rank_host ):
    """The hosts (names or addresses) advertised under more than one host,
    which so identify none of them: a container bridge gateway is the same
    string on every machine.  Such an address may be used for the rank that
    named it, never adopted as a host's preferred one.
    """
    first_host_by_address = {}
    non_identifying = set()

    for rank, candidates in enumerate( candidate_urls ):
        if candidates is None:
            continue    # a removed rank
        host = rank_host[ rank ] if (rank < len( rank_host )) else -1
        if host < 0:
            continue    # ungrouped: says nothing about any host

        for candidate in candidates:
            seen = first_host_by_address.setdefault( candidate.host, host )
            if seen != host:
                non_identifying.add( candidate.host )
    # end for

    if non_identifying:
        _log.debug( "Addresses advertised under more than one host, and so usable only "
                    "for the rank that named them: {}".format( sorted( non_identifying ) ) )
    return non_identifying
# end get_non_identifying_hosts


def order_urls_by_unique_hosts_first( candidates, non_identifying ):
    """The candidates naming a single host first, the ambiguous ones after, in
    their order; deprioritized, never dropped.  For the head node's address,
    which the cluster is addressed by from then on.
    """
    if not non_identifying:
        return candidates
    identifying = [ url for url in candidates if url.host not in non_identifying ]
    ambiguous   = [ url for url in candidates if url.host in non_identifying ]
    if ambiguous:
        _log.debug( "Head rank addresses naming more than one host, tried last: "
                    "{}".format( [ str( url ) for url in ambiguous ] ) )
    return identifying + ambiguous
# end order_urls_by_unique_hosts_first


def order_urls_by_preferred_host_first( candidates, preferred_host ):
    """The candidates on the preferred host first, every other one after, in
    their order.  Matched on the host alone: the preferred address was proven
    by another rank, with its own port and path.
    """
    if preferred_host is None:
        return candidates
    preferred = [ url for url in candidates if url.host == preferred_host ]
    rest      = [ url for url in candidates if url.host != preferred_host ]
    return preferred + rest
# end order_urls_by_preferred_host_first


# ---------------------------------------------------------------------------
# Cluster identity
# ---------------------------------------------------------------------------

def cluster_identity_signature( system_properties ):
    """A value that differs between two clusters whose configurations differ:
    ring name, cluster name, and advertised and private rank addresses.

    One-sided: differing signatures prove two addresses are different
    clusters; matching ones prove nothing.  Use it to reject, never to confirm.
    """
    if system_properties is None:
        return ""

    signature = []
    for key in (RING_NAME, CLUSTER_NAME, SERVER_URLS, SERVER_URLS_PRIVATE):
        value = system_properties.get( key )
        if value and str( value ).strip():
            signature.append( "{}={}\n".format( key, str( value ).strip() ) )
    return "".join( signature )
# end cluster_identity_signature


# ---------------------------------------------------------------------------
# Phase 3: probing.  `is_running(url)` says whether a rank answers there, and
# `properties_at(url)` returns the system properties an address reports.
# ---------------------------------------------------------------------------

def _is_rank_running( url, is_running ):
    """Whether a rank answers at the address; any error means it does not,
    which costs multi-head rather than the connection.
    """
    try:
        return bool( is_running( url ) )
    except Exception as ex:
        _log.debug( "Rank address <{}> could not be probed: {}".format( url, ex ) )
        return False
# end _is_rank_running


def _is_different_cluster( candidate, reference_signature, properties_at, seen ):
    """Whether an address that answered reports a different cluster than the
    reference.  Rejects on evidence only: properties that cannot be read are no
    objection.
    """
    if not reference_signature:
        return False

    address = str( candidate )
    if address in seen:
        return seen[ address ]

    different = False
    try:
        signature = cluster_identity_signature( properties_at( candidate ) )
        different = bool( signature ) and (signature != reference_signature)
        if different:
            _log.debug( "Address <{}> answers, but reports a different cluster than the "
                        "one reached; skipping it".format( candidate ) )
    except Exception as ex:
        _log.debug( "Could not read properties at <{}> to compare clusters: "
                    "{}".format( candidate, ex ) )

    seen[ address ] = different
    return different
# end _is_different_cluster


def get_running_rank_url( candidates, is_running, answered_by_address = None, what = "rank",
                          reference_signature = None, properties_at = None,
                          different_cluster_by_address = None ):
    """The first candidate that answers -- and, given a reference signature,
    does not report a different cluster -- or None.

    The one implementation of "search this rank's addresses", for the head rank
    at connect and for each rank in a probe, so that the two cannot disagree
    about what a failure means.
    """
    if different_cluster_by_address is None:
        different_cluster_by_address = {}

    for candidate in candidates:
        address = str( candidate )
        answered = None if (answered_by_address is None) else answered_by_address.get( address )
        if answered is None:
            answered = _is_rank_running( candidate, is_running )
            if answered_by_address is not None:
                answered_by_address[ address ] = answered

        if answered:
            # Answering is not belonging: where the server is on this machine,
            # a second instance on another port answers just as well
            if ( reference_signature
                 and _is_different_cluster( candidate, reference_signature, properties_at,
                                            different_cluster_by_address ) ):
                continue
            return candidate

        _log.debug( "{} did not answer at <{}>".format( what, candidate ) )
    # end for

    return None
# end get_running_rank_url


def probe_rank_candidate_urls( candidate_urls, rank_host, head_node_url, is_running,
                               reference_signature = None, properties_at = None ):
    """Probe each worker rank's candidates for one that answers.

    Once an address answers for a rank, the other ranks on its host try that
    host's address first (every rank binds on every one of its host's
    interfaces), unless the address names more than one host.  The head node's
    own address is the head rank's host's address to begin with.

    Returns:
        (worker_urls, all_reachable): a worker address per rank slot after the
        head's, so entry i is rank i + 1, ``None`` for a removed rank and the
        first candidate for a rank that answered on none; and whether every
        live rank answered.
    """
    answered_by_address = {}
    different_cluster_by_address = {}
    preferred_host_by_host = {}

    non_identifying = get_non_identifying_hosts( candidate_urls, rank_host )

    if ( rank_host and (rank_host[0] >= 0) and (head_node_url is not None)
         and (head_node_url.host not in non_identifying) ):
        preferred_host_by_host[ rank_host[0] ] = head_node_url.host

    selected = []
    all_reachable = True

    # Slot 0 is the head rank, which is not a worker
    for rank in range( 1, len( candidate_urls ) ):
        candidates = candidate_urls[ rank ]
        if candidates is None:
            selected.append( None )     # a removed rank keeps its slot
            continue

        host = rank_host[ rank ] if (rank < len( rank_host )) else -1
        preferred_host = preferred_host_by_host.get( host ) if (host >= 0) else None

        answered = get_running_rank_url(
            order_urls_by_preferred_host_first( candidates, preferred_host ),
            is_running,
            answered_by_address,
            rank_label( rank ),
            reference_signature,
            properties_at,
            different_cluster_by_address )

        if answered is None:
            _log.debug( "{} answered on none of {}".format(
                rank_label( rank ), [ str( url ) for url in candidates ] ) )
            selected.append( candidates[0] if candidates else None )
            all_reachable = False
            continue

        selected.append( answered )

        # A success settles the address for the host -- where it names one
        if host >= 0:
            if answered.host in non_identifying:
                _log.debug( "{} answered at <{}>, which names more than one host; using it "
                            "for this rank only".format( rank_label( rank ), answered ) )
            else:
                preferred_host_by_host.setdefault( host, answered.host )
    # end for

    return selected, all_reachable
# end probe_rank_candidate_urls


def resolve_reachable_head_node_url( system_properties, selected, server_is_local,
                                     hostname_regex, is_running, properties_at ):
    """The address the cluster's head rank answers on, searched across every
    address advertised for it, starting with the one selection preferred; or
    None if it answers on none.  An address naming this machine is not tried
    where the server is elsewhere; where the server is here, an address that
    reports a different cluster is passed over.
    """
    candidates = None

    try:
        retained = retain_matching_rank_candidate_urls(
            extract_rank_candidate_urls( system_properties ), hostname_regex )
        retained = [ retain_credible_urls( rank_candidates, server_is_local, "head rank candidate" )
                     for rank_candidates in retained ]

        if retained:
            # An address naming several hosts must not become what the cluster
            # is addressed by while a specific one is available
            rank_host = extract_rank_host_mapping( system_properties, len( retained ) )
            candidates = order_urls_by_unique_hosts_first(
                retained[0], get_non_identifying_hosts( retained, rank_host ) )
    except GPUdbException as ex:
        # Selection already produced `selected` from these properties, so a
        # failure here means only that there are no alternatives to try
        _log.debug( "Could not re-derive head rank candidates: {}".format( ex ) )

    if not candidates:
        candidates = [ selected ]

    reference = cluster_identity_signature( system_properties ) if server_is_local else None

    return get_running_rank_url( candidates, is_running, None, "Head rank",
                                 reference, properties_at, {} )
# end resolve_reachable_head_node_url


def resolve_worker_rank_urls( system_properties, hostname_regex, head_node_url,
                              is_running, properties_at ):
    """The worker rank addresses a client can use, probed: each rank's
    candidates, kept by the hostname regex, tried in order.

    An address naming this machine is not tried where the server is
    elsewhere, for the same reason as at connect: it would
    reach whatever is listening locally.

    Returns:
        (worker_urls, available): one address per worker rank slot, ``None``
        for a removed rank; and whether multi-head can be used, which needs
        every live rank to have answered.

    Raises:
        GPUdbException, GPUdbHostnameRegexFailureException  as
            :func:`retain_matching_rank_candidate_urls`
    """
    retained = retain_matching_rank_candidate_urls(
        extract_rank_candidate_urls( system_properties ), hostname_regex )
    if not retained:
        return [], False

    server_is_local = (head_node_url is not None) and is_local_address( head_node_url.host )
    if not server_is_local:
        retained = [ retain_credible_urls( rank_candidates, False, "rank candidate" )
                     for rank_candidates in retained ]

    rank_host = extract_rank_host_mapping( system_properties, len( retained ) )
    reference = cluster_identity_signature( system_properties ) if server_is_local else None

    worker_urls, all_reachable = probe_rank_candidate_urls(
        retained, rank_host, head_node_url, is_running, reference, properties_at )

    return worker_urls, (all_reachable and bool( worker_urls ))
# end resolve_worker_rank_urls
