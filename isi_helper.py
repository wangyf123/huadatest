"""
Collection of functions that talk to snapshot and changelist PAPI endpoints
"""
import utils
import sys
from retrying import retry
import retry_settings
from requests.exceptions import URLRequired, MissingSchema, InvalidSchema, \
    InvalidURL, RequestException, HTTPError
import requests
import urllib3
import time
import os
import logging
import isi_sdk_utils
import json
import gevent
import gevent.pool
import copy

g_charset_detector = None
try:
    # in limited test cases it seemed like cchardet detected the encoding
    # correctly more often than pyicu or chardet so use it when json
    # library fails.
    import cchardet
    g_charset_detector = cchardet
except ImportError:
    pass


g_config = dict()

g_metadata_fields = "name,is_hidden,size,block_size,blocks,last_modified," \
    "change_time,access_time,create_time,mtime_val,ctime_val," \
    "atime_val,btime_val,owner,group,uid,gid,id,nlink,type,mode," \
    "container_path"

# multiple simultaneous requests are used for get_changelist_entries_metadata
g_async_request_pool = gevent.pool.Pool(5)

g_isi_sdk = None
g_isi_api_client = None

ISI_PROTOCOL = "https"
ISI_PORT = 8080
CHANGELIST_JOB_TIMEOUT = 129600
# if it takes longer than a day and a half to generate a changelist then it
# probably means you'd be better off running the initial_indexer.py

LOG = logging.getLogger(__name__)


def get_logger():
    return LOG


class NamespaceJsonLoader(object):
    """
    The /namespace end point (i.e. RAN API) has the potential to return
    filenames that are encoded in non-UTF-8 encodings and even in a mix of
    encodings, so we use this to try to guess the correct encoding.
    """

    def __init__(self):
        self.default_encoding = None
        if g_charset_detector is None:
            LOG.warning("Failed to import cchardet, "
                        "using fallback encoding only.")
            self.detect = None
        else:
            self.detect = self._cchardet_detect

    def _get_fallback_encoding(self):
        """
        This encoding will never fail, but it might produce incorrect results.
        If the text contains a mix of encodings then this might be the only
        encoding that works though.
        """
        return "ISO-8859-1"

    def _cchardet_detect(self, text):
        try:
            result = cchardet.detect(text)
            return result["encoding"]
        except Exception as exc:
            LOG.warning("cchardet failed to detect encoding in (%s).\n(%s)",
                        text, str(exc))
            return self._get_fallback_encoding()

    def loads(self, text):
        """
        Try default encoding, which if it is None then json assumes 'utf-8',
        if that fails then try to detect it, if that fails then use the
        fallback encoding.
        """
        encoding = self.default_encoding
        for attempt in range(0, 3):
            try:
                if encoding:
                    decode_text = text.decode(encoding)
                    return json.loads(decode_text)
                # return json.loads(text, encoding)
                return json.loads(text)
            except (UnicodeDecodeError, LookupError) as exc:
                if attempt == 0 and self.detect is not None:
                    LOG.info("Caught (%s) loading:\n%s...\nwith "
                             "encoding '%s'.",
                             str(exc), text[:1000], str(encoding))
                    encoding = self.detect(text)
                    LOG.info("Trying encoding '%s'.", encoding)
                elif encoding != self._get_fallback_encoding():
                    encoding = self._get_fallback_encoding()
                    LOG.warning("Trying fallback encoding '%s'%s.", encoding,
                                " Installing cchardet might help."
                                if self.detect is None else "")
                else:
                    LOG.error("Unable to find suitable encoding for:\n%s.",
                              text)
                    break
            except Exception as gen_exc:
                LOG.error("Caught unexpected (%s) exception loading json: (%s)",
                          str(gen_exc), text)


g_namespace_json_loader = NamespaceJsonLoader()
g_headers = {}


def prepare_headers(onefs_host, onefs_user, onefs_pass, verify_ssl):

    url = "https://{}:8080/session/1/session".format(onefs_host)

    payload = json.dumps({"username": onefs_user, "password": onefs_pass, "services": [
                         "platform", "namespace", "remote-service"]})
    headers = {'Content-Type': 'application/json'}

    response = requests.request(
        "POST", url, headers=headers, data=payload, verify=verify_ssl)

    headers["Cookie"] = response.headers['Set-Cookie']
    headers["X-CSRF-Token"] = response.headers['Set-Cookie'].split()[5].strip(";").split("=")[1]
    headers["Origin"] = 'https://{}:8080'.format(onefs_host)
    return headers


def init(onefs_host, onefs_user, onefs_pass, verify_ssl,
         default_encoding=None, onefs_ips=None):
    """ Init Stuff """
    g_config['onefs_host'] = onefs_host
    g_config['onefs_user'] = onefs_user
    g_config['onefs_pass'] = onefs_pass
    g_config['verify_ssl'] = verify_ssl

    if verify_ssl is False:
        urllib3.disable_warnings()
        requests.packages.urllib3.disable_warnings()

    global g_isi_sdk
    global g_isi_api_client
    try:
        g_isi_sdk, g_isi_api_client, version = \
            isi_sdk_utils.configure(
                onefs_host, onefs_user, onefs_pass, verify_ssl)
    except RuntimeError as exc:
        print("Failed to configure SDK for cluster %s. "
              "Exception raised: %s" % (onefs_host, str(exc)), file=sys.stderr)
        sys.exit(1)

    if int(version) >= 8:
        global g_metadata_fields
        g_metadata_fields += ",stub"

    if int(version) >= 9:
        global g_headers
        g_headers = prepare_headers(
            onefs_host, onefs_user, onefs_pass, verify_ssl)

    if onefs_ips is not None:
        isi_ip_list = [ip.strip() for ip in onefs_ips.split(',')]
    else:
        try:
            isi_ip_list = _get_external_ips()
        except RequestException as exc:
            print("Failed to get external IPs for cluster %s. "
                  "Exception raised: %s" % (onefs_host, str(exc)), file=sys.stderr)
            sys.exit(1)

    g_config["isi_ip_list"] = isi_ip_list

    g_namespace_json_loader.default_encoding = default_encoding


def test_cluster_ips(rank):
    ip_list = g_config["isi_ip_list"]

    if rank == 0:
        print("Checking connection to Cluster IPs: " + str(ip_list))

    async_request_pool = gevent.pool.Pool(len(ip_list))
    queries = []
    for ip_address in ip_list:
        job = async_request_pool.spawn(_test_namespace_query, ip_address)
        queries.append((job, ip_address))

    async_request_pool.join(raise_error=True)

    valid_ips = []
    for job, ip_address in queries:
        if job.value is True:
            valid_ips.append(ip_address)
        else:
            print("Rank(%d): failed to connect to %s, "
                  "dropping it." % (rank, ip_address), file=sys.stderr)

    g_config["isi_ip_list"] = valid_ips

    if len(g_config["isi_ip_list"]) == 0:
        print("Found no valid IPs amongst %s for cluster %s."
              % (str(ip_list), g_config["onefs_host"]), file=sys.stderr)
        sys.exit(1)


def _test_namespace_query(ip_address):
    try:
        # this assumes that url starts with '/'
        url = "/namespace/ifs"
        request_url = ("{0}://{1}:{2}{3}"
                       .format(ISI_PROTOCOL, ip_address, ISI_PORT, url))
        global g_headers
        if g_headers:
            headers = copy.deepcopy(g_headers)
            headers["Origin"] = "https://{}:{}".format(ip_address, ISI_PORT)
            resp = requests.get(request_url, headers=headers,
                                verify=g_config['verify_ssl'])
        else:
            resp = requests.get(request_url,
                                auth=(g_config['onefs_user'],
                                      g_config['onefs_pass']),
                                verify=g_config['verify_ssl'],
                                timeout=1.0)

        # if the request is unsuccessful then raise an exception
        resp.raise_for_status()
        # if no exception is raised then return True
        return True
    except RequestException:
        return False


### Snapshot Routines ###
def create_snapshot(snap_dir):
    """ Creates a snapshot """
    snapshotApi = g_isi_sdk.SnapshotApi(g_isi_api_client)
    # Create unique-enough snapshot name
    snap_name = 'isi_metadata_indexer_{0}'.format(utils.gen_unique_name())
    snapshotCreateParams = g_isi_sdk.SnapshotSnapshotCreateParams(
        name=snap_name, path=snap_dir)
    try:
        resp = snapshotApi.create_snapshot_snapshot(snapshotCreateParams)
        # OneFS v7.0 doesn't return the name
        if resp.name is None:
            resp.name = snap_name
        return resp.id, resp.name
    except (urllib3.exceptions.HTTPError,
            g_isi_sdk.rest.ApiException) as exc:
        LOG.error("Could not take snapshot of %s.\n(%s)",
                  snap_dir, str(exc))
    return None, None


def cleanup_stale_snapshots(snap_dir, current_snapshot_name, confirm_cb=None):
    # delete all indexer snapshots that aren't the current snapshot
    snapshotApi = g_isi_sdk.SnapshotApi(g_isi_api_client)
    try:
        resp = snapshotApi.list_snapshot_snapshots()
    except (urllib3.exceptions.HTTPError,
            g_isi_sdk.rest.ApiException) as exc:

        return False, exc.message
    success = True
    snap_template_name = "isi_metadata_indexer_"
    for snapshot in resp.snapshots:
        if snapshot.name.startswith(snap_template_name) \
                and snapshot.path == snap_dir \
                and snapshot.name != current_snapshot_name:
            if confirm_cb is None \
                    or confirm_cb("Are you sure you want to "
                                  "delete snapshot %s" % snapshot.name) is True:
                if delete_snapshot(snapshot.name) is False:
                    success = False

    return success, None


def get_snapshot(snap_name_or_id):
    # Look up for snapshot on the cluster with the snap_name_or_id
    snapshotApi = g_isi_sdk.SnapshotApi(g_isi_api_client)
    try:
        resp = snapshotApi.get_snapshot_snapshot(snap_name_or_id)
        return resp.snapshots[0]
    except (urllib3.exceptions.HTTPError,
            g_isi_sdk.rest.ApiException) as exc:
        LOG.error("Could not get snapshot %s.\n(%s)",
                  str(snap_name_or_id), str(exc))
    return None


def delete_snapshot(snapshot_name):
    """
    Delete the specified snapshot.
    """
    snapshotApi = g_isi_sdk.SnapshotApi(g_isi_api_client)
    try:
        snapshotApi.delete_snapshot_snapshot(snapshot_name)
        return True
    except (urllib3.exceptions.HTTPError,
            g_isi_sdk.rest.ApiException) as exc:
        LOG.warning("Failed to delete snapshot named: %s.\n(%s)",
                    snapshot_name, str(exc))
    return False


def get_snap_path(path, snap_prefix):
    # the path has to start with /ifs so this will replace the initial /ifs
    # with /ifs/.snapshot/<name of snapshot>
    snap_path = snap_prefix + path[4:]

    return snap_path


### Changelist Routines ###
def create_changelist(old_snap_id, new_snap_id):
    """ Enlist a changelist create job and wait for it to complete/ fail """

    # Create a changelist job  these two snapshots
    job_id = create_changelist_job(old_snap_id, new_snap_id)

    # Check the job status - poll until success/ failure/ timeout
    status = check_job_status(job_id, CHANGELIST_JOB_TIMEOUT)

    if status == "succeeded":
        cl_name = str(old_snap_id) + '_' + str(new_snap_id)
        return cl_name

    LOG.error("Creation of changelist old:%s new:%s failed. Job=%d Status=%s",
              str(old_snap_id), str(new_snap_id), job_id, status)

    return None


def delete_changelist(changelist_name):
    """
    Delete the specified changelist
    """
    snapshotApi = g_isi_sdk.SnapshotApi(g_isi_api_client)
    try:
        snapshotApi.delete_snapshot_changelist(changelist_name)
        return True
    except (urllib3.exceptions.HTTPError,
            g_isi_sdk.rest.ApiException) as exc:
        LOG.error("Failed to delete changelist named: %s.\n(%s)",
                  changelist_name, str(exc))
    return False


def get_changelist_entries(cl_name, limit=None, resume=None):
    # Return up to limit entries of a changelist or resume from a previous get.

    # Can't use the g_isi_sdk here because it contains a bug in the
    # description which prevents it from being able to handle the limit and
    # resume parameters.
    # interestingly, the resume arg and limit arg are not permitted together.
    # the way this works is that the limit is set by first query and then the
    # limit is basically embedded into the resume
    endpoint = ("/platform/3/snapshot/changelists/{0}/lins"
                .format(cl_name))

    url_params = ""
    if resume is not None:
        url_params = "?resume=" + resume
    elif limit is not None:
        url_params = "?limit=" + str(limit)

    res = _request_with_retry("GET", endpoint, url_params)

    json_resp = json.loads(res.content)
    cl_entries = json_resp['lins']
    try:
        resume = json_resp['resume']
    except KeyError:
        resume = None

    return cl_entries, resume


### Stat routines ###
def _url_encode(path):
    try:
        # the requests module only url-encodes non-reserved characters, so
        # we have to encode the reserved characters with quote
        return requests.utils.quote(path)
    except KeyError:
        # if the path contains unicode characters that aren't ascii encode-able
        # then quote throws a KeyError. Deal with that by re-encoding the path
        # with utf8. Also, urllib3 will log a warning when this happens, but
        # there doesn't seem to be any way to prevent the warning from being
        # logged.
        return requests.utils.quote(path.encode("utf8"))


def get_lin_from_path(path):
    """ Get LIN from ifs """
    # This is a workaround for bug 168687
    if path == "/":
        return 0  # root lin
    res = _namespace_request("GET", _url_encode(path), "?metadata")

    tmp = g_namespace_json_loader.loads(res.content)
    attr_list = tmp['attrs']

    for attr in attr_list:
        if attr['name'] == "id":
            return attr['value']

    return None


def get_stat_from_path(path):
    """
    Get stat in the form of a dictionary (for lookup).
    """
    # the namespace API has a bug where it doesn't return the correct type for
    # a metadata query, so we work around this by doing a query on the parent
    # directory.
    parent_path = os.path.dirname(path)
    if parent_path != "/":

        url_params = "?query&detail={0}".format(g_metadata_fields)

        name = os.path.basename(path)
        query = {"scope": {
            "logic": "or",
            "conditions":
            [{"operator": "=", "attr": "name", "value": name}]}}

        res = _namespace_request(
            "POST", _url_encode(parent_path), url_params, query)
        resp_json = g_namespace_json_loader.loads(res.content)
        stat_dict = resp_json['children'][0]

        return stat_dict

    # special case for the /ifs directory because we can only use RAN API
    # on /ifs and thus can't do a "query" on the parent of /ifs
    res = _namespace_request("GET", path, "?metadata")

    stat_dict = dict()
    resp_json = g_namespace_json_loader.loads(res.content)
    for attr in resp_json["attrs"]:
        stat_dict[attr["name"]] = attr['value']
    # add the name
    stat_dict["name"] = "ifs"
    stat_dict["container_path"] = "/"
    return stat_dict


def get_changelist_entries_metadata(
        snapshot_path, snapshot_root_name, changelist_entries, debug=False):
    # if the snapshot was done on the /ifs directory then there is no root
    # directory inside the /ifs/.snapshot/<snapshot_name>, whereas if the
    # snapshot was done on /ifs/alex then it will be
    # /ifs/.snapshot/<snapshot_name/alex, so the snapshot_root_name is "alex".
    if snapshot_root_name == 'ifs':
        snapshot_path, snapshot_root_name = os.path.split(snapshot_path)

    # get the metadata for each changelist entry and the parent directory
    # we need the parent directory so that we know each item's parent_lin
    # (which may or may not be included int the changelist_entries list)
    # return a dict whose key is the changelist entry's path
    # build the query
    queries = []
    # there's a limit of 8192 on the query size so we might have to break this
    # up into multiple queries

    class QueryData(object):
        def __init__(self,
                     query, conditions, query_size):
            self.query = query
            self.conditions = conditions
            self.query_size = query_size

    def _build_query():
        return {"scope": {"logic": "or", "conditions": []}}

    def _build_condition(entry_name):
        return {"operator": "=", "attr": "name", "value": entry_name}

    def _get_query_by_prefix(cur_dir, queries_by_prefix):
        try:
            # find a query that is on the same directory as this entry
            query_data = queries_by_prefix[cur_dir]
        except KeyError:
            # add query
            # start a new query
            query = _build_query()
            conditions = query["scope"]["conditions"]
            query_size = 0
            query_data = QueryData(query, conditions, query_size)
            queries_by_prefix[cur_dir] = query_data

        return query_data

    class DebugJob(object):
        def __init__(self, value):
            self.value = value

    # group queries by url prefix
    queries_by_prefix = {}
    # split the query if the length grows larger than max_query or the number
    # of conditions exceeds max_conditions
    max_query_size = 6000  # we will be conservative because we aren't gonna
    # check for the need to split the query after adding every condition
    max_conditions = 25

    results = dict()
    for entry in changelist_entries:
        # changelist entries either start with '/' or are '', the latter
        # indicates that the entry corresponds to the snapshot_root_name
        if entry["path"] in results:
            continue

        entry_path = '/' + snapshot_root_name + entry["path"]

        entry_dir, entry_name = os.path.split(entry_path)

        query_data = _get_query_by_prefix(entry_dir, queries_by_prefix)
        condition = \
            _build_condition(entry_name)
        query_data.conditions.append(condition)

        # check if we need to start new query
        query_data.query_size += len(entry_name)
        if query_data.query_size >= max_query_size \
                or len(query_data.conditions) >= max_conditions:
            # this query is full so spawn it
            if debug is False:
                job = g_async_request_pool.spawn(
                    _get_changelist_entries_metadata,
                    query_data.query, entry_dir,
                    snapshot_path, snapshot_root_name)
                queries.append(job)
            else:
                result = \
                    _get_changelist_entries_metadata(
                        query_data.query, entry_dir,
                        snapshot_path, snapshot_root_name)
                queries.append(DebugJob(result))
            # this query is spawned so delete it
            del queries_by_prefix[entry_dir]

        results[entry["path"]] = None
        # if the item is the snapshot_root_name directory then the
        # entry["path"] is set to the empty string.
        if entry["path"] == '':
            continue

        # check if the entry_dir components are part of the query yet, if not
        # then add them
        loop_entry_dir = os.path.dirname(entry_path)
        while loop_entry_dir != '/':
            loop_entry_dir_path, entry_dir_name = os.path.split(loop_entry_dir)
            if loop_entry_dir not in results:
                query_data = \
                    _get_query_by_prefix(
                        loop_entry_dir_path, queries_by_prefix)
                condition = \
                    _build_condition(entry_dir_name)
                query_data.conditions.append(condition)
                # check if we need to start new query
                query_data.query_size += len(entry_dir_name)
                if query_data.query_size >= max_query_size \
                        or len(query_data.conditions) >= max_conditions:
                    # add current query
                    if debug is False:
                        job = g_async_request_pool.spawn(
                            _get_changelist_entries_metadata,
                            query_data.query, loop_entry_dir_path,
                            snapshot_path, snapshot_root_name)
                        queries.append(job)
                    else:
                        result = \
                            _get_changelist_entries_metadata(
                                query_data.query, loop_entry_dir_path,
                                snapshot_path, snapshot_root_name)
                        queries.append(DebugJob(result))
                    # this query is spawned so delete it
                    del queries_by_prefix[loop_entry_dir_path]

                result_dir = loop_entry_dir[len(snapshot_root_name)+1:]
                results[result_dir] = None

            loop_entry_dir = loop_entry_dir_path

    # add the final query (if any)
    for url_prefix, query_data in queries_by_prefix.items():
        if debug is False:
            job = g_async_request_pool.spawn(
                _get_changelist_entries_metadata,
                query_data.query, url_prefix,
                snapshot_path, snapshot_root_name)
            queries.append(job)
        else:
            result = \
                _get_changelist_entries_metadata(
                    query_data.query, url_prefix,
                    snapshot_path, snapshot_root_name)
            queries.append(DebugJob(result))

    g_async_request_pool.join(raise_error=True)
    for job in queries:
        results.update(job.value)

    return results


def _get_changelist_entries_metadata(query, prefix,
                                     snapshot_path, snapshot_root_name):
    """
    Retrieve the metadata for items in a changelist.
    """
    url_params = "?query&detail={0}".format(g_metadata_fields)
    # Note that snapshot_path and prefix both start with '/'
    try:
        res = _namespace_request(
            "POST", snapshot_path + _url_encode(prefix), url_params, query)
    except HTTPError as exception:
        if exception.response is None or exception.response.status_code != 404:
            raise exception
        # 404 so return an empty dict
        return dict()

    results = dict()
    base_path_len = len(os.path.normpath(
        snapshot_path + '/' + snapshot_root_name))
    resp_json = g_namespace_json_loader.loads(res.content)
    for child in resp_json['children']:
        child_name = child['name']
        child_path = child['container_path']
        if child_name == snapshot_root_name and \
                child_path == snapshot_path:
            # handle special case for changelist entry that is for the
            # root snapshot directory, which is indicated by
            # entry["path"] == ''
            results[''] = child
            continue
        # strip the base_path and add name to get original entry path
        child_path = child_path[base_path_len:]
        if len(child_path) == 0:
            child_path = '/'
        entry_path = os.path.join(child_path, child_name)
        results[entry_path] = child

    return results


def _build_fullpath_condition(name, dir_path):
    """
    Build a condition that matches a file by name and container_path.
    """
    condition = dict()
    condition["logic"] = "and"
    condition["conditions"] = [
        {"operator": "=", "attr": "name", "value": name},
        {"operator": "=", "attr": "container_path", "value": dir_path}]

    return condition


def get_children(dir_path, limit, max_depth, resume=None):
    url_params = \
        ("?&detail={0}&limit={1}&max-depth={2}"
         .format(g_metadata_fields, limit, max_depth))

    if resume is not None:
        url_params += "&resume=" + resume

    res = _namespace_request("GET", _url_encode(dir_path), url_params)

    json_resp = g_namespace_json_loader.loads(res.content)
    try:
        resume = json_resp["resume"]
    except KeyError:
        resume = None

    return json_resp['children'], resume


def _namespace_request(method, path, url_params="", json_data=None,
                       headers=None):
    # this assumes that path starts with '/'
    url = "/namespace" + path
    return _request_with_retry(method, url, url_params, json_data, headers)


def isi_logout(onefs_host, headers):

    url = "https://{}:8080/session/1/session".format(onefs_host)
    response = requests.request("DELETE", url, headers=headers, verify=False)


@retry(wait_exponential_multiplier=retry_settings.wait_exponential_multiplier,
       wait_exponential_max=retry_settings.wait_exponential_max,
       retry_on_exception=retry_settings.retry_if_io_error)
def _request_with_retry(method, url, url_params="", json_data=None,
                        headers=None):
    # randomly pick a node
    cluster_node = utils.pick_node(g_config["isi_ip_list"])
    node_index = -1
    while True:
        resp = None
        try:
            # this assumes that url starts with '/'
            request_url = ("{0}://{1}:{2}{3}{4}"
                           .format(
                               ISI_PROTOCOL, cluster_node, ISI_PORT,
                               url, url_params))
            global g_headers
            if g_headers:
                if cluster_node not in g_headers.get("Origin"):
                    headers = prepare_headers(
                        cluster_node, g_config["onefs_user"], g_config["onefs_pass"], g_config['verify_ssl'])
                    resp = requests.request(
                        method, request_url, json=json_data, headers=headers, verify=g_config['verify_ssl'])
                    isi_logout(cluster_node, headers)
                else:
                    resp = requests.request(
                        method, request_url, json=json_data, headers=g_headers, verify=g_config['verify_ssl'])
            else:
                resp = requests.request(method,
                                        request_url, json=json_data, headers=headers,
                                        auth=(g_config['onefs_user'],
                                              g_config['onefs_pass']),
                                        verify=g_config['verify_ssl'],
                                        timeout=5.0)

            # if the request is unsuccessful then raise an exception
            resp.raise_for_status()
            # if no exception is raised then return the response
            return resp

        except (URLRequired, MissingSchema, InvalidSchema,
                InvalidURL) as bug:
            # these exceptions are caused by bugs in my code, so no need to try
            # the request on other nodes, just exit (note that these exceptions
            # do not trigger a retry in th retry_settings.retry_if_io_error
            # function and thus this exception will bubble up to caller).
            raise bug
        except RequestException as exception:
            # if this is not one of those code-bug type exceptions above then
            # try the request again on a different node.
            if resp is None:
                LOG.warning("Failed to %s %s. Exception: (%s)",
                            method, request_url, str(exception))
            elif resp.status_code != 404:  # no need to log 404
                LOG.warning("Failed to %s %s. %d - %s.",
                            method, request_url, resp.status_code, resp.reason)
            while True:
                node_index += 1
                if node_index >= len(g_config["isi_ip_list"]):
                    # if we tried all the nodes then raise the exception and
                    # allow the retry logic to retry in a bit.
                    raise exception
                next_node = g_config["isi_ip_list"][node_index]
                # if next_node is the same as cluster_node then skip it
                if next_node != cluster_node:
                    cluster_node = next_node
                    break


### JE routines ###
def create_changelist_job(old_snap_id, new_snap_id):
    """ Submit a job to create changelist and return job_id if successful """

    jobApi = g_isi_sdk.JobApi(g_isi_api_client)
    job = g_isi_sdk.JobJobCreateParams(type="ChangeListCreate", allow_dup=True, changelistcreate_params={
                                       "older_snapid": old_snap_id, "newer_snapid": new_snap_id})

    try:
        resp = jobApi.create_job_job(job)
        return resp.id
    except (urllib3.exceptions.HTTPError,
            g_isi_sdk.rest.ApiException) as exc:
        LOG.error("Failed to create changelist job for snapshots: %d "
                  "%d.\n(%s)", old_snap_id, new_snap_id, str(exc))

    return None


def check_job_status(job_id, timeout):

    jobApi = g_isi_sdk.JobApi(g_isi_api_client)

    start_time = time.time()

    while True:
        try:
            res = jobApi.get_job_job(job_id)
            status = res.jobs[0].state
            if status in ["succeeded", "failed"]:
                return status
            print("Changelist job status=" + str(status))
        except (urllib3.exceptions.HTTPError,
                g_isi_sdk.rest.ApiException) as exc:
            LOG.error("Error checking job status.\n(%s)", str(exc))
            return "error"

        if (time.time() - start_time) > timeout:
            LOG.error("Changelist job timed out after %d seconds. "
                      "The changelist is probably too big, run the initial "
                      "indexer instead.", timeout)
            return "timeout"

        time.sleep(60)

    return None


### Utility routines ###
def check_snapshot_license():
    """ Check if the cluster has valid license for snapshots """
    snapshotApi = g_isi_sdk.SnapshotApi(g_isi_api_client)
    try:
        res = snapshotApi.get_snapshot_license()
        if res.status == "Evaluation":
            curtime = time.time()
            expiration = res.expiration
            return res.days_to_expiry > 0
        else:
            return res.status == "Activated"
    except (urllib3.exceptions.HTTPError,
            g_isi_sdk.rest.ApiException) as exc:
        LOG.error("Failed to check snapshot license.\n(%s)", str(exc))

    return False


def _get_external_ips():
    """ Get list of all the external IP addresses of the cluster """
    # g_isi_sdk version of this function is broken
    papi_url = ('{0}://{1}:{2}/platform/1/cluster/external-ips'
                .format(ISI_PROTOCOL, g_config['onefs_host'], ISI_PORT))

    global g_headers
    if g_headers:
        res = requests.get(
            papi_url, verify=g_config['verify_ssl'], headers=g_headers)
    else:
        res = requests.get(papi_url, verify=g_config['verify_ssl'],
                           auth=(g_config['onefs_user'], g_config['onefs_pass']))

    res.raise_for_status()
    isi_ip_list = json.loads(res.content)
    return isi_ip_list


def get_cluster_name():
    # get the Cluster API
    cluster_api = g_isi_sdk.ClusterApi(g_isi_api_client)
    try:
        resp = cluster_api.get_cluster_identity()
        return resp.name
    except (urllib3.exceptions.HTTPError,
            g_isi_sdk.rest.ApiException) as exc:
        LOG.error("Failed to get cluster name.\n(%s)", str(exc))

    return None


def get_cluster_guid():
    cluster_api = g_isi_sdk.ClusterApi(g_isi_api_client)
    try:
        resp = cluster_api.get_cluster_config()
        return resp.guid
    except (urllib3.exceptions.HTTPError,
            g_isi_sdk.rest.ApiException) as exc:
        LOG.error("Failed to get cluster name.\n(%s)", str(exc))

    return None
