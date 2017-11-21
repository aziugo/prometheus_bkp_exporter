#!/usr/bin/env python
"""
Collect file date from backup folder and expose them to prometheus.io via http interface

Note: the http interface is not protected so you should block it from outside using a firewall

Usage: bkp_exporter.py [-p PORT] [-f CONF_FILE]
"""

# Core libs
import argparse
import yaml
import json
import requests
import sys
import time
import os
import Queue
import threading
import re

# Third-party libs
from prometheus_client import start_http_server, Metric, REGISTRY, Gauge
from prometheus_client.core import GaugeMetricFamily


# global consts
DEFAULT_PORT = 9110


# Custom Error
class ExporterError(RuntimeError):
    pass


class BkpCollector(object):
    """
    Main class, collecting data for prometheus client API
    """

    def __init__(self, conf):
        """
        Create the collector instance
        :param conf:    The configuration of backups
        :type conf:     dict
        """
        super(BkpCollector, self).__init__()
        self._paths = {}
        self._rewrites = {}
        self._ts_metrics = {}
        self._sz_metrics = {}
        self._parse_conf(conf)

    def collect(self):
        """
        check and look for data
        :yield:  GaugeMetricFamily
        """
        for path_name, (path, format) in self._paths.iteritems():
            content_group_index = next(i+1 for i in range(format.groups) if i+1 not in format.groupindex.values())
            for file in os.listdir(path):
                match = format.match(file)
                if not match:
                    continue
                value = match.group(content_group_index)
                filepath = os.path.join(path, file)
                tags = {"location":path_name}
                for var_name, group_pos in format.groupindex.iteritems():
                    tag_value = match.group(group_pos).strip()
                    if not tag_value:
                        continue
                    if var_name in self._rewrites and tag_value in self._rewrites[var_name].keys():
                        tag_value = self._rewrites[var_name][tag_value]
                    tags[var_name] = tag_value
                tags['file'] = value
                tags['filepath'] = filepath
                frozen_tags = frozenset(tags.items())
                if frozen_tags not in self._ts_metrics.keys():
                    self._ts_metrics[frozen_tags] = GaugeMetricFamily("backup_file_timestamp", "Backup file timestamp", labels=sorted([x[0] for x in frozen_tags]))
                    self._sz_metrics[frozen_tags] = GaugeMetricFamily("backup_file_size", "Backup file size", labels=sorted([x[0] for x in frozen_tags]))
                modif_time = os.path.getmtime(filepath)
                file_size = os.path.getsize(filepath)
                self._ts_metrics[frozen_tags].add_metric([x[1] for x in sorted(frozen_tags, key=lambda y: y[0])], modif_time)
                self._sz_metrics[frozen_tags].add_metric([x[1] for x in sorted(frozen_tags, key=lambda y: y[0])], file_size)
                yield self._ts_metrics[frozen_tags]
                yield self._sz_metrics[frozen_tags]

    def _parse_conf(self, conf):
        if 'locations' not in conf or not conf['locations']:
            raise ExporterError("no location defined")
        for loc_name, loc_conf in conf['locations'].iteritems():
            if 'path' not in loc_conf:
                raise ExporterError("no path defined for the '"+loc_name+"' section")
            if not os.path.exists(loc_conf['path']):
                raise ExporterError("folder '"+loc_conf['path']+"' doesn't exists")
            loc_format = re.compile("^"+loc_conf['format']+"$") if 'format' in loc_conf else re.compile("^(.*)$")
            self._paths[loc_name]= (loc_conf['path'], loc_format)
        if 'rewrite' in conf:
            for var_name, rewrite_conf in conf['rewrite'].iteritems():
                if not rewrite_conf:
                    continue
                self._rewrites[var_name] = rewrite_conf


def main(port, config_file=None):
    """
    Main function.
    Parse config, create Aws connections to read data and create a web-server for prometheus
    :param port:            The http port the server will listen for incoming http requests
    :type port:             int
    :param config_file:     The path of the config file, optional. If none, look for config in the script folder
    :type config_file:      str|None
    :return:                The exit code
    :rtype:                 int
    """
    try:
        if not port:
            port = DEFAULT_PORT

        if not config_file:
            config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.yml")

        if not os.path.exists(config_file):
            raise ExporterError("Unable to load config file '"+str(config_file)+"'")

        with open(config_file, "r") as cfg_fh:
            cfg_content = cfg_fh.read()

        collector = BkpCollector(yaml.load(cfg_content))
        REGISTRY.register(collector)
        start_http_server(port)
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print ("\nExiting, please wait...")
        return 0
    except SystemExit:
        raise
    except ExporterError as error:
        sys.stderr.write(error.message)
        sys.stderr.write("\n")
        sys.stderr.flush()
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-p", type=int, help="exporter port, default: "+str(DEFAULT_PORT))
    parser.add_argument("--config-file", "-f", help="configuration file")
    args = parser.parse_args()
    exit_code = 2
    try:
        exit_code = main(args.port, args.config_file)
    except KeyboardInterrupt: 
        exit_code = 0
        print "\nExiting, please wait..."
    except SystemExit:
        pass
    except ExporterError as e:
        sys.stderr.write(e.message)
        sys.stderr.write("\n")
        sys.stderr.flush()
        exit_code = 1
    sys.stdout.write("Bye bye\n")
    sys.stdout.flush()
    os._exit(exit_code)  # we call the brutal exit function because the web-server tends to never close


