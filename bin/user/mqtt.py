# $Id: mqtt.py 1483 2016-04-25 06:53:19Z mwall $
# Copyright 2013 Matthew Wall
"""
Upload data to MQTT server

This service requires the python bindings for mqtt:

   pip install paho-mqtt

Minimal configuration:

[StdRestful]
    [[MQTT]]
        server_url = mqtt://username:password@localhost:1883/
        topic = weather
        unit_system = METRIC

Use of the inputs map to customer name, format, or units:

[StdRestful]
    [[MQTT]]
        ...
        unit_system = METRIC # default to metric
        [[[inputs]]]
            [[[[outTemp]]]]
                name = inside_temperature  # use a label other than outTemp
                format = %.2f              # two decimal places of precision
                units = degree_F           # convert outTemp to F, others in C
            [[[[windSpeed]]]]
                units = knot  # convert the wind speed to knots
"""

import Queue
import paho.mqtt.client as mqtt
import sys
import syslog
import time
import urlparse

import weewx
import weewx.restx
import weewx.units
from weeutil.weeutil import to_bool, accumulateLeaves

try:
    import cjson as json
    # XXX: maintain compatibility w/ json module
    setattr(json, 'dumps', json.encode)
    setattr(json, 'loads', json.decode)
except Exception, e:
    try:
        import simplejson as json
    except Exception, e:
        import json

VERSION = "0.15"

if weewx.__version__ < "3":
    raise weewx.UnsupportedFeature("weewx 3 is required, found %s" %
                                   weewx.__version__)

def logmsg(level, msg):
    syslog.syslog(level, 'restx: MQTT: %s' % msg)

def logdbg(msg):
    logmsg(syslog.LOG_DEBUG, msg)

def loginf(msg):
    logmsg(syslog.LOG_INFO, msg)

def logerr(msg):
    logmsg(syslog.LOG_ERR, msg)

def _compat(d, old_label, new_label):
    if d.has_key(old_label) and not d.has_key(new_label):
        d.setdefault(new_label, d[old_label])
        d.pop(old_label)

# some unit labels are rather lengthy.  this reduces them to something shorter.
UNIT_REDUCTIONS = {
    'degree_F': 'F',
    'degree_C': 'C',
    'inch': 'in',
    'mile_per_hour': 'mph',
    'mile_per_hour2': 'mph',
    'km_per_hour': 'kph',
    'km_per_hour2': 'kph',
    'meter_per_second': 'mps',
    'meter_per_second2': 'mps',
    'degree_compass': None,
    'watt_per_meter_squared': 'Wpm2',
    'uv_index': None,
    'percent': None,
    'unix_epoch': None,
    }

# return the units label for an observation
def _get_units_label(obs, unit_system):
    (unit_type, _) = weewx.units.getStandardUnitType(unit_system, obs)
    return UNIT_REDUCTIONS.get(unit_type, unit_type)

# get the template for an observation based on the observation key
def _get_template(obs_key, overrides, append_units_label, unit_system):
    tmpl_dict = dict()
    if append_units_label:
        label = _get_units_label(obs_key, unit_system)
        if label is not None:
            tmpl_dict['name'] = "%s_%s" % (obs_key, label)
    for x in ['name', 'format', 'units']:
        if x in overrides:
            tmpl_dict[x] = overrides[x]
    return tmpl_dict


class MQTT(weewx.restx.StdRESTbase):
    def __init__(self, engine, config_dict):
        """This service recognizes standard restful options plus the following:

        Required parameters:

        server_url: URL of the broker, e.g., something of the form
          mqtt://username:password@localhost:1883/
        Default is None

        Optional parameters:

        unit_system: one of US, METRIC, or METRICWX
        Default is None; units will be those of data in the database

        topic: the MQTT topic under which to post
        Default is 'weather'

        append_units_label: should units label be appended to name
        Default is True

        obs_to_upload: Which observations to upload.  Possible values are
        none or all.  When none is specified, only items in the inputs list
        will be uploaded.  When all is specified, all observations will be
        uploaded, subject to overrides in the inputs list.
        Default is all

        inputs: dictionary of weewx observation names with optional upload
        name, format, and units
        Default is None
        """
        super(MQTT, self).__init__(engine, config_dict)        
        loginf("service version is %s" % VERSION)
        try:
            site_dict = config_dict['StdRESTful']['MQTT']
            site_dict = accumulateLeaves(site_dict, max_level=1)
            site_dict['server_url']
        except KeyError, e:
            logerr("Data will not be uploaded: Missing option %s" % e)
            return

        # for backward compatibility: 'units' is now 'unit_system'
        _compat(site_dict, 'units', 'unit_system')

        site_dict.setdefault('topic', 'weather')
        site_dict.setdefault('append_units_label', True)
        site_dict.setdefault('augment_record', True)
        site_dict.setdefault('obs_to_upload', 'all')
        site_dict.setdefault('retain', False)
        site_dict.setdefault('aggregation', 'individual,aggregate')

        usn = site_dict.get('unit_system', None)
        if usn is not None:
            site_dict['unit_system'] = weewx.units.unit_constants[usn]

        if config_dict['StdRESTful']['MQTT'].has_key('inputs'):
            site_dict['inputs'] = dict(config_dict['StdRESTful']['MQTT']['inputs'])

        site_dict['append_units_label'] = to_bool(site_dict.get('append_units_label'))
        site_dict['augment_record'] = to_bool(site_dict.get('augment_record'))
        site_dict['retain'] = to_bool(site_dict.get('retain'))
        binding = site_dict.pop('binding', 'archive')

        # if we are supposed to augment the record with data from weather
        # tables, then get the manager dict to do it.  there may be no weather
        # tables, so be prepared to fail.
        try:
            if site_dict.get('augment_record'):
                _manager_dict = weewx.manager.get_manager_dict_from_config(
                    config_dict, 'wx_binding')
                site_dict['manager_dict'] = _manager_dict
        except weewx.UnknownBinding:
            pass

        self.archive_queue = Queue.Queue()
        self.archive_thread = MQTTThread(self.archive_queue, **site_dict)
        self.archive_thread.start()

        if binding == 'archive':
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        else:
            self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)

        if 'topic' in site_dict:
            loginf("topic is %s" % site_dict['topic'])
        if usn is not None:
            loginf("desired unit system is %s" % usn)
        loginf("Data will be uploaded to %s" % site_dict['server_url'])

    def new_archive_record(self, event):
        self.archive_queue.put(event.record)

    def new_loop_packet(self, event):
        self.archive_queue.put(event.packet)

class MQTTThread(weewx.restx.RESTThread):
    def __init__(self, queue, server_url,
                 topic='', unit_system=None, skip_upload=False,
                 augment_record=True, retain=False, aggregation='individual',
                 inputs={}, obs_to_upload='all', append_units_label=True,
                 manager_dict=None,
                 post_interval=None, max_backlog=sys.maxint, stale=None,
                 log_success=True, log_failure=True,
                 timeout=60, max_tries=3, retry_wait=5):
        super(MQTTThread, self).__init__(queue,
                                         protocol_name='MQTT',
                                         manager_dict=manager_dict,
                                         post_interval=post_interval,
                                         max_backlog=max_backlog,
                                         stale=stale,
                                         log_success=log_success,
                                         log_failure=log_failure,
                                         max_tries=max_tries,
                                         timeout=timeout,
                                         retry_wait=retry_wait)
        self.server_url = server_url
        self.topic = topic
        self.upload_all = True if obs_to_upload.lower() == 'all' else False
        self.append_units_label = append_units_label
        self.inputs = inputs
        self.unit_system = unit_system
        self.augment_record = augment_record
        self.retain = retain
        self.aggregation = aggregation
        self.templates = dict()
        self.skip_upload = skip_upload

    def filter_data(self, record):
        # if uploading everything, we must check the upload variables list
        # every time since variables may come and go in a record.  use the
        # inputs to override any generic template generation.
        if self.upload_all:
            for f in record:
                if f not in self.templates:
                    self.templates[f] = _get_template(f,
                                                      self.inputs.get(f, {}),
                                                      self.append_units_label,
                                                      record['usUnits'])

        # otherwise, create the list of upload variables once, based on the
        # user-specified list of inputs.
        elif not self.templates:
            for f in self.inputs:
                self.templates[f] = _get_template(f, self.inputs[f],
                                                  self.append_units_label,
                                                  record['usUnits'])

        # loop through the templates, populating them with data from the record
        data = dict()
        for k in self.templates:
            try:
                v = float(record.get(k))
                name = self.templates[k].get('name', k)
                fmt = self.templates[k].get('format', '%s')
                to_units = self.templates[k].get('units')
                if to_units is not None:
                    (from_unit, from_group) = weewx.units.getStandardUnitType(
                        record['usUnits'], k)
                    from_t = (v, from_unit, from_group)
                    v = weewx.units.convert(from_t, to_units)[0]
                s = fmt % v
                data[name] = s
            except (TypeError, ValueError):
                pass
        # FIXME: generalize this
        if 'latitude' in data and 'longitude' in data:
            parts = [str(data['latitude']), str(data['longitude'])]
            if 'altitude_meter' in data:
                parts.append(str(data['altitude_meter']))
            elif 'altitude_foot' in data:
                parts.append(str(data['altitude_foot']))
            data['position'] = ','.join(parts)
        return data

    def process_record(self, record, dbm):
        import socket
        if self.augment_record and dbm is not None:
            record = self.get_record(record, dbm)
        if self.unit_system is not None:
            record = weewx.units.to_std_system(record, self.unit_system)
        data = self.filter_data(record)
        if weewx.debug >= 2:
            logdbg("data: %s" % data)
        if self.skip_upload:
            loginf("skipping upload")
            return
        url = urlparse.urlparse(self.server_url)
        for _count in range(self.max_tries):
            try:
                mc = mqtt.Client()
                if url.username is not None and url.password is not None:
                    mc.username_pw_set(url.username, url.password)
                mc.connect(url.hostname, url.port)
                mc.loop_start()
                if self.aggregation.find('aggregate') >= 0:
                    tpc = self.topic + '/loop'
                    (res, mid) = mc.publish(tpc, json.dumps(data),
                                            retain=self.retain)
                    if res != mqtt.MQTT_ERR_SUCCESS:
                        logerr("publish failed for %s: %s" % (tpc, res))
                if self.aggregation.find('individual') >= 0:
                    for key in data:
                        tpc = self.topic + '/' + key
                        (res, mid) = mc.publish(tpc, data[key],
                                                retain=self.retain)
                        if res != mqtt.MQTT_ERR_SUCCESS:
                            logerr("publish failed for %s: %s" % (tpc, res))
                mc.loop_stop()
                mc.disconnect()
                return
            except (socket.error, socket.timeout, socket.herror), e:
                logdbg("Failed upload attempt %d: %s" % (_count+1, e))
            time.sleep(self.retry_wait)
        else:
            raise weewx.restx.FailedPost("Failed upload after %d tries" %
                                         (self.max_tries,))
