#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.01.2018
# Last Modified Date: 04.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.01.2018
# Last Modified Date: 04.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.01.2018
# Last Modified Date: 04.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.01.2018
# Last Modified Date: 04.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.01.2018
# Last Modified Date: 03.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.01.2018
# Last Modified Date: 03.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.01.2018
# Last Modified Date: 03.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.01.2018
# Last Modified Date: 03.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 03.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, importlib, time

from ampel.pipeline.t0.AmpelAlert import AmpelAlert
from ampel.pipeline.t0.AlertFileList import AlertFileList
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
from ampel.pipeline.t0.ingesters.ZIAlertIngester import ZIAlertIngester

from ampel.flags.AlertFlags import AlertFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.JobFlags import JobFlags
from ampel.flags.ChannelFlags import ChannelFlags

from ampel.pipeline.common.LoggingUtils import LoggingUtils
from ampel.pipeline.common.ChannelsConfig import ChannelsConfig
from ampel.pipeline.common.db.DBJobReporter import DBJobReporter




class AlertProcessor:
	""" 
		Class handling T0 pipeline operations.

		For each alert, following tasks are performed:
			* Load the alert
			* Filter alert based on the configured filter
			* Set policies
			* Ingest alert based on the configured ingester

		Ampel makes sure that each dictionary contains an alflags key 
	"""
	version = 0.14

	def __init__(
		self, instrument="ZTF", alert_format="IPAC", 
		load_channels=True, config_file=None, mock_db=False
	):
		"""
			Parameters:
				* filter : 
					ID of the filter to be used in the method run().
					For a list of avail filter IDs, see the docstring of set_filter_id()
				* mock_db: 
					If True, every database operation will be run by mongomock rather than pymongo 

		"""
		self.logger = LoggingUtils.get_ampel_console_logger()

		if mock_db:
			from mongomock import MongoClient
		else:
			from pymongo import MongoClient

		self.mongo_client = MongoClient()
		self.db_job_reporter = DBJobReporter(self.mongo_client, AlertProcessor.version)
		self.db_job_reporter.add_flags(JobFlags.T0)
		self.db_log_handler = LoggingUtils.add_db_log_handler(self.logger, self.db_job_reporter)

		if config_file is None:
			db = self.mongo_client.get_database("Ampel")
			col = db.get_collection("config")
			self.config = col.find({}).next()
		else:
			import json
			self.config = json.load(open(config_file))
			db = self.mongo_client.get_database("Ampel")
			db['config'].insert_one(self.config)

		self.channels_config = ChannelsConfig(self.config['channels'])

		if load_channels:
			self.load_channels()

		if instrument == "ZTF":

			if alert_format == "IPAC":

				# Reference to function loading IPAC generated avro alerts
				self.alert_loading_func = ZIAlertLoader.get_flat_pps_list_from_file

				# Set static AmpelAlert alert flags
				AmpelAlert.add_class_flags(
					AlertFlags.INST_ZTF | AlertFlags.ALERT_IPAC | AlertFlags.PP_IPAC
				)

				# Set static AmpelAlert dict keywords
				AmpelAlert.set_pp_dict_keywords(
					self.config['global']['photoPoints']['ZTFIPAC']['dictKeywords']
				)
	
				# Set ingester class
				self.ingester_class = ZIAlertIngester

			# more alert_formats may be defined later
			else:
				raise ValueError("No implementation exists for the alert issuing entity: " + alert_format)

		# more instruments may be defined later
		else:
			raise ValueError("No implementation exists for the instrument: "+instrument)


	def load_config_from_file(self, file_name):
		"""
		"""
		import json
		with open(file_name, "r") as data_file:
			self.config = json.load(data_file)


	def load_channels(self):
		"""
			Loads all T0 channel configs defined in the T0 config 
		"""
		channels_ids = self.config["channels"].keys()
		self.t0_channels = [None] * len(channels_ids)

		for i, channel_name in enumerate(channels_ids):
			self.t0_channels[i] = self.__create_channel(channel_name)

		if hasattr(self, 'ingester'):
			self.active_chanlist_change = True


	def load_channel(self, channel_name):
		"""
			Loads a channel config, that will be used in the method run().
			This method can be called multiple times with different channel names.
			Known channels IDs (as for Sept 2017) are:
			"NoFilter", "SN", "Random", "Neutrino" 
		"""
		if not hasattr(self, 't0_channels'):
			self.t0_channels = []
		else:
			for channel in self.t0_channels:
				if channel["name"] == channel_name:
					self.logger.info("Channel "+channel_name+" already loaded")
					return

		self.t0_channels.append(
			self.__create_channel(channel_name)
		)

		if hasattr(self, 'ingester'):
			self.active_chanlist_change = True


	def reset_channels(self):
		"""
		"""
		self.t0_channels = []
		if hasattr(self, 'ingester'):
			self.active_chanlist_change = True


	def __create_channel(self, channel_name):
		"""
			Creates T0 channel dictionary.
			It contains mainly flag values and a reference 
			to the method of an instanciated T0 filter class
		"""
		# Feedback
		self.logger.info("Setting up channel: " + channel_name)

		# Shortcut
		d_filter = self.channels_config.get_filter_config(channel_name)

		# New channel dict
		channel = {"name": channel_name}

		# Instanciate filter class associated with this channel
		self.logger.info("Loading filter: " + d_filter['className'])
		module = importlib.import_module("ampel.pipeline.t0.filters." + d_filter['className'])
		fobj = getattr(module, d_filter['className'])()
		fobj.set_filter_parameters(d_filter['parameters'])

		# Create the enum flags that will be associated with matching transients
		t2s = self.channels_config.get_channel_t2s_flag(channel_name)
		self.logger.info("On match flags: " + str(t2s))

		# Associate enum flag with the filter instance
		fobj.set_on_match_default_flags(t2s)

		# Reference to the "apply()" function of the T0 filter (used in run())
		channel['filter_func'] = fobj.apply

		# LogRecordFlag and TransienFlag associated with the current channel
		channel['log_flag'] = LogRecordFlags[self.channels_config.get_channel_flag_label(channel_name)]
		channel['flag'] = self.channels_config.get_channel_flag(channel_name)

		# Build these two log entries once and for all (outside the main loop in run())
		channel['log_accepted'] = " -> Channel '" + channel_name + "': alert passes filter criteria"
		channel['log_rejected'] = " -> Channel '" + channel_name + "': alert was rejected"

		return channel


	def set_ingester_instance(self, ingester_instance):
		"""
			Sets custom ingester instance to be used in the method run().
			If unspecified, a new instance of ZIAlertIngester() is used
			Known ingester (as for Sept 2017) are:
				* t0.ingesters.MemoryIngester
				* t0.ingesters.ZIAlertIngester
		"""
		self.ingester = ingester_instance


	def load_ingester(self, ingester_class):
		"""
			Loads and returns an ingester intance using the provided metoclass
		"""
		self.logger.info("Loading %s", ingester_class.__name__)
		return ingester_class(
			self.mongo_client, self.config, self.t0_channels
		)


	def get_iterable_paths(self, base_dir="/Users/hu/Documents/ZTF/Ampel/alerts/", extension="*.avro"):

		# Container class allowing to conveniently iterate over local avro files 
		aflist = AlertFileList()
		aflist.set_folder(base_dir)
		aflist.set_extension(extension)
		
		self.logger.info("Returning iterable for file paths in folder: %s", base_dir)
		return iter(aflist.get_files())


	def run(self, iterable):
		"""
			For each alert:
				* Load the alert
				* Filter alert and set policies for every configured channels (defined by load_config())
				* Ingest alert based on PipelineIngester (default) 
				or the ingester instance set by the method set_ingester(obj)
		"""

		self.logger.info("#######     Processing alerts     #######")

		# Save current time to later evaluate how low was the pipeline processing time
		start_time = int(time.time())


		# Check if a ingester instance was defined
		if not hasattr(self, 'ingester'):
			if not hasattr(self, 'ingester_class'):
				raise ValueError('Ingester instance and class are missing. Please provide either one.')
			else:
				self.ingester = self.load_ingester(self.ingester_class)
		else:
			if hasattr(self, 'active_chanlist_change'):
				self.ingester = self.load_ingester(self.ingester_class)
				del self.active_chanlist_change
				


		# Create new "job" document in the DB
		self.db_job_reporter.insert_new(self)

		# Set ingester jobId 	(will be inserted in the transient documents)
		self.ingester.set_jobId(
			self.db_job_reporter.getJobId()
		)

		# Array of JobFlags. Each element is set by each T0 channel 
		scheduled_t2_modules = [None] * len(self.t0_channels) 

		# python micro-optimization
		loginfo = self.logger.info
		logdebug = self.logger.debug
		dblh_set_tranId = self.db_log_handler.set_tranId
		dblh_set_temp_flags = self.db_log_handler.set_temp_flags
		dblh_unset_temp_flags = self.db_log_handler.unset_temp_flags
		dblh_unset_tranId = self.db_log_handler.unset_tranId
		alert_loading_func = self.alert_loading_func
		ingest = self.ingester.ingest

		# Iterate over alerts
		for element in iterable:

			try:
				logdebug("Processing: " + element)

				# Load avro file into python dict instance
				trans_id, pps_list = alert_loading_func(element)
				loginfo("Processing alert: " + str(trans_id))

				# AmpelAlert will create an immutable list of immutable pp dictionaries
				alert = AmpelAlert(trans_id, pps_list)

				# Associate upcoming log entries with the current transient id
				dblh_set_tranId(trans_id)

				# Loop through initialized channels
				for i, channel in enumerate(self.t0_channels):

					# Associate upcoming log entries with the current channel
					dblh_set_temp_flags(channel['log_flag'])

					# Apply filter (returns None in case of rejection or t2 modules ids in case of match)
					scheduled_t2_modules[i] = channel['filter_func'](alert)

					# Log feedback
					if scheduled_t2_modules[i] is not None:
						loginfo(channel['log_accepted'])
						# TODO push transient journal entry
					else:
						loginfo(channel['log_rejected'])

					# Unset channel id <-> log entries association
					dblh_unset_temp_flags(channel['log_flag'])

				if not any(scheduled_t2_modules):
					# TODO: implement AlertDisposer class ?
					self.logger.info("Disposing rejected candidates not implemented yet")
				else:
					# Ingest alert
					logdebug(" -> Ingesting alert")
					ingest(trans_id, pps_list, scheduled_t2_modules)

				# Unset log entries association with transient id
				dblh_unset_tranId()

			except:
				self.logger.exception("")
				self.logger.critical("Exception occured")

		duration = int(time.time()) - start_time

		self.db_job_reporter.set_duration(duration)
		loginfo("Pipeline processing completed (time required: " + str(duration) + "s)")

		self.db_log_handler.flush()
