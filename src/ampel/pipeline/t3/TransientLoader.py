#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TransientLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 30.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.PhotoPoint import PhotoPoint
from ampel.base.UpperLimit import UpperLimit
from ampel.base.Compound import Compound
from ampel.base.LightCurve import LightCurve
from ampel.base.Transient import Transient
from ampel.base.ScienceRecord import ScienceRecord
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.db.LightCurveLoader import LightCurveLoader
from ampel.pipeline.db.DBResultOrganizer import DBResultOrganizer
from ampel.pipeline.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.pipeline.db.query.QueryLoadTransientInfo import QueryLoadTransientInfo
from ampel.pipeline.t3.TransientItems import TransientItems

import operator, logging, json
from datetime import datetime


class TransientLoader:
	"""
	"""
	all_doc_types = (
		AlDocTypes.PHOTOPOINT |
		AlDocTypes.UPPERLIMIT |
		AlDocTypes.COMPOUND |
		AlDocTypes.TRANSIENT |
		AlDocTypes.T2RECORD
	)


	def __init__(self, central_db, config_db=None, verbose=False, logger=None):
		"""
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.lcl = LightCurveLoader(central_db, logger=self.logger)
		self.config_db = config_db
		self.main_col = central_db["main"]
		self.photo_col = central_db["photo"]
		self.al_pps = {}
		self.lc = {}
		self.verbose = verbose


	def load_new(
		self, tran_id, content_types=all_doc_types, 
		state="latest", channels=None, t2_ids=None
	):
		"""
		Arguments:
		----------

		-> tran_id: transient id (string)

		-> content_types: AlDocTypes flag combination. 
		Possible values are:
		* 'AlDocTypes.TRANSIENT': 
			-> Add info from DB doc to the returned ampel.base.Transient instance
			-> For example: channels, flags (has processing errors), 
			   latest photopoint observation date, ...

		* 'AlDocTypes.PHOTOPOINT': 
			-> load *all* photopoints avail for this transient (regardless of provided state)
			-> The transient will contain a list of ampel.base.PhotoPoint instances 
			-> No policy is set for all PhotoPoint instances

		* 'AlDocTypes.UPPERLIMIT': 
			-> load *all* upper limits avail for this transient (regardless of provided state)
			-> The transient will contain a list of ampel.base.UpperLimit instances 
			-> No policy is set for all UpperLimit instances

		* 'AlDocTypes.COMPOUND': 
			-> ampel.base.LightCurve instances are created based on DB documents 
			   (with alDocType AlDocTypes.COMPOUND)
			-> if 'state' is 'latest' or a state id (md5 string) is provided, 
			   only one LightCurve instance is created. 
			   if 'state' is 'all', all available lightcurves are created.
			-> the lightcurve instance(s) will be associated with the 
			   returned ampel.base.Transient instance

		* 'AlDocTypes.T2RECORD': 
			...

		-> state:
		* "latest": latest state will be retrieved
		* "all": all states present in DB (at execution time) will be retrieved
		* <compound_id>: provided state will be loaded. 
		  The compound id must be a 32 alphanumerical string
	
		"""

		# Robustness check 1
		if content_types is None or content_types == 0:
			raise ValueError("Parameter content_types not conform")

		# Option 1: Find latest state, then update search query parameters
		if state == "latest":

			# Feedback
			self.logger.info(
				"Retrieving %s for latest state of transient %s" % 
				(content_types, tran_id)
			)

			# Execute DB query returning a dict represenation 
			# of the latest compound dict (alDocType: COMPOUND) 
			latest_compound_dict = next(
				self.main_col.aggregate(
					QueryLatestCompound.general_query(tran_id)
				),
				None
			)

			if latest_compound_dict is None:
				self.logger.info("Transient %s not found" % tran_id)
				return None

			self.logger.info(
				" -> Latest lightcurve id: %s " % 
				latest_compound_dict['_id']
			)

			# Build query parameters (will return adequate number of docs)
			search_params = QueryLoadTransientInfo.build_statebound_query(
				tran_id, 
				content_types, 
				compound_ids = latest_compound_dict["_id"], 
				t2_ids = t2_ids,
				comp_already_loaded = True
			)

		# Option 2: Load every available transient state
		elif state == "all":

			# Feedback
			self.logger.info(
				"Retrieving %s for all states of transient %s" % 
				(content_types, tran_id)
			)

			# Build query parameters (will return adequate number of docs)
			search_params = QueryLoadTransientInfo.build_stateless_query(
				tran_id, content_types, t2_ids=t2_ids
			)


		# Option 3: Load a user provided state(s)
		else:

			# Feedback
			self.logger.info(
				"Retrieving %s for state %s of transient %s" % 
				(content_types, state, tran_id)
			)

			# (Lousy/incomplete) check if md5 string was provided
			if len(state) != 32 and type(state) is not list:
				raise ValueError("Provided state must be 32 alphanumerical characters or a list")

			# Build query parameters (will return adequate number of docs)
			search_params = QueryLoadTransientInfo.build_statebound_query(
				tran_id, content_types, state, t2_ids = t2_ids
			)
		
		self.logger.debug(
			"Retrieving transient info using query: %s" % 
			search_params
		)

		# Execute DB query
		main_cursor = self.main_col.find(search_params)

		# Robustness: check empty
		res_count = main_cursor.count()
		if res_count == 0:
			self.logger.warn("No db document found associated with %s" % tran_id)
			return None

		# Effectively perform DB query (triggered by casting cursor to list)
		self.logger.info(" -> Fetching %i results from main col" % res_count)
		res_main_list = list(main_cursor)

		# Photo DB query 
		if AlDocTypes.PHOTOPOINT|AlDocTypes.UPPERLIMIT in content_types:
			photo_cursor = self.photo_col.find({'tranId': tran_id})
			self.logger.info(" -> Fetching %i photo measurements" % photo_cursor.count())
			res_photo_list = list(photo_cursor)
		else:
			if AlDocTypes.PHOTOPOINT in content_types:
				photo_cursor = self.photo_col.find({'tranId': tran_id, '_id': {'$gt': 0}})
				self.logger.info(" -> Fetching %i photo points" % photo_cursor.count())
				res_photo_list = list(photo_cursor)
			if AlDocTypes.UPPERLIMIT in content_types:
				photo_cursor = self.photo_col.find({'tranId': tran_id, '_id': {'$lt': 0}})
				self.logger.info(" -> Fetching %i upper limits" % photo_cursor.count())
				res_photo_list = list(photo_cursor)


		return self.load_tran_items(
			res_main_list, res_photo_list, channels, state=state
		)


	def load_tran_items(
		self, main_list, photo_list=None, channels=None, state="latest", 
		load_lightcurves=True, feedback=True, verbose_feedback=False
	):
		"""
		"""
		# Convert non array into array for convenience
		if channels is None:
			channels = [None]

		# Build set: we need intersections later
		channels = set(channels)

		# Stores loaded transient items. 
		# Key: tran_id, value: TransientItems instance
		register = {}

		# Loop through ampel docs
		tran_id = ""
		for doc in main_list:
			
			# main_list results are sorted by tranId
			if tran_id != doc['tranId']:

				# Instantiate ampel.base.Transient object
				tran_id = doc['tranId']
				tran_items = TransientItems(tran_id, state, self.logger)
				register[tran_id] = tran_items

			# Pick up transient document
			if doc["alDocType"] == AlDocTypes.TRANSIENT:

				# Load, translate alFlags from DB into a TransientFlags enum flag instance 
				# and associate it with the ampel.base.Transient object instance

				tran_items.set_flags(
					TransientFlags(
						FlagUtils.dbflag_to_enumflag(
							doc['alFlags'], TransientFlags
						)
					)
				)

				for channel in (channels & set(doc['channels'])):

					found_first = False
					last_entry = None

					# Journal entries are time ordered
					for entry in doc['journal']:

						if entry['tier'] != 0 or channel not in entry['chans']:
							continue

						# First entry is creation date 
						if not found_first:
							tran_items.set_created(
								datetime.utcfromtimestamp(entry['dt']), 
								channel
							)
						else:
							last_entry = entry

						if last_entry is not None:
							tran_items.set_modified(
								datetime.utcfromtimestamp(last_entry['dt']), 
								channel
							)

			# Pick compound dicts 
			if doc["alDocType"] == AlDocTypes.COMPOUND:

				tran_items.add_compound(
					Compound(doc, read_only=True), 
					channels if len(channels) == 1 
					else channels & set(doc['channels']) # intersection
				)

				if state == "latest":
					tran_items.set_latest_state(
						doc['_id'], 
						channels if len(channels) == 1 
						else channels & set(doc['channels']) # intersection
					)

			# Pick t2 records
			if doc["alDocType"] == AlDocTypes.T2RECORD:

				sr = ScienceRecord(doc, read_only=True)
				tran_items.add_science_record(
					sr,
					channels if len(channels) == 1 
					else channels & set(doc['channels']) # intersection
				)


		if photo_list is not None:

			loaded_tran_ids = register.keys()

			# Share common upper limits among different Transients
			loaded_uls = {}

			# Loop through photo results
			for doc in photo_list:
	
				# Pick photo point dicts
				if doc["_id"] > 0:
	
					# Photopoints instance attached to the transient instance 
					# are not bound to a compound and come thus without policy 
					register[doc['tranId']].add_photopoint(
						PhotoPoint(doc, read_only=True)
					)
	
				# Pick upper limit dicts
				else:
	
					if type(doc['tranId']) is int:
						register[doc['tranId']].add_upperlimit(
							UpperLimit(doc, read_only=True)
						)
					
					else: # list
						
						doc_id = doc["_id"]
						for tran_id in (loaded_tran_ids & doc['tranId']):
							if doc_id not in loaded_uls:
								loaded_uls[doc_id]	= UpperLimit(doc, read_only=True)
							register[tran_id].add_upperlimit(loaded_uls[doc_id])
					 

		if load_lightcurves and photo_list is not None:
		
			for tran_items in register.values():

				if len(tran_items.compounds) == 0:
					continue

				# This dict aims at avoiding unnecesssary re-instantiations 
				# of PhotoPoints objects referenced in several different LightCurves. 
				frozen_photo_dict = {**tran_items.photopoints, **tran_items.upperlimits}
				len_uls = len(tran_items.upperlimits)

				# Transform 
				# {
				#   'HU_LENS': [comp1, comp2, comp3],
				#   'HU_SN': [comp1, comp3]
				# }
				# into:
				# {
				#   'comp1': {'HU_LENS', 'HU_SN'},
				#   'comp2': {'HU_LENS'},
				#   'comp3': {'HU_LENS', 'HU_SN'},
				# }
				inv_map = {}
				for chan_name, comp_list in tran_items.compounds.items():
					for comp_obj in comp_list:
						if comp_obj in inv_map:
							inv_map[comp_obj].append(chan_name)
						else:
							inv_map[comp_obj] = [chan_name]

				for comp, chan_names in inv_map.items():

					if (len_uls == 0 and len([el for el in comp.content if 'ul' in el]) > 0):
						self.logger.info(
							" -> LightCurve loading aborded for %s (upper limits required)" % 
							comp.get_id()
						)
						continue

					lc = self.lcl.load_using_objects(comp, frozen_photo_dict)

					# Associate it to the ampel.base.Transient instance
					tran_items.add_lightcurve(lc, chan_names)

		# Feedback
		if feedback:
			self.log_feedback(register.values(), photo_list, channels, verbose_feedback)

		return register


	def log_feedback(self, dict_values, photo_list, channels, verbose_feedback):
		"""
		"""


		if channels is None:
			channels = [None]

		for tran_items in dict_values:

			len_comps = len({ell.id for el in tran_items.compounds.values() for ell in el})
			len_lcs = len({ell.id for el in tran_items.lightcurves.values() for ell in el})
			len_srs = len({hash(ell) for el in tran_items.science_records.values() for ell in el})

			if photo_list is not None:

				pps = tran_items.photopoints
				uls = tran_items.upperlimits

				self.logger.info(
					" -> %i loaded: PP: %i, UL: %i, CP: %i, LC: %i, SR: %i" % 
					(tran_items.tran_id, len(pps), len(uls), len_comps, len_lcs, len_srs)
				)

				if verbose_feedback: 

					if len(pps) > 0:
						self.logger.info(
							" -> Photopoint(s): {}".format(
								(*pps,) if len(pps) > 1 else next(iter(pps))
							)
						)

					if len(uls) > 0:
						self.logger.info(
							" -> Upper limit(s): {}".format(
								(*uls,) if len(uls) > 1 else next(iter(uls))
							)
						)
			else:

				self.logger.info(
					" -> %i loaded: PP: 0, UL: 0, CP: %i, LC: %i, SR: %i" % 
					(tran_items.tran_id, len_comps, len_lcs, len_srs)
				)

			if verbose_feedback:
				
				if len_comps > 0:
					for channel in tran_items.compounds.keys():
						self.logger.info(
							" -> %s Compound(s): %s " %
							(
								"" if channel is None else "[%s]" % channel,
								[el.id.hex() for el in tran_items.compounds[channel]]
							)
						)


				if len_lcs > 0:
					for channel in tran_items.lightcurves.keys():
						self.logger.info(
							" -> %s LightCurves(s): %s " %
							(
								"" if channel is None else "[%s]" % channel,
								[el.id.hex() for el in tran_items.lightcurves[channel]]
							)
						)


				if len_srs > 0:

					t2_ids = {ell.t2_unit_id for el in tran_items.science_records.values() for ell in el}
	
					for channel in tran_items.science_records.keys():
						for t2_id in t2_ids:
							srs = len(list(filter(
								lambda x: x.t2_unit_id == t2_id,
								tran_items.science_records[channel]
							)))
							if srs > 0:
								self.logger.info(
									" -> %s T2 %s: %s " %
									(
										"" if channel is None else "[%s]" % channel,
										t2_id, srs	
									)
								)
