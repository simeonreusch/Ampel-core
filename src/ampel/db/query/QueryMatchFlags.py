#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/db/query/QueryMatchFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.03.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.FlagUtils import FlagUtils
import enum


class QueryMatchFlags:
	"""
	DEPRECATED
	"""

	@staticmethod
	def add_match_criteria(flags, query, field_name):
		"""
		DEPRECATED

		Parameters:
		-----------
		'flags': must be one of ampel.base/core.flags.* enum flag instance 
		or a list of instances (of the same class).

		'query': dict (can be empty) that will be used as query criteria.
		This dict will be updated with the additional search criteria computed by this method. 
		ValueError will be thrown if 'query' already contains:
		  * the value of 'field_name' as key
		  * the key '$or' (only if 'flags' is a *list* of flags)
		
		Example: {} or {'tranId': 'ZTFabc'}

		'field_name': name (string) of the database field containing the flag values

		Returns:
		--------
		Dict instance referenced by query (which was updated)

		Examples: (see jupyter notebook 'FlagUtils enum flags to db query')
		---------
		General example. Let's consider the imaginary ampel.base.flags.DemoFlags.
		The parameter 'flags' could be:
		-> either an instance of ampel.base.flags.DemoFlags 
		  (can contain multiples flags that are 'AND' connected)
		  Search criteria would be that result documents must have all the channels 
		  defined in the channelFlag instance
		-> or list of instances of ampel.base.flags.DemoFlags whereby the embedded instances 
		  (the list elements) are connected with the 'OR' logical operator.

		Applied example: 
		-> demo_flags = DemoFlags.HU_EARLY_SN: 
		   finds transient associated with the channel HU_EARLY_SN

		-> demo_flags = DemoFlags.HU_EARLY_SN|DemoFlags.HU_RANDOM
		   finds transient that associated with *both* the channels HU_EARLY_SN and HU_RANDOM

		-> demo_flags = [DemoFlags.LENS, DemoFlags.HU_EARLY_SN|DemoFlags.HU_RANDOM]
		   finds transient that associated with 
		   * either with the LENS channel
		   * or with both the channels HU_EARLY_SN and HU_RANDOM
		"""
		if flags is None:
			raise ValueError('Illegal parameters')

		if isinstance(flags.__class__, enum.Flag.__class__):

			db_flag_array = FlagUtils.enumflag_to_dbtags(flags)

			query[field_name] = (
				db_flag_array[0] if len(db_flag_array) == 1
				else {'$all': db_flag_array}
			)

		elif type(flags) is list:

			or_list = []

			for flag in flags:

				if not isinstance(flag.__class__, enum.Flag.__class__):
					raise ValueError('Illegal type for list member contained in parameter "flags"')

				db_flag_array = FlagUtils.enumflag_to_dbtags(flag)
				or_list.append(
					{field_name: db_flag_array[0]} if len(db_flag_array) == 1
					else {field_name: {'$all': db_flag_array}}
				)

			if len(or_list) == 1:
				query[field_name] = or_list[0][field_name]
			else:
				query['$or'] = or_list

		return query


	@staticmethod
	def add_nomatch_criteria(flags, query, field_name=int):
		"""
		DEPRECATED

		ATTENTION: call this method *last* if you want to combine 
		the match criteria generated by this method with the one 
		computed in method add_match_criteria(...)

		Parameters: see docstring of add_match_criteria(...)
		Returns: dict instance referenced by parameter 'query' (which was updated)

		Examples: see jupyter notebook 'FlagUtils enum flags to db query'
		"""
		if flags is None:
			raise ValueError('Illegal parameters')

		# Check if field_name was set previously by add_match_criteria()
		# type of flag value is int
		if field_name in query and type(query[field_name]) is int:

			# If a previous scalar flag matching criteria was set
			# then we need rename it since query[field_name] will become a dict 
			query[field_name] = {'$eq': query[field_name]}

		if isinstance(flags.__class__, enum.Flag.__class__):

			db_flag_array = FlagUtils.enumflag_to_dbtags(flags)

			if field_name not in query:
				query[field_name] = {}
				
			if len(db_flag_array) == 1:
				query[field_name]['$ne'] = db_flag_array[0]
			else:
				query[field_name]['$not'] = {'$all': db_flag_array}

		elif type(flags) is list:

			and_list = []

			for flag in flags:

				if not isinstance(flag.__class__, enum.Flag.__class__):
					raise ValueError('Illegal type for list member contained in parameter "flags"')

				db_flag_array = FlagUtils.enumflag_to_dbtags(flag)

				and_list.append(
					{field_name: {'$ne': db_flag_array[0]}} if len(db_flag_array) == 1
					else {field_name: {'$not': {'$all': db_flag_array}}}
				)

			if len(and_list) == 1:
				query[field_name] = and_list[0][field_name]
			else:
				query['$and'] = and_list

		return query
