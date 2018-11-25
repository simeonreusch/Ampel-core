#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/query/QueryMatchTransients.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 25.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson.objectid import ObjectId
from ampel.core.flags.AlDocType import AlDocType
from ampel.pipeline.config.t3.LogicSchemaUtils import LogicSchemaUtils
from ampel.pipeline.t3.TimeConstraint import TimeConstraint
from ampel.pipeline.db.query.QueryMatchSchema import QueryMatchSchema


class QueryMatchTransients:
	"""
	"""

	@staticmethod
	def match_transients(
		channels=None, with_flags=None, without_flags=None, time_created=None, time_modified=None
	):
		"""
		Merely a shortcut method made of several function calls

		:type channels: str, dict
		:param channels: string (one channel only) or a dict schema \
		(see :obj:`QueryMatchSchema <ampel.pipeline.db.query.QueryMatchSchema>` \
		for syntax details). None (no criterium) means all channels are considered. 

		:type with_flags: str, int, dict
		:param with_flags: string/int (one flag only) or a dict schema \
		(see :obj:`QueryMatchSchema <ampel.pipeline.db.query.QueryMatchSchema>` \
		for syntax details). Important: dict schema must contain **db flags** \
		(integers representing enum members position within enum class), please see \
		:func:`FlagUtils.to_dbflags_schema <ampel.core.flags.FlagUtils.to_dbflags_schema>` \
		docstring for more info.

		:type without_flags: str, int, dict
		:param without_flags: similar to parameter with_flags, except it's without.

		:param TimeConstraint time_created: instance of ampel.pipeline.t3.TimeConstraint
		:param TimeConstraint time_modified: instance of ampel.pipeline.t3.TimeConstraint

		:rtype: dict
		:returns: query dict with matching criteria
		"""

		query = {}

		if channels is not None:
			QueryMatchSchema.apply_schema(
				query, 'channels', channels
			)

		if with_flags is not None:
			QueryMatchSchema.apply_schema(
				query, 'alFlags', with_flags
			)

		# Order matters, parse_dict(...) must be called *after* parse_excl_dict(...)
		if without_flags is not None:
			QueryMatchSchema.apply_excl_schema(
				query, 'alFlags', without_flags
			)

		if time_created or time_modified:
				
			chans = LogicSchemaUtils.reduce_to_set("Any" if channels is None else channels)

			for chan_name in chans:

				if time_created:
					query['created.' + chan_name] = \
						QueryMatchTransients._add_time_constraint(time_created)

				if time_modified:
					query['modified.' + chan_name] = \
						QueryMatchTransients._add_time_constraint(time_modified)

		return query


	@staticmethod
	def _add_time_constraint(tc):
		"""
		:param TimeConstrain tc: instance of ampel.pipeline.t3.TimeConstraint.py
		:returns: dict such as:
			{
				'$gt': 1223142,
				'$lt': 9894324923
			}
		"""

		if type(tc) is not TimeConstraint:
			raise ValueError("Parameter must be a TimeConstraint instance")

		d = {}
		if tc.has_constraint():
			for key, op in {'after': '$gte', 'before': '$lte'}.items():
				val = tc.get(key)
				if val:
					d[op] = val.timestamp()
		return d
