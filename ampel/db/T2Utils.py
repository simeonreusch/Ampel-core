#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/db/T2Utils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2021
# Last Modified Date: 23.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime
from pymongo.collection import Collection
from typing import Optional, Any, Iterable, Union, Dict, Literal, Sequence, get_args

from ampel.type import UnitId, Tag, StrictIterable, ChannelId, StockId
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.content.T2Document import T2Document
from ampel.content.JournalRecord import JournalRecord
from ampel.enum.T2SysRunState import T2SysRunState
from ampel.db.query.general import build_general_query
from ampel.util.collections import check_seq_inner_type


class T2Utils:


	def __init__(self, logger: AmpelLogger):
		self._confirm = False
		self.logger = logger


	def i_know_what_i_am_doing(self) -> "T2Utils":
		self._confirm = True
		return self


	def reset_t2s(self, col: Collection, run_id: int, soft: bool = False, cli: bool = False, **kwargs) -> int:
		"""
		:param soft: if True, 'body' will not be deleted
		"""

		if not self._confirm:
			self.logger.warn("Danger zone: please confirm you know what you are doing")
			return 0

		jrec = JournalRecord(tier=-1, run=run_id, ts=datetime.utcnow().timestamp())

		if cli:
			jrec['extra'] = {'cli': True}

		update: Dict[str, Any] = {
			"$set": {"status": T2SysRunState.NEW},
			"$push": {"journal": jrec}
		}

		if not soft:
			update["$unset"] = {"body": 1}

		return col \
			.update_many(self.match_t2s(**kwargs), update) \
			.modified_count


	def get_t2s(self, col: Collection, **kwargs) -> Iterable[T2Document]:
		return col.find(
			self.match_t2s(**kwargs)
		)


	def match_t2s(self,
		unit: Optional[Union[UnitId, StrictIterable[UnitId]]] = None,
		config: Optional[Union[str, int]] = None,
		status: Optional[Union[int, Sequence[int]]] = None,
		link: Optional[Union[str, Sequence[str]]] = None,
		stock: Optional[Union[StockId, StrictIterable[StockId]]] = None,
		channel: Optional[Union[ChannelId, Dict, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]]] = None,
		tag: Optional[Dict[Literal['with', 'without'], Union[Tag, Dict, AllOf[Tag], AnyOf[Tag], OneOf[Tag]]]] = None,
		custom: Optional[Dict[str, Any]] = None,
		id_mapper: Optional[AbsIdMapper] = None,
		**kwargs
	) -> Dict[str, Any]:
		"""
		:param config: use string "null" to match null
		:param link: hex encoded bytes
		:param custom: custom match criteria, for example: {"body.result.sncosmo_info.chisqdof": {'$lte': 4}}
		"""

		if id_mapper and (isinstance(stock, str) or check_seq_inner_type(stock, str)):
			stock = id_mapper.to_ampel_id(stock) # type: ignore

		match = build_general_query(stock=stock, channel=channel, tag=tag)

		if unit:
			match['unit'] = unit if isinstance(unit, get_args(UnitId)) else {'$in': unit}

		if config:
			if isinstance(config, str) and config == "null":
				match['config'] = None
			else:
				if isinstance(config, int):
					match['config'] = config
				elif isinstance(config, (list, tuple)):
					match['config'] = {'$in': [el for el in config if config != "null"]}
					if "null" in config:
						match['config']['$in'].append(None)

		if link:
			match['link'] = bytes.fromhex(link) if isinstance(link, str) else {'$in': [bytes.fromhex(el) for el in link]}

		if custom:
			match.update(custom)

		if status is not None:
			match['status'] = status

		if kwargs.get('debug'):
			self.logger.debug("Using following matching criteria: %s" % match)

		return match