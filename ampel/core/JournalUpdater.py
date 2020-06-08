#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/JournalUpdater.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 08.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from bson import ObjectId
from pymongo.errors import BulkWriteError
from pymongo.operations import UpdateMany, UpdateOne
from typing import List, Any, Optional, Sequence, Union, Dict, Literal, get_args

from ampel.db.AmpelDB import AmpelDB
from ampel.log.LogUtils import LogUtils
from ampel.log.AmpelLogger import AmpelLogger
from ampel.content.JournalRecord import JournalRecord
from ampel.struct.JournalExtra import JournalExtra
from ampel.type import ChannelId, Tag, StockId

tag_type = get_args(Tag)
chan_type = get_args(ChannelId)

class JournalUpdater:


	def __init__(self,
		ampel_db: AmpelDB, tier: Literal[0, 1, 2, 3], run_id: int, process_name: str,
		logger: AmpelLogger, verbose: int = 0, raise_exc: bool = False,
		extra_tag: Optional[Union[Tag, Sequence[Tag]]] = None
	) -> None:
		"""
		:param raise_exc: raise exception rather than populating 'troubles' collection
		"""

		self._ampel_db = ampel_db
		self.col_stock = ampel_db.get_collection('stock')
		self.run_id = run_id
		self.tier = tier
		self.raise_exc = raise_exc
		self.process_name = process_name
		self.extra_tag = extra_tag
		self.logger = logger
		self.verbose = verbose
		self.reset()


	def reset(self) -> None:
		self.journal_updates: List[Any] = []
		self.journal_updates_count = 0


	def new_record(self,
		unit: Optional[Union[int, str]] = None,
		channels: Optional[Union[ChannelId, Sequence[ChannelId]]] = None,
		doc_id: Optional[ObjectId] = None,
		now: Optional[Union[int, float]] = None
	) -> JournalRecord:

		ret: JournalRecord = {
			'tier': self.tier,
			'ts': now if now else int(time()),
			'process': self.process_name,
			'run': self.run_id
		}

		if channels:
			ret['channel'] = channels

		if unit:
			ret['unit'] = unit

		if doc_id:
			ret['doc'] = doc_id.binary

		if self.extra_tag:
			ret['tag'] = self.extra_tag

		return ret


	def add_record(self,
		stock: Union[StockId, List[StockId]],
		journal_extra: Optional[JournalExtra] = None,
		doc_id: Optional[ObjectId] = None,
		unit: Optional[Union[int, str]] = None,
		channel: Optional[Union[ChannelId, Sequence[ChannelId]]] = None,
		now: Optional[Union[int, float]] = None
	) -> JournalRecord:
		"""
		:returns: the JournalRecord dict instance associated with the stock document(s) update
		is returned by this method so that customization can be made if necessary.
		Note that the associated update operation does more than just adding a journal record,
		it also modifies the "modified" field of stock document(s).
		"""

		if isinstance(stock, list):
			self.journal_updates_count += len(stock)
			match: Any = {'$in': stock}
			Op = UpdateMany
		else:
			self.journal_updates_count += 1
			match = stock
			Op = UpdateOne

		jrec = self.new_record(unit, channel, doc_id, now)

		if journal_extra:

			if journal_extra.tag:
				self.include_tags(jrec, journal_extra.tag)

			if journal_extra.extra:
				jrec['extra'] = journal_extra.extra

			if journal_extra.status and journal_extra.status > 0:
				jrec['status'] = journal_extra.status

		if channel:
			if isinstance(channel, chan_type):
				maxd = {f'modified.{channel}': jrec['ts']}
			else:
				maxd = {
					f'modified.{chan}': jrec['ts']
					for chan in channel # type: ignore[union-attr]
				}

		maxd['modified.any'] = jrec['ts']

		self.journal_updates.append(
			Op(
				{'_id': match},
				{'$push': {'journal': jrec}, '$max': maxd}
			)
		)

		return jrec


	def flush(self) -> None:

		if not self.journal_updates:
			return

		jupds = self.journal_updates
		self.reset()

		try:

			if self.verbose > 1:
				for el in jupds:
					self.logger.verbose(f"Journal update: {str(el)}")

			self.col_stock.bulk_write(jupds)

			if self.verbose:
				self.logger.verbose(f"{len(jupds)} journal entries inserted")

		except Exception as e:

			if self.raise_exc:
				if isinstance(e, BulkWriteError):
					raise ValueError(f"Journal update error: {e.details}")
				raise ValueError("Journal update error")

			info: Dict[str, Any] = {
				'process': self.process_name,
				'msg': 'Exception in flush()',
				'journalUpdateCount': self.journal_updates_count
			}

			if isinstance(e, BulkWriteError):
				info['BulkWriteError'] = str(e.details)

			# Populate troubles collection
			LogUtils.report_exception(
				self._ampel_db, self.logger, tier=self.tier,
				exc=e, run_id=self.run_id, info=info
			)



	@staticmethod
	def include_tags(jrec: JournalRecord, tag: Union[Tag, Sequence[Tag]]):
		""" Modifies the input JournalRecord dict """

		if tag:

			# journal record also contains tag(s)
			if 'tag' in jrec:

				# tweak request is about a single tag (not a sequence of tags)
				if isinstance(tag, tag_type):
					# journal record contains single tag (not a sequence of tags)
					if isinstance(jrec['tag'], tag_type):
						jrec['tag'] = [jrec['tag'], tag] # type: ignore[list-item]
					else:
						jrec['tag'].append(tag) # type: ignore[union-attr]

				# multi-tag tweak request
				else:
					# journal record contains single tag (not a sequence of tags)
					if isinstance(jrec['tag'], tag_type):
						jrec['tag'] = [*jrec['tag'], tag] # type: ignore[list-item, misc]
					else:
						jrec['tag'] = jrec['tag'] + tag # type: ignore[operator]

			# journal record contains no tag
			else:
				jrec['tag'] = tag
