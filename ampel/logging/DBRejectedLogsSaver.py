#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/DBRejectedLogsSaver.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 04.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from logging import DEBUG, WARNING, Handler, LogRecord
from typing import Dict, Any, List, Union, Optional
from pymongo.errors import BulkWriteError
from pymongo.operations import UpdateOne
from ampel.db.AmpelDB import AmpelDB
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.logging.AmpelLoggingError import AmpelLoggingError
from ampel.logging.LoggingErrorReporter import LoggingErrorReporter


class DBRejectedLogsSaver(Handler):
	"""
	Class responsible for saving rejected log events (by T0 filters)
	into the NoSQL database. This class does not inherit logging.Handler
	but implements the method handle() so that this class can be used together
	with RecordsBufferingHandler.forward() or copy()
	"""

	def __init__(
		self, ampel_db: AmpelDB, channel: str, logger: AmpelLogger,
		single_rej_col: bool = False, aggregate_interval: int = 1, flush_len: int = 1000
	):
		"""
		:param AmpelLogger logger:
		:type channel: str, None
		:param str channel: channel name
		:param bool single_rej_col:
			- False: rejected logs are saved in channel specific collections
			(collection name equals channel name)
			- True: rejected logs are saved in a single collection called 'logs'
		:param int aggregate_interval: logs with similar attributes (log level,
		possibly stock id & channels) are aggregated in one document instead of being split
		into several documents (spares some index RAM). *aggregate_interval* is the max interval
		of time in seconds during which log aggregation takes place. Beyond this value,
		attempting a database bulk_write operation.
		:raises: None
		"""

		# required when not using super().__init__
		self.filters = []
		self.lock = None
		self._name = None
		self.level = DEBUG
		self.flush_len = flush_len
		self.aggregate_interval = aggregate_interval
		self.logger = logger
		self.log_dicts: List[Dict[str, Any]] = []
		self.prev_records: Optional[LogRecord] = None
		self.run_id: Optional[Union[int, List[int]]] = None
		self.channel = channel
		self.single_rej_col = single_rej_col
		col_name = "rejected" if single_rej_col else channel
		ampel_db.enable_rejected_collections([col_name])
		self.col = ampel_db.get_collection(col_name)


	def set_run_id(self, run_id: Union[int, List[int]]) -> None:
		self.run_id = run_id


	def get_run_id(self) -> Optional[Union[int, List[int]]]:
		return self.run_id


	def emit(self, record: LogRecord) -> None:

		try:

			# extra (alert id, stock id) is set by AlertProcessor
			extra = getattr(record, 'extra')

			# Same flag, date (+- 1 sec), tran_id and chans
			if (
				self.prev_records and
				record.created - self.prev_records.created < self.aggregate_interval and
				extra == getattr(self.prev_records, 'extra', None)
			):

				prev_dict = self.log_dicts[-1]
				if type(prev_dict.get('msg', None)) is not list:
					prev_dict['msg'] = [prev_dict.get('msg', None), record.msg]
				else:
					prev_dict['msg'].append(record.msg)

			else:

				if len(self.log_dicts) > self.flush_len:
					self.flush()

				# If duplication exists between keys in extra and in standard rec,
				# the corresponding extra items will be overwritten (and thus ignored)
				d = extra.copy()

				d['_id'] = extra['alert']
				d['dt'] = int(time())

				if record.levelno > WARNING:
					d['run'] = self.run_id

				if record.msg:
					d['msg'] = record.msg

				if self.single_rej_col:
					d['channel'] = self.channel

				try:
					del d['alert']
				except Exception:
					pass

				self.log_dicts.append(d)
				self.prev_records = record

		except Exception as e:
			LoggingErrorReporter.report(self, e)
			raise AmpelLoggingError from None


	def flush(self) -> None:
		""" Will raise Exception if DB issue occurs """

		# No log entries
		if not self.log_dicts:
			return

		try:

			# Empty referenced logs entries
			dicts = self.log_dicts
			self.log_dicts = []
			self.prev_records = None

			self.col.insert_many(dicts, ordered=False)

		except BulkWriteError as bwe:

			upserts = []

			# Recovery procedure for 'already existing logs'
			# In production, we should process alerts only once (per channel(s))
			# but during testing, reprocessing may occur.
			# In this case, we overwrite previous rejected logs
			for err_dict in bwe.details.get('writeErrors', []):

				# 'code': 11000, 'errmsg': 'E11000 duplicate key error collection: ...
				if err_dict.get("code") == 11000:
					lid = {'_id': err_dict['op'].pop('_id')}
					del err_dict['op']['stock']
					upserts.append(
						UpdateOne(lid, {'$set': err_dict['op']})
					)

			if len(upserts) != len(bwe.details.get('writeErrors', [])):
				LoggingErrorReporter.report(self, bwe, bwe.details)
				raise AmpelLoggingError from None

			self.logger.warn("Overwriting rejected alerts logs")

			try:
				# Try again, with updates this time
				self.col.bulk_write(upserts, ordered=False)
				return

			except BulkWriteError as bwee:
				LoggingErrorReporter.report(self, bwe, bwe.details)
				LoggingErrorReporter.report(self, bwee, bwee.details)

			raise AmpelLoggingError from None

		except Exception as e:

			LoggingErrorReporter.report(self, e)
			# If we can no longer keep track of what Ampel is doing,
			# better raise Exception to stop processing
			raise AmpelLoggingError from None
