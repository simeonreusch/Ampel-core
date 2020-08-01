#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/log/AmpelLogger.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.09.2018
# Last Modified Date: 11.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, sys, traceback
from sys import _getframe
from os.path import basename
from typing import Dict, Optional, Union, Any, List, TYPE_CHECKING
from ampel.type import ChannelId, StockId
from ampel.log.LighterLogRecord import LighterLogRecord
from ampel.log.LogRecordFlag import LogRecordFlag
from ampel.log.handlers.LoggingHandlerProtocol import LoggingHandlerProtocol
from ampel.log.handlers.AmpelStreamHandler import AmpelStreamHandler
from ampel.log.handlers.DBLoggingHandler import DBLoggingHandler


ERROR = LogRecordFlag.ERROR
WARNING = LogRecordFlag.WARNING
SHOUT = LogRecordFlag.SHOUT
INFO = LogRecordFlag.INFO
VERBOSE = LogRecordFlag.VERBOSE
DEBUG = LogRecordFlag.DEBUG

if TYPE_CHECKING:
	from ampel.core.AmpelContext import AmpelContext

class AmpelLogger:

	loggers: Dict[Union[int, str], 'AmpelLogger'] = {}
	_counter: int = 0
	verbose: int = 0


	@classmethod
	def get_logger(cls, name: Optional[Union[int, str]] = None, force_refresh: bool = False, **kwargs) -> 'AmpelLogger':
		"""
		Creates or returns an instance of :obj:`AmpelLogger <ampel.log.AmpelLogger>`
		that is registered in static dict 'loggers' using the provided name as key.
		If a logger with the given name already exists, the existing logger instance is returned.
		If name is None, unique (int) name will be generated
		:param ``**kwargs``: passed to constructor

		Typical use:\n
		.. sourcecode:: python\n
			logger = AmpelLogger.get_logger()
		"""

		if not name:
			cls._counter += 1
			name = cls._counter

		if name not in AmpelLogger.loggers or force_refresh:
			AmpelLogger.loggers[name] = AmpelLogger(name=name, **kwargs)

		return AmpelLogger.loggers[name]


	@staticmethod
	def from_profile(context: 'AmpelContext', profile: str, run_id: Optional[int] = None, **kwargs) -> 'AmpelLogger':

		handlers = context.config.get(f'logging.{profile}', dict, raise_exc=True)
		logger = AmpelLogger.get_logger(console=False, **kwargs)

		if "db" in handlers:

			if run_id is None:
				raise ValueError("Parameter 'run_id' is required when log_profile requires db logging handler")

			logger.add_handler(
				DBLoggingHandler(context.db, run_id, **handlers['db'])
			)

		if "console" in handlers:
			logger.add_handler(
				AmpelStreamHandler(**handlers['console'])
			)

		return logger


	@staticmethod
	def get_console_level(context: 'AmpelContext', profile: str) -> Optional[int]:

		handlers = context.config.get(f'logging.{profile}', dict, raise_exc=True)

		if "console" in handlers:
			if 'level' in handlers['console']:
				return handlers['console']['level']
			return LogRecordFlag.INFO.__int__()

		return None


	@classmethod
	def has_verbose_console(cls, context: 'AmpelContext', profile: str) -> bool:

		if lvl := cls.get_console_level(context, profile):
			return lvl < INFO
		return False


	def __init__(self,
		name: Union[int, str] = 0,
		base_flag: Optional[LogRecordFlag] = None,
		handlers: Optional[List[LoggingHandlerProtocol]] = None,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		# See AmpelStreamHandler annotations for more details
		console: Optional[Union[bool, Dict[str, Any]]] = True
	) -> None:

		self.name = name
		self.base_flag = base_flag.__int__() if base_flag else 0
		self.handlers = handlers or []
		self.channel = channel
		self.level = 0
		self.fname = _getframe().f_code.co_filename

		if console:
			self.add_handler(
				AmpelStreamHandler() if console is True else AmpelStreamHandler(**console) # type: ignore
			)
		else:
			self.provenance = False

		self._auto_level()


	def _auto_level(self):

		self.level = min([h.level for h in self.handlers]) if self.handlers else 0
		if self.level < INFO:
			self.verbose = 2 if self.level < VERBOSE else 1
		else:
			if self.verbose != 0:
				self.verbose = 0


	def add_handler(self, handler: LoggingHandlerProtocol) -> None:

		if handler.level < self.level:
			self.level = handler.level

		if isinstance(handler, AmpelStreamHandler) and handler.provenance:
			self.provenance = True

		if self.level < INFO:
			self.verbose = 2 if self.level < VERBOSE else 1

		self.handlers.append(handler)


	def remove_handler(self, handler: LoggingHandlerProtocol) -> None:
		self.handlers.remove(handler)
		self._auto_level()


	def get_db_logging_handler(self) -> Optional[DBLoggingHandler]:
		for el in self.handlers:
			if isinstance(el, DBLoggingHandler):
				return el
		return None


	def error(self, msg: Union[str, Dict[str, Any]], *args,
		exc_info: Optional[Exception] = None,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):
		self.log(ERROR, msg, *args, exc_info=exc_info, channel=channel or self.channel, stock=stock, extra=extra)


	def warn(self, msg: Union[str, Dict[str, Any]], *args,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):
		if self.level <= WARNING:
			self.log(WARNING, msg, *args, channel=channel or self.channel, stock=stock, extra=extra)


	def info(self, msg: Optional[Union[str, Dict[str, Any]]], *args,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	) -> None:
		if self.level <= INFO:
			self.log(INFO, msg, *args, channel=channel or self.channel, stock=stock, extra=extra)


	def debug(self, msg: Optional[Union[str, Dict[str, Any]]], *args,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):
		if self.level <= DEBUG:
			self.log(DEBUG, msg, *args, channel=channel or self.channel, stock=stock, extra=extra)


	def handle(self, record: Union[LighterLogRecord, logging.LogRecord]) -> None:
		for h in self.handlers:
			if record.levelno >= h.level:
				h.handle(record)


	def flush(self) -> None:
		for h in self.handlers:
			h.flush()


	def log(self,
		lvl: int, msg: Optional[Union[str, Dict[str, Any]]], *args,
		exc_info: Optional[Union[bool, Exception]] = None,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):

		if args and isinstance(msg, str):
			msg = msg % args

		record = LighterLogRecord(name=self.name, levelno=lvl | self.base_flag, msg=msg)

		if lvl > WARNING or self.provenance:
			frame = _getframe(1) # logger.log(...) was called directly
			if frame.f_code.co_filename == self.fname:
				frame = _getframe(2) # logger.info(...), logger.debug(...) was used
			record.__dict__['filename'] = basename(frame.f_code.co_filename)
			record.__dict__['lineno'] = frame.f_lineno

		if extra:
			record.extra = extra

		if stock:
			record.stock = stock

		if channel or self.channel:
			record.channel = channel or self.channel

		if exc_info:

			if exc_info == 1:
				exc_info = sys.exc_info() # type: ignore
				lines = traceback.format_exception(*sys.exc_info())
			elif isinstance(exc_info, tuple):
				lines = traceback.format_exception(*sys.exc_info())
			elif isinstance(exc_info, Exception):
				lines = traceback.format_exception(
					etype=type(exc_info), value=exc_info, tb=exc_info.__traceback__
				)
			else:
				lines = []

			erec = AmpelLogger.fork_rec(record, "\n")
			for h in self.handlers:
				h.handle(erec)

			for el in lines:
				for l in el.split('\n'):
					if not l:
						continue
					erec = AmpelLogger.fork_rec(record, l)
					for h in self.handlers:
						h.handle(erec)

			if record.msg:
				rec2 = AmpelLogger.fork_rec(record, "-" * len(record.msg))
				for h in self.handlers:
					h.handle(record)
					h.handle(rec2)

			return

		for h in self.handlers:
			if record.levelno >= h.level:
				h.handle(record)


	@staticmethod
	def fork_rec(orig: LighterLogRecord, msg: str) -> LighterLogRecord:
		new_rec = LighterLogRecord(name=0, msg=None, levelno=0)
		for k, v in orig.__dict__.items():
			new_rec.__dict__[k] = v
		new_rec.msg = msg
		return new_rec