#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/handlers/EnclosedChanRecordBufHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.05.2020
# Last Modified Date: 09.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Logger, Handler
from typing import Union, Optional, Dict
from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler
from ampel.log.handlers.LoggingHandlerProtocol import LoggingHandlerProtocol
from ampel.type import StockId, ChannelId


class EnclosedChanRecordBufHandler(RecordBufferingHandler):

	__slots__ = '_channel', '_empty_msg'


	def __init__(self, channel: ChannelId) -> None:
		super().__init__()
		self._channel = channel
		self._empty_msg = {'c': channel}


	def forward(self,
		target: Union[Logger, Handler, LoggingHandlerProtocol],
		stock: Optional[StockId],
		extra: Optional[Dict] = None,
		clear: bool = True
	) -> None:
		"""
		Forwards saved log records to provided logger/handler instance.
		Clears the internal record buffer.
		"""
		for rec in self.buffer:

			if rec.msg:
				rec.msg = {'t': rec.msg, 'c': self._channel}
			else:
				rec.msg = self._empty_msg

			rec.stock = stock # type: ignore

			if extra:
				if 'extra' in rec.__dict__:
					rec.extra = {**rec.extra, **extra} # type: ignore
				else:
					rec.extra = extra # type: ignore

			target.handle(rec) # type: ignore

		if clear:
			self.buffer = []
			self.has_error = False
