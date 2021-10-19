#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsT3Stager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.01.2020
# Last Modified Date: 19.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional, Generator

from ampel.types import ChannelId
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.content.T3Document import T3Document
from ampel.core.ContextUnit import ContextUnit
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.log.AmpelLogger import AmpelLogger


class AbsT3Stager(AmpelABC, ContextUnit, abstract=True):
	"""
	Supply stock views to one or more T3 units.
	"""

	logger: AmpelLogger
	stock_updr: MongoStockUpdater

	channel: Optional[ChannelId] = None

	#: name of the associated process
	process_name: str

	#: raise exceptions instead of catching and logging
	raise_exc: bool = True

	#: contextual information for this run
	session_info: Optional[Dict[str, Any]] = None

	#: number of buffers to process at once. Set to 0 to disable chunking
	chunk_size: int = 1000

	#: Cast ampel buffers into views for each t3 unit (meaning possibly redundantly)
	#: since there is no real read-only struct in python
	paranoia: bool = True

	#: If true, value of T3Document.config will be the config dict rather than its hash
	resolve_config: bool = False

	#: If true, value of T3Document._id will be [process_name] [time_stamp]
	human_id: bool = False

	#: If true, a value will be set for T3Document.datetime
	human_timestamp: bool = False

	#: Used if human_timestamp is true
	human_timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f"


	@abstractmethod
	def stage(self, data: Generator[AmpelBuffer, None, None]) -> Optional[Generator[T3Document, None, None]]:
		""" Process a chunk of AmpelBuffer instances """
