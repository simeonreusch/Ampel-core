#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/update/StockT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.03.2020
# Last Modified Date: 11.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from pymongo import UpdateOne
from typing import Union, List, Tuple
from ampel.types import StockId, ChannelId
from ampel.enum.DocumentCode import DocumentCode
from ampel.abstract.ingest.AbsT2Ingester import AbsT2Ingester
from ampel.abstract.ingest.AbsStockT2Ingester import AbsStockT2Ingester
from ampel.abstract.compile.AbsStockT2Compiler import AbsStockT2Compiler
from ampel.ingest.StockT2Compiler import StockT2Compiler


class StockT2Ingester(AbsStockT2Ingester):

	# override
	compiler: AbsStockT2Compiler = StockT2Compiler()

	def ingest(self,
		stock_id: StockId,
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]]
	) -> None:

		optimized_t2s = self.compiler.compile(chan_selection)
		now = int(time())

		# Loop over t2 units to be created
		for (t2_unit_id, run_config), chans in optimized_t2s.items():

			jchan, chan_add_to_set = AbsT2Ingester.build_query_parts(chans)

			# Append update operation to bulk list
			self.updates_buffer.add_t2_update(
				UpdateOne(
					# Matching search criteria
					{
						'stock': stock_id,
						'unit': t2_unit_id,
						'config': run_config,
						'col': 'stock',
						'link': stock_id,
					},
					{
						# Attributes set if no previous doc exists
						'$setOnInsert': {
							'stock': stock_id,
							'tag': self.tags,
							'unit': t2_unit_id,
							'config': run_config,
							'code': DocumentCode.NEW.value,
						},
						# Journal and channel update
						'$addToSet': {
							'channel': chan_add_to_set,
							'journal': {
								'tier': self.tier,
								'ts': now,
								'channel': jchan
							}
						}
					},
					upsert=True
				)
			)
