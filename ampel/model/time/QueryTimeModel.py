#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/QueryTimeModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.12.2019
# Last Modified Date: 06.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Optional, Any
from ampel.base.AmpelBaseModel import AmpelBaseModel


class QueryTimeModel(AmpelBaseModel):
	"""
	Standardized parameter for the class QueryMatchStock
	"""
	before: Optional[Union[int, float]] = None
	after: Optional[Union[int, float]] = None

	def __bool__(self) -> bool:
		return self.before is not None or self.after is not None

	def dict(self, **kwargs) -> dict[str, Any]:
		"""
		Example:
		{
			'$gt': 1575000000.003819,
			'$lt': 1575966106.003819
		}
		"""
		d = super().dict()
		d['$lt'] = d.pop('before')
		d['$gt'] = d.pop('after')
		return {k: v for k, v in d.items() if v is not None}
