#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3TranSelectConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import Union, List
from ampel.pipeline.config.time.TimeConstraintConfig import TimeConstraintConfig
from ampel.pipeline.config.GettableConfig import GettableConfig
from ampel.pipeline.common.docstringutils import gendocstring

@gendocstring
class TranSelectConfig(BaseModel, GettableConfig):
	""" """
	created: Union[None, TimeConstraintConfig] = None
	modified: Union[None, TimeConstraintConfig] = None
	channels: Union[None, str, List[str]] = None
	withFlags: Union[None, str, List[str]] = None
	withoutFlags: Union[None, str, List[str]] = None

	@validator('channels', 'withFlags', 'withoutFlags', pre=True, whole=True)
	def make_it_a_list(cls, v):
		if type(v) is not list:
			return [v]
		return v
