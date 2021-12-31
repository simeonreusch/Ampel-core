#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ChannelModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Optional, Any, Sequence
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.purge.PurgeModel import PurgeModel
#from ampel.model.ViewModel import ViewModel


class ChannelModel(AmpelBaseModel):

	channel: Union[int, str]
	version: Union[int, float, str]
	purge: PurgeModel = {
		'content': {'delay': 100, 'format': 'json', 'unify': True},
		'logs': {'delay': 50, 'format': 'csv'}
	} # type: ignore[assignment]
	# view: str = "MongoChannelView"
	active: bool = True
	hash: Optional[int]
	distrib: Optional[str]
	source: Optional[str]
	contact: Optional[str]
	access: Sequence[str] = []
	#: Identities allowed to access data associated with this channel
	members: Optional[Sequence[str]]
	policy: Sequence[str] = []


	def dict(self, **kwargs) -> dict[str, Any]:
		if 'exclude_defaults' not in kwargs:
			kwargs['exclude_defaults'] = False
		return super().dict(**kwargs)
