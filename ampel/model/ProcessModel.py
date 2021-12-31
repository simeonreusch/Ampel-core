#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ProcessModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.10.2019
# Last Modified Date: 30.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule as sched
from typing import Sequence, Optional, Literal, Union
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.types import ChannelId
from ampel.model.UnitModel import UnitModel
from ampel.config.ScheduleEvaluator import ScheduleEvaluator


class ProcessModel(AmpelBaseModel):

	name: str
	version: Union[int, float, str]
	active: bool = True
	tier: Optional[Literal[0, 1, 2, 3]]
	schedule: Sequence[str]
	channel: Optional[Union[ChannelId, Sequence[ChannelId]]]
	distrib: Optional[str]
	source: Optional[str]
	isolate: bool = True
	multiplier: int = 1
	log: Optional[str]
	controller: UnitModel = UnitModel(unit='DefaultProcessController')
	processor: UnitModel


	def __init__(self, **kwargs) -> None:

		if isinstance(kwargs.get('schedule'), str):
			kwargs['schedule'] = [kwargs['schedule']]

		super().__init__(**kwargs)

		evaluator = None
		for el in self.schedule:
			if el == "super":
				continue
			try:
				if evaluator is None:
					evaluator = ScheduleEvaluator()
				evaluator(sched.Scheduler(), el).do(lambda x: None)
			except Exception:
				raise ValueError("Incorrect 'schedule' parameter")
