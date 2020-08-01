#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T0AddModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 05.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import Field
from typing import List, Optional
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.T1CombineModel import T1CombineModel
from ampel.model.ingest.T2ComputeModel import T2ComputeModel

class T0AddModel(UnitModel):
	# Override 'unit' to enable alias
	unit: str = Field(..., alias='ingester')
	# config (datapoint ingester config [from UnitModel])
	t1_combine: Optional[List[T1CombineModel]]
	t2_compute: Optional[T2ComputeModel]