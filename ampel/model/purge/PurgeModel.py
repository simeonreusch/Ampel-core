#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/purge/PurgeModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.06.2020
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.purge.PurgeContentModel import PurgeContentModel
from ampel.model.purge.PurgeLogsModel import PurgeLogsModel


class PurgeModel(AmpelBaseModel):
	content: PurgeContentModel
	logs: PurgeLogsModel
