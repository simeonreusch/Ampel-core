#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/complement/AbsT3DataAppender.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.01.2020
# Last Modified Date: 14.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Iterable
from ampel.log import AmpelLogger
from ampel.base import abstractmethod
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.core.AdminUnit import AdminUnit


class AbsT3DataAppender(AdminUnit, abstract=True):
	"""
	Inherits AdminUnit because implementing classes might need access to
	an AmpelConfig instance (foremost to the contained resource definitions)
	"""

	logger: AmpelLogger

	@abstractmethod
	def complement(self, it: Iterable[AmpelBuffer]) -> None:
		"""Add information to each :class:`~ampel.core.AmpelBuffer.AmpelBuffer` """
		...