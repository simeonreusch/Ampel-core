#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBUpdateException.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.common.AmpelUtils import AmpelUtils

class DBUpdateException:


	@staticmethod
	def report(handler, e, bwe_details=None):
		"""
		"""
		# Print log stack using std logging 
		logger = AmpelLogger.get_unique_logger()

		AmpelUtils.log_exception(logger, e, msg="Primary exception:")

		if bwe_details:
			logger.error("BulkWriteError details:")
			logger.error(bwe_details)
			logger.error("#"*52)

		logger.error("DB log flushing error, un-flushed (json) logs below.")
		logger.error("*"*52)

		for d in handler.log_dicts:
			logger.error(str(d))
		logger.error("#"*52)

		try: 
			# This will fail as well if we have DB connectivity issues
			AmpelUtils.report_exception(
				0, dblh=handler,
				info = None if bwe_details is None else {'BulkWriteError': str(bwe_details)}
			)
		except Exception as ee:
			AmpelUtils.log_exception(
				logger, ee, last=True,
				msg="Could not update troubles collection as well (DB offline?)"
			)

		# TODO: try slack ? (will fail if network issue)