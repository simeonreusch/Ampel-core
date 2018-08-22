#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/LoggingUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 22.08.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, sys
from datetime import datetime


class LoggingUtils:
	"""
		Logging related static util method(s)
	"""

	@staticmethod
	def get_logger(unique=False, log_pathname=False):
		"""
		Returns a logger (registered as 'Ampel' in the module logging is unique=False)
		with the following parameters:
			* Log level DEBUG
			* Logging format:
				'%(asctime)s %(filename)s:%(lineno)s %(funcName)s() %(levelname)s %(message)s'
		"""

		logger = logging.getLogger(
			"Ampel-"+str(datetime.utcnow().time()) if unique is True else "Ampel"
		)
		logger.setLevel(logging.DEBUG)

		ch = logging.StreamHandler()
		ch.setLevel(logging.DEBUG)
		ch.setFormatter(
			LoggingUtils.get_formatter(log_pathname)
		)

		logger.addHandler(ch)
		logger.propagate = False

		return logger


	@staticmethod
	def get_formatter(log_pathname=False):
		"""
		"""
		return logging.Formatter(
			fmt=(
				'%(asctime)s %(pathname)s %(filename)s:%(lineno)s %(funcName)s() %(levelname)s %(message)s'
				if log_pathname is True
				else '%(asctime)s %(filename)s:%(lineno)s %(funcName)s() %(levelname)s %(message)s'
			), 
			datefmt="%Y-%m-%d %H:%M:%S"
		)


	@staticmethod
	def set_console_loglevel(logger, lvl):
		"""
		"""
		for handler in logger.handlers:
			if isinstance(handler, logging.StreamHandler):
				handler.setLevel(lvl)
				return


	@staticmethod
	def quiten_console_logger(logger):
		""" """
		LoggingUtils.set_console_loglevel(logger, logging.WARN)

		
	@staticmethod
	def louden_console_logger(logger):
		""" """
		LoggingUtils.set_console_loglevel(logger, logging.DEBUG)
