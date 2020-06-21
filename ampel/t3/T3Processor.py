#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3Processor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 21.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Sequence, List

from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.log import AmpelLogger, LogRecordFlag, DBEventDoc, SHOUT
from ampel.log.utils import report_exception
from ampel.model.t3.T3Directive import T3Directive
from ampel.t3.load.AbsT3Loader import AbsT3Loader
from ampel.t3.run.AbsT3UnitRunner import AbsT3UnitRunner
from ampel.t3.select.AbsT3Selector import AbsT3Selector
from ampel.t3.complement.AbsT3DataAppender import AbsT3DataAppender
from ampel.t3.context.AbsT3RunContextAppender import AbsT3RunContextAppender


class T3Processor(AbsProcessorUnit):
	"""
	:param log_profile: See AbsProcessorUnit docstring
	:param db_handler_kwargs: See AbsProcessorUnit docstring
	:param base_log_flag: See AbsProcessorUnit docstring
	:param raise_exc: See AbsProcessorUnit docstring (default False)

	:param update_journal: Record the invocation of this event in the transient journal
	:param update_events: Record this event in the events collection

	Note: if you want to run an "anonymous" event, please:
	- use a log_profile without db logging (profiles are defined under 'logging' in the ampel config)
	- set update_journal = False (no update to the transient doc)
	- set update_events = False (no update to the events collection)
	- set raise_exc = True (troubles collection will not be populated if an exception occurs)
	"""

	directives: Sequence[T3Directive]
	chunk_size: int = 200


	def __init__(self, update_journal: bool = True, update_events: bool = True, **kwargs) -> None:
		"""
		Note that update_journal and update_event are admin run options and
		should be set only on command line. They are thus not defined as part of the underlying model.
		"""

		super().__init__(**kwargs)

		self.update_journal = update_journal
		self.update_events = update_events

		if 'db' not in self.context.config.get(f"logging.{self.log_profile}", dict, raise_exc=True):
			for el in ("update_journal", "update_events"):
				if getattr(self, el):
					raise ValueError(
						f"{el} cannot be True without a logger associated with a db logging handler"
					)


	def run(self) -> None:

		event_doc = None
		run_id = self.new_run_id()

		try:

			logger = AmpelLogger.from_profile(
				self.context, self.log_profile, run_id,
				base_flag = LogRecordFlag.T3 | LogRecordFlag.CORE | self.base_log_flag,
				force_refresh = True
			)

			# Admins have the option to to run a T3 process silently/anonymously
			if self.update_events:

				if not self.process_name:
					raise ValueError("Parameter process_name must be defined")

				# Create event doc
				event_doc = DBEventDoc(
					self.context.db, tier=3, run_id=run_id,
					process_name=self.process_name,
				)

			# Feedback
			logger.log(SHOUT, f"Running {self.process_name}")

			for directive in self.directives:

				# run context
				#############

				run_context: Dict[str, Any] = {}

				if self.context.admin_msg:
					run_context['admin_msg'] = self.context.admin_msg

				if directive.context:

					for el in directive.context:
						self.context.loader \
							.new_admin_unit(
								unit_model = el,
								context = self.context,
								sub_type = AbsT3RunContextAppender,
								logger = logger,
								process_name = self.process_name
							) \
							.update(run_context)


				# target selection
				##################

				if directive.select:

					# Spawn and run a new selector instance
					# stock_ids is an iterable (often a pymongo cursor)
					selector = self.context.loader.new_admin_unit(
						unit_model = directive.select,
						context = self.context,
						sub_type = AbsT3Selector,
						logger = logger
					)

					# Usually, id_key is '_id' but it can be 'stock' if the
					# selection is based on t2 documents for example
					id_key = selector.field_name


					# Content loader
					################

					# Spawn requested content loader
					content_loader = self.context.loader \
						.new_admin_unit(
							unit_model = directive.load,
							context = self.context,
							sub_type = AbsT3Loader,
							logger = logger
						)


					# Content complementer
					######################

					# Spawn potentialy requested snapdata complementers
					if directive.complement:
						comps: List[AbsT3DataAppender] = [
							self.context.loader \
								.new_admin_unit(
									unit_model = conf_el,
									context = self.context,
									sub_type = AbsT3DataAppender,
									logger = logger
								)
							for conf_el in directive.complement
						]


					# Unit runner
					#############

					# The default runner provided by pyampel-core is T3DefaultUnitRunner
					runner = self.context.loader \
						.new_admin_unit(
							unit_model = directive.run,
							context = self.context,
							sub_type = AbsT3UnitRunner,
							logger = logger,
							run_id = run_id,
							process_name = self.process_name,
							channel = self.channel,
							raise_exc = self.raise_exc,
							run_context = run_context
						)

					# get pymongo cursor
					if stock_ids := selector.fetch():

						# Run start
						###########

						# Loop until cursor/iterator dries out
						while True:

							# Chunk stock ids, thereby we consume the pymongo cursor
							chunk_ids = [
								sid[id_key] for i, sid in enumerate(stock_ids, start=1)
								if i < self.chunk_size
							]

							# iterator dried out
							if not chunk_ids:
								break

							# Load info from DB
							tran_data = content_loader.load(chunk_ids)

							# Potentialy add complementary information (spectra, TNS names, ...)
							if directive.complement:
								for appender in comps:
									appender.complement(tran_data)

							# Run T3 units defined for this process
							runner.run(list(tran_data))

		except Exception as e:

			if self.raise_exc:
				raise e

			if not logger:
				logger = AmpelLogger.get_logger()

			report_exception(
				self.context.db, logger, exc=e,
				info={'process': self.process_name}
			)

		finally:

			if not logger:
				logger = AmpelLogger.get_logger()

			# Feedback
			logger.log(SHOUT, f"Done running {self.process_name}")
			logger.flush()

			# Register the execution of this event into the events col
			if event_doc:
				event_doc.update(logger)
