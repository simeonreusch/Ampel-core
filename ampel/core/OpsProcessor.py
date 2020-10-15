
from typing import Any

from ampel.abstract.AbsOpsUnit import AbsOpsUnit
from ampel.model.UnitModel import UnitModel
from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.log import AmpelLogger, LogRecordFlag, SHOUT
from ampel.log.utils import report_exception

class OpsProcessor(AbsProcessorUnit):

    execute: UnitModel
    log_profile: str = "console_verbose"

    def run(self) -> Any:

        logger = None

        try:
            logger = AmpelLogger.from_profile(
                self.context, self.log_profile,
                base_flag = LogRecordFlag.CORE | self.base_log_flag,
                force_refresh = True
            )
            self.context.loader \
                .new_admin_unit(
                    unit_model = self.execute,
                    context = self.context,
                    sub_type = AbsOpsUnit,
                    logger = logger,
                ) \
                .run()
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