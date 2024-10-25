import logging
from collections.abc import Iterable

from mo.application.actions import MakeDirectoryAction
from mo.application.interfaces.organizer import IOrganizer
from mo.application.plan import Plan
from mo.application.use_cases.organize import (
    OrganizeConfig,
    OutputDirectoryNotEmptyError,
)
from mo.application.use_cases.unzip_bundles import UnzipBundles
from mo.application.utils import dir_not_empty


class Consolidate:
    """
    Consolidate downloaded data to Parquet files.

    Args:
        config (OrganizeConfig): The configuration for organizing the data.
        organizers (Sequence[IOrganizer]): The list of organizers to be used.

    Attributes:
        config (OrganizeConfig): The configuration for organizing the data.
        log (Logger): The logger instance for logging messages.
        organizers (Sequence[IOrganizer]): The list of organizers to be used.
    """

    def __init__(self, config: OrganizeConfig, organizers: Iterable[IOrganizer]):
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
        self.organizers = organizers

    def execute(self) -> None:
        self.log.info("Consolidating data")
        self.log.debug("Config:")
        self.log.debug(f"  Inputs: {self.config.inputs}")
        self.log.debug(f"  Output directory: {self.config.output}")
        self.log.debug(f"  Allow unzip: {self.config.allow_unzip}")
        self.log.debug(f"  Remove: {self.config.remove}")
        self.log.debug(f"  Merge: {self.config.merge}")
        self.log.debug(f"  Strip PII: {self.config.strip_pii}")
        self.log.debug(f"  Dry run: {self.config.dry_run}")
        self.log.debug("Organizers:")
        for organizer in self.organizers:
            self.log.debug(f"  {organizer.data_type.value}: `{organizer.pattern}` ({organizer})")

        plan = Plan()

        self.log.info(f"Setting up output directory: {self.config.output.absolute()}")
        if not self.config.dry_run and not self.config.merge and dir_not_empty(self.config.output):
            raise OutputDirectoryNotEmptyError(self.config.output)
        plan.actions.append(MakeDirectoryAction(self.config.output))

        self.log.info("Checking for existing files in output directory")
        if dir_not_empty(self.config.output):
            if self.config.merge:
                self.log.warning("Operation will merge with existing files in output directory")
            self.log.info("Adding output directory to input directories for merging")
            self.config.inputs.append(self.config.output)

        if self.config.allow_unzip:
            self.log.info("Unzipping input bundles")
            UnzipBundles(self.config.inputs, self.config.output).execute()

        self.log.info("Planning how to consolidate")
        for organizer in self.organizers:
            plan += organizer.consolidate(self.config.inputs, self.config.output)

        if self.config.dry_run:
            print(plan)
        else:
            self.log.info("Executing plan")
            self.log.debug(plan)
            plan.execute()
