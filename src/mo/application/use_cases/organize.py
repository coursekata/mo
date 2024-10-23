import logging
from collections.abc import Iterable
from pathlib import Path

from pydantic import DirectoryPath

from mo.application.actions import MakeDirectoryAction
from mo.application.config import CommonConfig
from mo.application.organizers.base import IOrganizer
from mo.application.plan import Plan
from mo.application.use_cases.unzip_bundles import UnzipBundles
from mo.application.utils import dir_not_empty


class OrganizeConfig(CommonConfig):
    inputs: list[DirectoryPath]
    """Directories where the raw data can be found."""

    output: Path
    """Directory where the output data should be written."""

    allow_unzip: bool = False
    """Whether to allow unzipping bundles before organizing."""


class OutputDirectoryNotEmptyError(Exception):
    def __init__(self, path: Path):
        super().__init__(f"Output directory {path} is not empty")


class Organize:
    """
    Organize downloaded data.

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
        """
        Executes the organize use case.

        This method sets up the output directory, uses the organizers to plan how to organize the
        data, and then executes the plan. If `dry_run` is set to True in the configuration, the plan
        will be printed instead of executing it.

        Raises:
            OutputDirectoryNotEmptyError: If the output directory is not empty and `merge` is set to
                False in the configuration.
        """
        self.log.info("Organizing data")
        self.log.debug("Config:")
        self.log.debug(f"  Inputs: {self.config.inputs}")
        self.log.debug(f"  Output directory: {self.config.output}")
        self.log.debug(f"  Allow unzip: {self.config.allow_unzip}")
        self.log.debug(f"  Remove: {self.config.remove}")
        self.log.debug(f"  Overwrite: {self.config.merge}")
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
                self.log.warning("Operation will overwrite existing files in output directory")
            self.log.info("Adding output directory to input directories for merging")
            self.config.inputs.append(self.config.output)

        if self.config.allow_unzip:
            self.log.info("Unzipping input bundles")
            UnzipBundles(self.config.inputs).execute()

        self.log.info("Planning how to organize")
        for organizer in self.organizers:
            plan += organizer.organize(self.config.inputs, self.config.output)

        if self.config.dry_run:
            print(plan)
        else:
            self.log.info("Executing plan")
            plan.execute()
