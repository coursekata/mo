from collections.abc import Iterable
from itertools import chain
from pathlib import Path
from typing import final

from mo.domain.config import Config
from mo.domain.data_format import DataFormat
from mo.domain.data_types import DataType
from mo.usecases.process_responses_usecase import ProcessResponsesUseCase
from mo.usecases.usecase import DataReadingUseCase


class Input(Config):
    inputs: Iterable[Path]
    output: Path
    output_format: DataFormat = DataFormat.PARQUET
    data_types: list[DataType] = [data_type for data_type in DataType]
    input_formats: list[DataFormat] = [data_format for data_format in DataFormat]
    remove_input: bool = False
    allow_unpack: bool = False


@final
class ConsolidateUseCase(DataReadingUseCase):
    Input = Input

    def execute(self, input: Input) -> None:
        input_dirs = list(input.inputs)
        output_dir = input.output
        for data_type in input.data_types:
            output_path = output_dir / f"{data_type.value}.{input.output_format.value}"
            if output_path.exists():
                raise FileExistsError(f"Output file already exists: {output_path}")

        # unpack any relevant files from archives in the input directory
        if input.allow_unpack:
            self.log.info("Unpacking archives")
            for file in chain.from_iterable(input_dir.rglob("*.zip") for input_dir in input_dirs):
                self._unpack_targets(file, self._targets(input.data_types, input.input_formats))

        # read and consolidate each data type into a single file with the specified output format
        for data_type in input.data_types:
            self.log.info(f"Consolidating {data_type} data")

            # find all the input files
            targets = self._find_data(input_dirs, data_type, input.input_formats)
            if not targets:
                self.log.debug(f"No input files found for {data_type}")
                continue

            # make a plan to load all data of this type for the given format(s)
            data = self._load_data(data_type, targets)

            # load and write the consolidated data to the output directory
            output_path = input.output / f"{data_type.value}.{input.output_format.value}"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            self._write_data(data, output_path, input.output_format)

            # remove the input files if specified
            if input.remove_input:
                for target in targets:
                    if target != output_path:
                        self.log.debug(f"Removing {str(target)}")
                        target.unlink()

        # extra processing for specific data types
        # clean responses
        if DataType.RESPONSES in input.data_types:
            ProcessResponsesUseCase().execute(ProcessResponsesUseCase.Input(input=input.output))

        # merge classes and manifest data
        if DataType.CLASSES in input.data_types and DataType.MANIFEST in input.data_types:
            self.log.info("Merging classes and manifest data")
            self._merge_manifests(input.output, input.output_format, input.remove_input)

    def _merge_manifests(
        self, input: Path, output_format: DataFormat, remove_input: bool = False
    ) -> None:
        classes_path = input / f"{DataType.CLASSES.value}.{output_format.value}"
        manifest_path = input / f"{DataType.MANIFEST.value}.{output_format.value}"
        if classes_path.exists() and manifest_path.exists():
            manifest = self._load_data(DataType.MANIFEST, [manifest_path])
            classes = self._load_data(DataType.CLASSES, [classes_path]).rename(
                {"course_name": "course", "release": "version", "teacher_id": "instructor_id"}
            )
            merged = self._concat_data([manifest, classes]).unique("class_id", keep="first")
            self._write_data(merged, classes_path, output_format)
            if remove_input:
                manifest_path.unlink()
