import zipfile
from pathlib import Path


class UnzipBundles:
    def __init__(
        self,
        inputs: list[Path],
        output: Path | None = None,
        pattern: str = "*.zip",
    ) -> None:
        self.inputs = inputs
        self.output = output
        self.pattern = pattern

    def execute(self) -> None:
        for input_dir in self.inputs:
            for bundle in input_dir.rglob(self.pattern):
                output_path = self.get_output_path(input_dir, bundle)
                output_path.mkdir(exist_ok=True)
                with zipfile.ZipFile(bundle, "r") as zip:
                    zip.extractall(output_path)

    def get_output_path(self, input_path: Path, bundle_path: Path) -> Path:
        output_base_dir = self.output or input_path
        return output_base_dir / bundle_path.relative_to(input_path).with_suffix("")
