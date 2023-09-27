# `mo`: Methodically Organize CourseKata Data

This repository contains the `mo` package, which is used to methodically organize data downloaded from [CourseKata](https://coursekata.org/) classes. The general idea is that you download the data from CourseKata, and then use `mo` to organize it into a format that is easier to work with.

## Installation

`mo` is not yet available via `pip`, but you can install from GitHub:

```bash
pip install git+https://github.com/coursekata/mo
```

## Usage

1. Download data from [coursekata.org](https://coursekata.org). You can download it in batches or individually, it doesn't matter.
2. Put all of the data in to a single directory on your computer.
3. Run `mo` on that directory. For example, if you downloaded the data to a directory called `coursekata_data`, you would run:

   ```bash
   mo organize coursekata_data
   ```

4. `mo` will organize your data and compress it into parquet files. The original data will be deleted in favor of the new format. If for some reason you wanted to retain that raw data, you could use the `--keep-source` flag, but there is almost no reason to do so.
