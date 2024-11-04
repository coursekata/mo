# `mo`: Methodically Organize CourseKata Data

mo is a simple, opinionated command-line tool designed to efficiently organize your CourseKata data download directories. It moves files from one or more input directories to an output directory based on predefined rules, ensuring a clean and structured data environment. Duplicate files are detected and handled automatically to prevent redundancy.

Features

- Automatic Organization: Moves files into categorized subdirectories based on file types.
- Duplicate Checking: Identifies and skips duplicate files using content hashing.
- Minimal Configuration: Designed to work out-of-the-box with minimal setup.
- Simple CLI: Easy-to-use command-line interface with straightforward commands.

## Installation

`mo` is not yet available via `pip`, but you can install from GitHub:

```bash
pip install git+https://github.com/coursekata/mo
```

## Usage

`mo` has two main commands: `organize` and `compress`. The `organize` command is used to organize CourseKata CSV files into a tidy directory, while the `compress` command merges CSV files of the same type into a single Parquet file for each type.

### Organize

1. Download data from [coursekata.org](https://coursekata.org). You can download it in batches or individually, it doesn't matter.
2. Run `mo organize` wherever you downloaded those data to. For example, if you downloaded the data to a directory called `raw-data` and you want to organize it into `data-organized` you would run:

   ```bash
   mo organize raw-data --output data-organized
   ```

   If your data is in a few different directories, you can pass them all in:

   ```bash
   mo organize raw-data1 raw-data2 raw-data3 --output data-organized
   ```

`mo` will organize your data, checking for duplicates, and only keep the most recent files. By default, files are moved to the output directory, and any other detected CourseKata files are deleted. Care is taken to ensure that only the most up-to-date files are considered, and that the structure of these files is a perfect match to CourseKata formats. If there is any ambiguity, the file is ignored instead of being deleted.

For more information on how to customize the behavior, run `mo organize --help`:

```text
 Usage: mo organize [OPTIONS] INPUTS...

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────╮
│ *    inputs      INPUTS...  Directories to organize. [default: None] [required]               │
╰───────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────╮
│ *  --output             -o      PATH  Directory where the output data should be written.      │
│                                       [default: None]                                         │
│                                       [required]                                              │
│    --copy               -c            Copy the files instead of moving them.                  │
│    --dry-run            -d            Perform a dry run without affecting any files.          │
│    --verbose            -v            Enable verbose logging.                                 │
│    --ignore             -i            Don't delete duplicate input files and legacy types.    │
│    --ignore-legacy                    Don't delete legacy data types.                         │
│    --ignore-duplicates                Don't delete duplicate input files.                     │
│    --log-file                   PATH  File to write logs to. [default: None]                  │
│    --help                             Show this message and exit.                             │
╰───────────────────────────────────────────────────────────────────────────────────────────────╯
```

#### How It Works

1. **Plan**: `mo` generates a plan based on the input directories and the output directory. This plan includes all the files that will be moved, copied, deleted, or ignored. Because of this, `mo` offers a dry-run mode that will show you the plan without actually affecting any files. The contents of the plan will include:

   1. **File Detection**: CourseKata files are always downloaded with a particular structure where the files for a class are in a folder named with the class ID. Inside those folders will be

      - `responses.csv`: The student responses to the questions.
      - `media_views.csv`: Information about student interactions with videos and other media.
      - `page_views.csv`: Information about student engagement with the course pages.
      - `supplementary`: A folder containing supplementary files added by instructors.
      - Legacy Files:
        - `tags.csv`: A legacy file that is no longer used and can be safely deleted.
        - `items.csv`: Information about the questions.

      Additionally, a `classes.csv` file will usually be present just outside the class folders. This file contains information about the classes themselves.

      Note that `mo` is a little more sophisticated than just reading the file names, as it will also check that the contents of the files match the expected structure. This is to prevent accidentally organizing a directory that doesn't contain CourseKata data. Anything that doesn't match the expected structure will be ignored.

      Finally, because these files are often in zipped archives, `mo` will check the contents of all archives when scanning for files.

   2. **Duplicate Detection**: If multiple files are found for the same class, only the most recent file is kept. This is because the more recent file is likely to be the most complete and up-to-date. The other files are considered duplicates and are ignored or deleted based on the options passed to `mo`.
   3. **Moving Files**: Files are moved to their respective category folders within the output directory.
   4. **Deleting Legacy Files and Duplicates**: CourseKata Files that are not moved are deleted. This includes the `tags.csv` and `items.csv` files, which are no longer used. Duplicate files are also deleted. You can control this behavior with CLI flags, see `mo --help` for more information.

2. **Execute**: Once the plan is generated, `mo` will execute it. This includes moving, copying, and deleting files as necessary. Actions are logged to the console as they happen.

   - **Output Structure**: The output directory will have a single folder for each class, named with the class ID. Inside each class folder will be the following:

     - `responses.csv`
     - `media_views.csv`
     - `page_views.csv`
     - `supplementary`: A folder containing supplementary files added by instructors.

     Any `classes.csv` files will be merged and moved to the output directory itself.

3. **Validate**: Finally, `mo` will check the `classes.csv` for consistency with the other files. If there are any classes in the file that don't have corresponding data files, or if there are data files that don't have corresponding entries, `mo` will log a warning with the details.

### Compress

1. Download data from [coursekata.org](https://coursekata.org). You can download it in batches or individually, it doesn't matter.
2. Run `mo compress` wherever you downloaded those data to. For example, if you downloaded the data to a directory called `raw-data` and you want to organize it into `data-compressed` you would run:

   ```bash
   mo compress raw-data --output data-compressed
   ```

   If your data is in a few different directories, you can pass them all in:

   ```bash
   mo compress raw-data1 raw-data2 raw-data3 --output data-compressed
   ```

Similar to `mo organize`, `mo` will look through the data, searching for valid files. Care is taken to ensure that the structure of these files is a perfect match to CourseKata formats. If there is any ambiguity, the file is ignored. After finding and validating the files, `mo` will compress them into Parquet files, one for each type of data, yielding an output directory like this:

```text
data-compressed
├── classes.parquet
├── media_views.parquet
├── page_views.parquet
├── responses.parquet
└── supplementary
    ├── class_1
    │   └── file_1
    └── class_2
        └── file_2
```

> **Note**: You can run `mo compress` again with new data and the same output directory. `mo` will automatically detect and merge the new data with the existing data. Do note however, that if you are adding in a lot of data to an already large dataset, the process might fail. This is because `mo` only keeps unique data, which means that the data is loaded into memory and compared to the existing data. If the data is too large, it might exceed the memory limits of your machine.

For more information on how to customize the behavior, run `mo compress --help`:

```text
 Usage: mo compress [OPTIONS] INPUTS...

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────╮
│ *    inputs      INPUTS...  Directories to organize. [default: None] [required]               │
╰───────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────╮
│ *  --output           -o      PATH  Directory where the output data should be written.        │
│                                     [default: None]                                           │
│                                     [required]                                                │
│    --move             -m            Delete the input files after compressing.                 │
│    --skip-validation  -s            Skip validation of the input files.                       │
│    --dry-run          -d            Perform a dry run without affecting any files.            │
│    --verbose          -v            Enable verbose logging.                                   │
│    --log-file                 PATH  File to write logs to. [default: None]                    │
│    --help                           Show this message and exit.                               │
╰───────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch: `git checkout -b feature-name`.
3. Commit your changes: `git commit -am 'feat: new feature'`.
4. Push to the branch: `git push origin feature-name`.
5. [Open a Pull Request](https://github.com/coursekata/mo/pulls).

## License

`mo` is licensed under the MIT License.

## Contact

For questions, suggestions, or feedback, [please submit an Issue](https://github.com/coursekata/mo/issues).
