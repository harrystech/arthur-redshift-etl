#!  /usr/bin/env python3

"""
Split input file into multiple files (suitable for parallel loading).

This assumes the input file is a compressed CSV file and has a preamble
consisting of 0 or more comments (lines that start with #) and a header.

If there are fewer than n*n data rows in the input file (or no data rows),
no output file is created.

NB Make sure that no old partition files exist if you reduce the number of
partitions!
"""

import argparse
import concurrent.futures
import csv
import gzip
from itertools import chain
import logging
import os.path

MAX_PARTITIONS = 128


def split_csv_file(filename, n, part_name, min_lines=None):
    """
    Split input file into (up to) N partitions, keeping preamble and header intact.

    If min_lines is None, then a minimum of n*n is selected.
    """
    logging.info("Splitting '{}' into (up to) {:d} partitions with format '{}'".format(filename, n, part_name))
    if min_lines is None:
        min_lines = n * n
    try:
        with open(filename, "rb") as readable:
            with gzip.open(readable, mode="rt", newline="") as csv_file:
                preamble = []
                for line in csv_file:
                    preamble.append(line)
                    if not line.startswith("#"):
                        break
                else:
                    raise ValueError("Found no header line in '%s'" % filename)
                reader = csv.reader(csv_file)
                buffer = []
                for row_number, row in zip(range(min_lines), reader):
                    buffer.append(row)
                if len(buffer) == 0:
                    logging.warning("Found no data rows in '%s'", filename)
                elif len(buffer) < min_lines:
                    logging.warning("Only %d data rows in '%s', skipping partitioning", len(buffer), filename)
                else:
                    files = []
                    writers = []
                    try:
                        for row_number, row in enumerate(chain(buffer, reader)):
                            index = row_number % n
                            if len(writers) == index:
                                f = open(part_name.format(index + 1), 'wb')
                                g = gzip.open(f, 'wt')
                                for line in preamble:
                                    g.write(line)
                                files.append((g, f))
                                writers.append(csv.writer(g))
                            writers[index].writerow(row)
                    finally:
                        for g, f in files:
                            g.close()
                            f.close()
                    logging.info("Wrote %d file(s) for '%s' to '%s'", len(files), filename, os.path.dirname(part_name))
    except Exception:
        logging.exception("Something terrible happened while processing '%s'", filename)
        raise


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--partitions", help="Number of desired partitions (default: %(default)s)",
                        type=int, default=16)
    parser.add_argument("-j", "--jobs", help="Number of parallel processes (default: %(default)s)", type=int, default=4)
    parser.add_argument("input", help="Input file or input directory with CSV files, possibly in sub-directories")
    parser.add_argument("output", help="Output directory (default: same as input)", nargs="?")
    parsed = parser.parse_args()
    if parsed.partitions < 2 or parsed.partitions > MAX_PARTITIONS:
        parser.error("invalid value for number of partitions (must be between 2 and {:d})".format(MAX_PARTITIONS))
    return parsed


def main(s_input, s_output, n_partitions, n_jobs):
    input_name = os.path.normpath(os.path.expanduser(s_input))
    input_dir = input_name if os.path.isdir(input_name) else os.path.dirname(input_name)
    output_dir = os.path.normpath(os.path.expanduser(s_output)) if s_output else input_dir
    if not os.path.exists(input_dir):
        raise OSError("Input directory not found")
    logging.info("Splitting files from: '%s' to: '%s'", input_name, output_dir)

    with concurrent.futures.ProcessPoolExecutor(max_workers=n_jobs) as executor:
        for root, dirs, files in os.walk(input_dir):
            for filename in sorted(files):
                input_file = os.path.join(root, filename)
                if input_file.endswith(".csv.gz") and (input_name == input_dir or input_name == input_file):
                    base, ext = os.path.splitext(filename)
                    sub_dir = output_dir + root[len(input_dir):]
                    if not os.path.exists(sub_dir):
                        logging.info("Creating directory '%s'", sub_dir)
                        os.makedirs(sub_dir)
                    output_format = os.path.join(sub_dir, base + ".part_{:03d}.gz")
                    executor.submit(split_csv_file, input_file, n_partitions, output_format)

if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(format="[%(asctime)s] %(levelname)s (%(threadName)s) %(message)s", level=logging.DEBUG)
    main(args.input, args.output, args.partitions, args.jobs)
