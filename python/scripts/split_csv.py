#!  /usr/bin/env python3

"""
Split input file into multiple files (suitable for parallel loading)

This assumes the input file
(1) is a compressed CSV file,
(2) has a preamble (lines that start with #), and
(3) has a header row.

NOTE that any existing files matching the pattern <input without .gz>.part_[0-9]*.gz
will be DELETED.
"""

import argparse
import csv
import gzip
import os.path

MAX_PARTITIONS = 128


def delete_part_files(part_name):
    """Delete all files that match the part_name format"""
    print("Deleting old files matching {}".format(part_name))
    directory = os.path.dirname(part_name)
    found = frozenset(os.path.join(directory, filename) for filename in os.listdir(directory))
    possible = frozenset(part_name.format(index) for index in range(0, MAX_PARTITIONS + 1))
    for filename in sorted(possible.intersection(found)):
        print("... deleting {}".format(filename))
        os.unlink(filename)


def split_csv_file(filename, N, part_name):
    """Split input file into (up to) N partitions, keeping preamble and header intact."""
    print("Splitting {} into {:d} partitions".format(filename, N))

    preamble = []
    files = []
    writers = []

    with gzip.open(filename, 'r') as csvfile:
        for line in csvfile:
            if line.startswith("#"):
                preamble.append(line)
            else:
                header = line
                break
        else:
            raise ValueError("Found no header line in %s" % filename)
        reader = csv.reader(csvfile)
        for row_number, row in enumerate(reader):
            index = row_number % N
            if len(writers) == index:
                o = gzip.open(part_name.format(index), 'w')
                for line in preamble:
                    o.write(line)
                o.write(header)
                files.append(o)
                writers.append(csv.writer(o))
            writers[index].writerow(row)
    if len(files) == 0:
        raise ValueError("Found no data rows in %s" % filename)
    elif len(files) < N:
        print("Created only %d file(s)" % len(files))
    for o in files:
        print("... closing {}".format(o.name))
        o.close()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--partitions", help="Number of files to split into (default: %(default)s)",
                        type=int, default=16)
    parser.add_argument("-o", "--output-dir", help="Output directory (default: directory of input file)")
    parser.add_argument("csv_file", help="Input file (CSV format, compressed)")
    parsed = parser.parse_args()
    if parsed.partitions < 2 or parsed.partitions > MAX_PARTITIONS:
        parser.error("invalid value for number of partitions (must be between 2 and {:d})".format(MAX_PARTITIONS))
    return parsed


if __name__ == "__main__":
    args = parse_args()
    csv_file = os.path.normpath(os.path.expanduser(args.csv_file))
    if args.output_dir is None:
        output_dir = os.path.dirname(csv_file)
    else:
        output_dir = os.path.normpath(args.output_dir)
    if not output_dir:
        output_dir = "."
    base, ext = os.path.splitext(os.path.basename(csv_file))
    output_format = os.path.join(output_dir, base + ".part_{:03d}.gz")

    delete_part_files(output_format)
    split_csv_file(csv_file, args.partitions, output_format)
