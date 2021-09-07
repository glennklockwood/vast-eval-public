#!/usr/bin/env python3
"""Extracts IOR arguments from IOR outputs

Finds IOR output files, extracts the command line used to generate that output
file, then identifies how many unique IOR configurations were used.  Useful
for finding common arguments across the outputs of large parameter sweeps.
"""

import os
import re
import gzip

# ior-scaling-rdma.vast.v3-naive ior+incompress       --stoneWallingWearOut=1 -C -D=45 -F -e -g -k -vv -w
# ior-scaling-rdma.vast.v3-naive ior+incompress       --stoneWallingWearOut=1 -C -D=45 -F -e -g -r -vv
# ior-scaling-rdma.vast.v4       ior+incompress       --stoneWallingWearOut=1 -C -D=45 -F -e -g -k -l=random -r -vv
# ior-scaling-rdma.vast.v4       ior+incompress       --stoneWallingWearOut=1 -C -D=45 -F -e -g -k -l=random -vv -w
# ior-scaling-rdma.vast.v4       ior+incompress       --stoneWallingWearOut=1 -C -D=45 -F -e -g -l=random -r -vv
# randio                         glior-3.3            -C -D=45 -F -e -g -k -r -vv -z
# randio                         glior-3.3            -C -D=45 -F -e -g -vv -w -z
# randio.odirect                 glior-3.3            --posix.odirect -C -D=45 -F -e -g -k -r -vv -z
# randio.odirect                 glior-3.3            --posix.odirect -C -D=45 -F -e -g -vv -w -z
# randio.vs-size                 glior-3.3            -C -D=45 -F -g -r -vv -z
# randio.vs-size                 ior+incompress       -C -D=300 -F -k -l=random -vv -w

DATASET_NAMES = {
    "ior-scaling-rdma.vast.v3-naive": "Bandwidth, Naive",
    "ior-scaling-rdma.vast.v4": "Bandwidth, Aged",
    "randio": "IOPS, Buffered I/O",
    "randio.odirect": "IOPS, Direct I/O",
    "randio.vs-size": "IOPS vs. Size",
}

def decode_command_line(line):
    line = line.split(':', 1)[-1]
    line = re.sub(r' -[btsp]\s+\S+', '', line)
    line = re.sub(r' -O stoneWallingStatusFile=\S+', '', line)
    line = re.sub(r' -O ', ' --', line)
    line = re.sub(r' -l ', ' -l=', line)
    line = re.sub(r' -D ', ' -D=', line)
    line = re.sub(r' -o \S+', '', line)
    line = re.sub(r'/ior-n\S+.out ', ' ', line)

    ior_exe, args = line.split(None, 1)

    # /global/u2/g/glock/src/git/n10/vast-eval/results/ior-scaling-rdma.vast.v4/../../src/ior+incompress/install.cgpu/bin/ior
    ior_exe = re.sub(r'\S+/([^/]+)/([^/]+)/bin/(ior|IOR)', r'\1', ior_exe)

    return ior_exe.strip(), args.strip()

def extract_commands(filename):
    if filename.endswith('.gz'):
        opener = gzip.open
    else:
        opener = open

    found = 0
    with opener(filename, 'r') as outfile:
        for line in outfile:
            if isinstance(line, bytes):
                line = line.decode()
            if line.startswith('Command line'):
                found += 1
                yield decode_command_line(line)

if __name__ == "__main__":
    ior_cmds = set()
    for dirname, subdirs, files in os.walk(os.getcwd()):
        for fname in files:
            if fname.endswith('.out') or fname.endswith('.out.gz'):
                dataset_name = os.path.basename(dirname)
                for ior_exe, ior_args in extract_commands(os.path.join(dirname, fname)):
                    ior_cmds.add("{} {} {}".format(
                        dataset_name,
                        ior_exe,
                        " ".join(sorted(ior_args.split()))
                    ))

    for ior_cmd in sorted(list(ior_cmds)):
        dataset_name, ior_exe, args = ior_cmd.split(None, 2)
        print("{:20s} {:20s} {}".format(DATASET_NAMES.get(dataset_name, dataset_name), ior_exe, args))
