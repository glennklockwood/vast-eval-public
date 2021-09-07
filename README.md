# Dataset Summary

This table is generated from `extract-ior-args.py` with a little manual
formatting to make common arguments easy to identify.

Dataset            | IOR version        | Step     | Arguments
-------------------|--------------------|----------|---------------------------------------------------------------------
Bandwidth, Naive   | ior-incompress     | Write    | --stoneWallingWearOut=1 -C -D=45  -F -e -g -k              -vv -w
Bandwidth, Naive   | ior-incompress     | Read     | --stoneWallingWearOut=1 -C -D=45  -F -e -g              -r -vv
Bandwidth, Aged    | ior-incompress     | Write    | --stoneWallingWearOut=1 -C -D=45  -F -e -g -k -l=random    -vv -w
Bandwidth, Aged    | ior-incompress     | Pre-Read |                         -C        -F -e -g    -l=random    -vv -w
Bandwidth, Aged    | ior-incompress     | Read     | --stoneWallingWearOut=1 -C -D=45  -F -e -g    -l=random -r -vv
IOPS, Buffered I/O | ior-3.3.0+6356464  | Write    |                         -C -D=45  -F -e -g                 -vv -w -z
IOPS, Buffered I/O | ior-incompress     | Pre-Read | --stoneWallingWearOut=1    -D=600 -F       -k -l=random    -vv -w   
IOPS, Buffered I/O | ior-3.3.0+6356464  | Read     |                         -C -D=45  -F -e -g -k           -r -vv    -z
IOPS, Direct I/O   | ior-3.3.0+6356464  | Write    | --posix.odirect         -C -D=45  -F -e -g                 -vv -w -z
IOPS, Direct I/O   | ior-incompress     | Pre-Read | --stoneWallingWearOut=1    -D=600 -F       -k -l=random    -vv -w   
IOPS, Direct I/O   | ior-3.3.0+6356464  | Read     | --posix.odirect         -C -D=45  -F -e -g -k           -r -vv    -z
IOPS vs. Size      | ior-incompress     | Pre-Read |                         -C -D=300 -F       -k -l=random    -vv -w
IOPS vs. Size      | ior-3.3.0+6356464  | Read     |                         -C -D=45  -F    -g              -r -vv    -z
