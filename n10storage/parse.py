"""Parsers for common benchmark tool outputs.
"""
import re
import datetime
import warnings

class BenchmarkOutput(dict):
    """Parses stdout of an I/O benchmark into dictionary format

    This is the base class for an object that extracts the performance
    measurements logged to stdout by a generic I/O benchmarking tool along
    with some metadata relevant to analysis.

    Args:
        content: File-like object that contains the stdout of a run
        normalize_results (bool): copy header key-values to every record
    """
    def __init__(self, content, normalize_results=False, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._content = content
        self._parser = self.find_run_begin
        self._this_record = None
        self._normalize_records = normalize_results

    @staticmethod
    def humansize2bytes(humansize):
        """Converts a human-readable quantity and unit to bytes

        Args:
            humansize (bytes): String of the format 4 MiB

        Returns:
            int: Number of bytes represented by humansize
        """
        xsize, unit = humansize.strip().split()
        xsize = float(xsize)
        if unit.endswith("/s"):
            unit = unit[:-2]
        if unit == 'bytes':
            pass
        elif unit == 'KiB':
            xsize *= 1024.0
        elif unit == 'MiB':
            xsize *= 2**20
        elif unit == 'GiB':
            xsize *= 2**30
        elif unit == 'TiB':
            xsize *= 2**40
        elif unit == 'PiB':
            xsize *= 2**50
        elif unit == 'EiB':
            xsize *= 2**60
        else:
            raise ValueError(f"got unreal xsize {xsize:f}, unit={unit}")

        return xsize

    @staticmethod
    def coerce_value(value):
        """Converts a string into a numeric value.

        Attempts to convert ints into ints, floats into floats, and handles
        explicitly undefined values.

        Args:
            value (str): String representation of an int, float, "-", or "NA".

        Returns:
            int, float, None
        """
        if value in ('-', 'NA'):
            return None
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    def load_output(self, content=None):
        """Initiates the parsing process on an IOR output.

        Results are stored as key-value pairs in self.

        Args:
            content: File-like object or None containing the text to be parsed.
                When provided, overrides the data source provided when the
                object was initialized.
        """
        if content:
            self._content = content
        line = next(self._content)
        while line:
            self.parse_line(line)
            line = next(self._content)

    def parse_line(self, line):
        """Attempts to parse a single line of IOR output.

        Calls the correct parser based on the internal state of the parsing
        process (its position within the expected IOR output format).

        Args:
            line (str): The line to be passed to the parser.
        """
        if isinstance(line, bytes):
            line = line.decode()
        if not self.parse_anywhere(line):
            self._parser(line)

    def parse_anywhere(self, line):
        """Identifies certain lines that can appear anywhere in the log

        Returns:
            bool: whether the line was successfully parsed or not
        """
        return False

    def find_run_begin(self, line):
        """Finds the beginning of a new job output

        Handles multiple jobs' outputs concatenated in a single file.
        """
        pass

class IorOutput(BenchmarkOutput):
    """Parses stdout of IOR into dictionary format

    Extracts the performance measurements logged to stdout by IOR along with
    some metadata relevant to analysis.  This is not meant to be comprehensive
    and only looks for specific data within the output.  It supports outputs
    of IOR 3.3.0 and later.

    Args:
        content: File-like object that contains the stdout of an IOR run.
        normalize_results (bool): copy header key-values to every record
    """
    def __init__(self, content, normalize_results=False, *args, **kwargs):
        super().__init__(*args, content=content, normalize_results=normalize_results, **kwargs)

        self._parser = self.find_run_begin
        self._result_columns = None
        self._summary_columns = None
        self._filename_rex = re.compile(
            r'ior-n(\d+)ppn(\d+)t(\d+)([mgkMGK]?)(\.\d+)?.out')
        # ior-n2p16T90-read.out
        self._filename_rex_simple = re.compile(
            r'ior-n(\d+)p(pn)?(\d+).*\.out')

        try:
            self.load_output()
        except StopIteration:
            pass

        if self._normalize_records:
            for rec in self.get('results', []):
                rec.update(self['header'])

    def parse_anywhere(self, line):
        """Identifies certain lines that can appear anywhere in the log

        Returns:
            bool: whether the line was successfully parsed or not
        """

        # detect individual stonewall pair reporting output -
        # there is no deterministic way to correctly associate each rank's
        # output with a specific read or write step within a
        # multi-iteration job.  this is because the order in which these
        # lines are printed to stdout are a function of the mpi job
        # launcher's multiplexing of stdout.  be very careful when trying
        # to interpret this data!
        if 'stonewalling pairs accessed:' in line:
            if 'stonewall_pairs' not in self:
                self['stonewall_pairs'] = [{}]

            tokens = line.split(':')
            key, val = int(tokens[0]), int(tokens[-1])

            # if we detect a repeat rank, it must mean we have begun a new
            # record this will misbehave if not all ranks report in; hopefully
            # this doesn't happen.
            if key in self['stonewall_pairs'][-1]:
                self['stonewall_pairs'].append({})

            self['stonewall_pairs'][-1][key] = val
            return True
        elif line.startswith('Finished            :'):
            if 'header' not in self:
                self['header'] = {}
            self['header']['end'] = int(datetime.datetime.strptime(
                line.split(":", 1)[-1].strip(),
                "%c").timestamp()) # Tue Jul 20 12:59:31 2021
            self._parser = self.find_run_begin
            return True
        return False

    def find_run_begin(self, line):
        """Finds the beginning of a new IOR output

        Handles multiple IOR outputs concatenated in a single file.
        """
        if line.startswith('IOR') \
        and 'MPI Coordinated Test of Parallel I/O' in line:
            self['header'] = {}
            self._parser = self.parse_run_metadata

    def parse_run_metadata(self, line):
        """Extracts metadata printed at IOR start
        """
        if 'header' not in self:
            self['header'] = {}
        # warning: these will currently collide with filename metadata
        if line.startswith('nodes               :'):
            self['header']['nodes'] = int(line.split(":")[-1].strip())
        elif line.startswith('tasks               :'):
            self['header']['nproc'] = int(line.split(":")[-1].strip())
        elif line.startswith('clients per node    :'):
            self['header']['ppn'] = int(line.split(":")[-1].strip())
        elif line.startswith('xfersize            :'):
            self['header']['xfersize'] = self.humansize2bytes(line.split(":")[-1])
        elif line.startswith('ordering in a file  :'):
            self['header']['ordering'] = line.split(":")[-1].strip()
        elif line.strip().startswith('aggregate filesize'):
            self['header']['aggsize_bytes'] = self.humansize2bytes(line.split(":")[-1])
            self._parser = self.find_results_begin
        elif line.startswith('StartTime           :'):
            self['header']['start'] = datetime.datetime.strptime(
                line.split(":", 1)[-1].strip(),
                "%c").timestamp() # Tue Jul 20 12:59:31 2021

    def find_results_begin(self, line):
        """Identifies the start of IOR run results being printed.
        """
        if line.strip() == 'Results:':
            self._parser = self.find_results_header

    def find_results_header(self, line):
        """Identifies the columns that will appear in IOR run results.
        """
        if line.strip() == '': # empty line
            pass
        elif line.startswith('WARNING: The file'):
            pass
        elif line.startswith('Using Time Stamp'):
            pass
        elif line.strip().startswith('access'):
            self._result_columns = [x.lower() for x in line.strip().split()]
            self._parser = self.parse_result
            next(self._content) # separator line

    def parse_result(self, line):
        """Parses a run result line in the results section.

        Also identifies some outputs generated by nonzero MPI ranks that may
        be printed during a run such as per-rank stonewall data.
        """
        if line.startswith('Max'):
            self._parser = self.find_summary

        # find a line containing a row of results
        elif line.startswith('read') \
        or line.startswith('write') \
        or line.startswith('remove'):
            values = [self.coerce_value(x) for x in line.strip().split()]
            if self._this_record is None:
                self._this_record = dict(
                    list(zip(self._result_columns, values)))
            else:
                self._this_record.update(
                    dict(list(zip(self._result_columns, values))))

            if self._this_record.get('access') in ('read', 'write') \
            and 'bw(mib/s)' in self._this_record:
                max_key = 'max_{}_mibs'.format(self._this_record['access'])
                if max_key not in self \
                or self[max_key] < self._this_record['bw(mib/s)']:
                    self[max_key] = self._this_record['bw(mib/s)']

            if 'results' not in self:
                self['results'] = []
            self['results'].append(self._this_record)
            self._this_record = None

        # find line timestamping the start of an individual run
        elif line.startswith('Commencing'):
            stamp = datetime.datetime.strptime(
                line.split(':', 1)[-1].strip(),
                '%a %b %d %H:%M:%S %Y')
            if self._this_record is None:
                self._this_record = {'timestamp': stamp.timestamp()}
            else:
                this_access = line.strip().split()[1].lower()
                expected_access = self._this_record.get('access').lower()
                if expected_access and expected_access != this_access:
                    warnings.warn(
                        'encountered {} timestamp for a {} record'.format(
                            this_access, expected_access))
                else:
                    self._this_record.update({'timestamp': stamp.timestamp()})

        # find stonewalling stats line
        elif line.startswith('stonewalling pairs accessed '):
            args = line.split()
            if self._this_record is None:
                self._this_record = {}
            self._this_record.update({
                'stonewall_min_xfers': int(args[4]),
                'stonewall_max_xfers': int(args[6]),
                'stonewall_time_secs': float(args[-1].rstrip('s'))
            })
        # this is degenerate with aggsize(MiB/s) in the summary line but may not be present in all files
        elif line.startswith('WARNING: Using actual aggregate bytes moved'):
            if self._this_record is None:
                self._this_record = {}
            self._this_record.update({
                'stonewall_bytes_moved': int(line.split()[-1].rstrip('.')),
            })

    def find_summary(self, line):
        """Finds the end of the results section and the end of the run.
        """
        if line.strip() == 'Summary of all tests:':
            line = next(self._content) # header line
            self._summary_columns = [x.lower() for x in line.strip().split()]
            self._parser = self.parse_summary

    def parse_summary(self, line):
        """Parses one line of the final min/max summary line.
        """
        if line.startswith('write') or line.startswith('read'):
            values = [self.coerce_value(x) for x in line.strip().split()]
            record = dict(list(zip(self._summary_columns, values)))
            if 'summaries' not in self:
                self['summaries'] = []
            self['summaries'].append(record)

    def add_filename_metadata(self, filename):
        """Extracts metadata from the name of the output file itself.

        Assumes that filenames follow the convention ior-nXXpYYtZZ.out where

        * XX = number of nodes used in this test
        * YY = number of processes per node used
        * ZZ = transferSize parameter used (in bytes)
        """
        match = self._filename_rex.search(filename)
        if 'header' not in self:
            self['header'] = {}
        if match:
            xfersize = int(match.group(3))
            if match.group(4).lower() == 'k':
                xfersize *= 2**10
            elif match.group(4).lower() == 'm':
                xfersize *= 2**20
            elif match.group(4).lower() == 'g':
                xfersize *= 2**30

            self['header'].update({
                'nodes': int(match.group(1)),
                'ppn': int(match.group(2)),
                'xfersize': xfersize
            })
            return
        match = self._filename_rex_simple.search(filename)
        if match:
            self['header'].update({
                'nodes': int(match.group(1)),
                'ppn': int(match.group(3)),
            })
            return
        warnings.warn("Could not extract metadata from filename {}".format(filename))

class MdWorkbenchOutput(BenchmarkOutput):
    """Parses stdout of md-workbench into dictionary format

    Args:
        content: File-like object that contains the stdout of a run.
        normalize_results (bool): copy header key-values to every record
    """
    def __init__(self, content, normalize_results=False, *args, **kwargs):
        super().__init__(*args, content=content, normalize_results=normalize_results, **kwargs)

        self._parser = self.find_run_begin
        self._stonewall_runtime_rex = re.compile(r"^(\d+): stonewall runtime ([^s]+)s")
        self._op_stats_record = re.compile(r"(\w+)\(([^s]+)s,\s*([^s]+)s,\s*([^s]+)s,\s*([^s]+)s,\s*([^s]+)s,\s*([^s]+)s,\s*([^s]+)s\)")

        try:
            self.load_output()
        except StopIteration:
            pass

        if self._normalize_records:
            for rec in self.get('results', []):
                rec.update(self['header'])

    def parse_anywhere(self, line):
        """Identifies certain lines that can appear anywhere in the log

        Returns:
            bool: whether the line was successfully parsed or not
        """

        match = self._stonewall_runtime_rex.search(line)

        if match:
            if 'stonewall_runtime' not in self:
                self['stonewall_runtime'] = [{}]

            rank, runtime = int(match.group(1)), float(match.group(2))

            # if we detect a repeat rank, it must mean we have begun a new
            # record this will misbehave if not all ranks report in; hopefully
            # this doesn't happen.
            if rank in self['stonewall_runtime'][-1]:
                self['stonewall_runtime'].append({})

            self['stonewall_runtime'][-1][rank] = runtime
            return True
        elif line.startswith("Total runtime"):
            if "header" not in self:
                self["header"] = {}
            _, runtime, timestamp = line.split(":", 2)
            self["header"]["walltime"] = int(runtime.strip().split("s", 1)[0])
            self["header"]["end"] = int(datetime.datetime.strptime(
                timestamp.strip(),
                "%Y-%m-%d %H:%M:%S").timestamp()) # 2021-08-30 09:57:39
            self._parser = self.find_run_begin
            return True
        return False

    def find_run_begin(self, line):
        """Finds the beginning of a new IOR output

        Handles multiple IOR outputs concatenated in a single file.
        """
        #if line.startswith('Args:'):
        if line.startswith("MD-Workbench total"):
            self['header'] = {}

            args = line.strip().split()
            self["header"].update({
                "total_objects": int(args[3]),
                "workingset_size_bytes": self.humansize2bytes(" ".join(args[6:8])),
                "version": args[9],
                "start": int(datetime.datetime.strptime(
                    " ".join(args[11:13]),
                    "%Y-%m-%d %H:%M:%S").timestamp()) # 2021-08-30 09:57:39
            })
            self._parser = self.find_results_line

    def find_results_line(self, line):
        if line.startswith("benchmark process"):
            args = line.strip().split()
            if 'results' not in self:
                self['results'] = []
            record = {
                "phase": "2",
                "walltime_max_secs": float(args[2].split(":", 1)[-1].rstrip("s")),
                "walltime_min_secs": float(args[3].split(":", 1)[-1].rstrip("s")),
                "walltime_mean_secs": float(args[5].rstrip("s")),
                "walltime_std_secs": float(args[7].split(":", 1)[-1]),
                "iops": float(args[8].split(":", 1)[-1].rstrip("s")),
                "num_objects": float(args[10].split(":", 1)[-1]),
                "cycle_rate": float(args[11].split(":", 1)[-1]),
                "bw(mib/s)": self.humansize2bytes(" ".join(args[13:15]).split(":", 1)[1]) / 2**20,
                "op_max_secs": float(args[15].split(":", 1)[-1].rstrip("s")),
                "num_op_errors": int(args[16].lstrip("(")),
                "stonewall_cycles": int(args[18].split(":", 1)[-1]),
            }

            for match in self._op_stats_record.finditer(line):
                opname = match.group(1)
                record.update({
                    f"{opname}_min_secs": float(match.group(2)),
                    f"{opname}_q1_secs": float(match.group(3)),
                    f"{opname}_median_secs": float(match.group(4)),
                    f"{opname}_q3_secs": float(match.group(5)),
                    f"{opname}_q90_secs": float(match.group(6)),
                    f"{opname}_q99_secs": float(match.group(7)),
                    f"{opname}_max_secs": float(match.group(8)),
                })
            self["results"].append(record)
