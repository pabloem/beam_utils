# Implements a few sources for

import csv
import json
from functools import partial

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import fileio

__all__ = ['JsonLinesFileSource', 'CsvFileSource']


class JsonLinesFileSource(beam.io.filebasedsource.FileBasedSource):

  def __init__(self, file_pattern,
               compression_type=fileio.CompressionTypes.AUTO,
               coder=coders.StrUtf8Coder(), validate=True):
    """ Initialize a JsonLinesFileSource.
    """

    super(self.__class__, self).__init__(file_pattern, min_bundle_size=0,
                                         compression_type=compression_type,
                                         validate=validate,
                                         splittable=False)
    self._coder = coder

  def read_records(self, file_name, range_tracker):
    self._file = self.open_file(file_name)
    for rec in self._json_parse(self._file):
      yield rec

  def _json_parse(self, fileobj):
    for line in fileobj:
      line = self._coder.decode(line)
      result = json.loads(line)
      yield result


class CsvFileSource(beam.io.filebasedsource.FileBasedSource):
  """ A source for a GCS or local comma-separated-file

  Parses a text file assuming newline-delimited lines,
  and comma-delimited fields. Assumes UTF-8 encoding.
  """

  def __init__(self, file_pattern,
               compression_type=fileio.CompressionTypes.AUTO,
               delimiter=',', header=True, dictionary_output=True,
               validate=True):
    """ Initialize a CSVFileSource.

    Args:
      delimiter: The delimiter character in the CSV file.
      header: Whether the input file has a header or not.
        Default: True
      dictionary_output: The kind of records that the CsvFileSource outputs.
        If True, then it will output dict()'s, if False it will output list()'s.
        Default: True

    Raises:
      ValueError: If the input arguments are not consistent.
    """
    self.delimiter = delimiter
    self.header = header
    self.dictionary_output = dictionary_output
    # Can't just split anywhere
    super(self.__class__, self).__init__(file_pattern,
                                         compression_type=compression_type,
                                         validate=validate,
                                         splittable=False)

    if not self.header and dictionary_output:
      raise ValueError(
          'header is required for the CSV reader to provide dictionary output')

  def read_records(self, file_name, range_tracker):
    # If a multi-file pattern was specified as a source then make sure the
    # start/end offsets use the default values for reading the entire file.
    headers = None
    self._file = self.open_file(file_name)

    reader = csv.reader(self._file, delimiter=self.delimiter)

    for i, rec in enumerate(reader):
      if (self.header or self.dictionary_output) and i == 0:
        headers = rec
        continue

      if self.dictionary_output:
        res = {header:val for header, val in zip(headers,rec)}
      else:
        res = rec
      yield res
