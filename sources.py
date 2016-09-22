# Reads a CSV file
import apache_beam as beam
import csv


class CsvFileSource(beam.io.textio._TextSource):
  """ A source for a GCS or local comma-separated-file

  Parses a text file assuming newline-delimited lines,
  and comma-delimited fields. Assumes UTF-8 encoding.
  It's completely based on the _TextSource.
  **Note**: It can't parse files with newlines within a field.
  """

  def __init__(self, *args, **kwargs):
    """ Initialize a CSVFileSource.

    Args:
      delimiter: The delimiter character in the CSV file.
      header: Whether the input file has a header or not.
        Default: True
      dictionary_output: The kind of records that the CsvFileSource should output.
        If True, then it will output dict()'s, if False it will output list()'s.
        Default: True

    Raises:
      ValueError: If the input arguments are not consistent.
    """
    self.delimiter = kwargs.pop('delimiter',',')
    self.header = kwargs.pop('header',True)
    self.dictionary_output = kwargs.pop('dictionary_output', True)
    super(self.__class__, self).__init__(*args, **kwargs)

    if not self.header and dictionary_output:
      raise ValueError(
          'a header is required for the CSV reader to provide dictionary output')

  def read_records(self, file_name, range_tracker):
    # If a multi-file pattern was specified as a source then make sure the
    # start/end offsets use the default values for reading the entire file.
    self._new_file = True
    headers = None
    recs = super(CsvFileSource, self).read_records(file_name, range_tracker)

    for rec in recs:
      if len(rec) == 0: continue
      if self._new_file and self.dictionary_output:
        headers = csv.reader([rec], delimiter=self.delimiter).next()
        self._new_file = False
        continue
      self._new_file = False

      if not self.dictionary_output:
        res = csv.reader([rec], delimiter=self.delimiter).next()
      else:
        res = csv.DictReader([rec], delimiter=self.delimiter, fieldnames=headers).next()
      yield res
