# Reads a CSV file
import apache_beam as beam
import csv


class CsvFileSource(beam.io.filebasedsource.FileBasedSource):
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
    # Can't just split anywhere
    kwargs['splittable'] = False
    super(self.__class__, self).__init__(*args, **kwargs)

    if not self.header and dictionary_output:
      raise ValueError(
          'a header is required for the CSV reader to provide dictionary output')

  def read_records(self, file_name, range_tracker):
    # If a multi-file pattern was specified as a source then make sure the
    # start/end offsets use the default values for reading the entire file.
    headers = None
    self._file = self.open_file(file_name)

    reader = csv.reader(self._file)

    for i, rec in enumerate(reader):
      if (self.header or self.dictionary_output) and i == 0:
        headers = rec
        continue

      if self.dictionary_output:
        res = {header:val for header, val in zip(headers,rec)}
      else:
        res = rec
      yield res
