# -*- coding: utf-8 -*-

import unittest

from beam_utils.sources import JsonLinesFileSource, CsvFileSource
from apache_beam.io.fileio import CompressionTypes

import gzip
import tempfile
try:
  from StringIO import StringIO
except ImportError:
  from io import StringIO

class TestCsvFileSource(unittest.TestCase):

    CSV_LINES_FILE = """header1,header2,header3
value1,value2,value3
"""


    def test_gzipped_csv(self):
        out = StringIO()
        with tempfile.NamedTemporaryFile() as f:
            with gzip.GzipFile(filename=f.name, mode='wb') as gz:
                gz.write(self.CSV_LINES_FILE)

            csv = CsvFileSource(
                    f.name,
                    compression_type=CompressionTypes.GZIP)

            # first record as a dict
            rec = csv.read_records(f.name, None).next()

            # make a dict from the csv lines
            lines = self.CSV_LINES_FILE.split('\n')
            csv_tuples = zip(lines[0].split(','), lines[1].split(','))
            csv_dict = {header: value for (header, value) in csv_tuples}

            # assert both dicts are equal
            for (header, value) in rec.iteritems():
                self.assertEqual(value, csv_dict[header])




class TestJsonLinesFileSource(unittest.TestCase):

  JSON_LINES_FILE = """{"name":"Pablo José"}
{"name":"Jan","age":23}
{"name":"Ñoño","country":"Norway"}
{"name":"Alex","country":"United\\nStates"}
"""
  EXPECTED_JSON = [
      {u"name": u"Pablo José"},
      {u"name": u"Jan", u"age": 23},
      {u"name": u"Ñoño", u"country": u"Norway"},
      {u"name": u"Alex", u"country": u"United\nStates"},
      ]

  def test_parse_multiple_json_lines(self):
    fileobj = StringIO(self.JSON_LINES_FILE)
    src = JsonLinesFileSource('anyfile', validate=False)

    result = list(src._json_parse(fileobj))
    print result
    self.assertEqual(result, self.EXPECTED_JSON)


if __name__ == '__main__':
  unittest.main()
