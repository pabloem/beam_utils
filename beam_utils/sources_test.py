# -*- coding: utf-8 -*-

import unittest

from beam_utils.sources import JsonLinesFileSource

try:
  from StringIO import StringIO
except ImportError:
  from io import StringIO


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
