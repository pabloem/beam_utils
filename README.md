## My Personal Beam Utils
This package is a small set of Apache Beam utilities that I've coded
as I've needed them. These might -or might not- eventually make it into
the Apache Beam Python SDK. The utilities are the following:

* **Sources**. A few sources for common file formats:
  * \[CsvFileSource\] - A source for CSV files. It returns Dictionaries and Lists
  * \[JsonLinesFileSource\] - A source for JsonLines files, where each JSON
    object is in a single line.

* **Coders**. A few coders for common encodings
  * \[NoopCoder\] - It does not do any operation when encoding/decoding.

## Installation
The package is available on PiPy as `beam_utils`. Just use `pip install
beam_utils`, and import it as necessary. The `apache_beam` package is a
dependency.
