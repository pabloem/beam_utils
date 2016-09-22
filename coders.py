class NoopCoder():
  """ Implements coder interface. Returns data as is.
  """
  def decode(self, record):
    return record
