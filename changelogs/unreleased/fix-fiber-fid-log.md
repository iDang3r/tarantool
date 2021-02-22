## bugfix/core

* Fix logger output for fiber id: format specificator has been
  using signed integers while fibers actually are defined as
  unsigned numbers (gh-5846).
