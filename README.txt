Careful, this test harness does NOT delete data. If you try to use it against an index that already has data you may get false positives, you need to clear it out manually.

Usage:

java -classpath elasticsearch-stresstest-1.0-SNAPSHOT.jar datagrid.Main <Arguments>
  -data VAL        : the data path to read the sample data from
  -docSizeToTest N : Document size to test (in bytes -- for sizeTest operation only)
  -endpoint VAL    : ES endpoint to connect to
  -index VAL       : index to write to
  -number N        : the number of writes to perform
  -operation VAL   : Operation to run ('partitionTest', 'sizeTest')
  -sleepmsecs N    : the length of time to pause in between write transactions
  -type VAL        : the elasticsearch type to use
