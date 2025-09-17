HiveConnectorMetadata

We define HiveConnectorMetadata as a general purpose base class of all
Hive connectors. LocalHiveConnectorMetadata is a testing-only
implementation of the interface that stores files and schema
information in the local file system.

Production implementtations are expected to connectt to a metadata server and to support some level of transactions.

We define HiveConnectorLayout which adds Hive specific properties to
the generic TableLayout. A TableLayout corresponds to a
materialization of the table. In Hive there is always one
materialization that may be variously bucketed and/or partitioned. The
getter partitionColumns() in the generic TableLayout refers to Hive
bucketing columns. therefor we add hivePartitionColumns which are the
Hive partition columns, i.e. columns whose value specifies a directory
for the files with a particular value of the column.  We also add
numBuckets for accessing the bucket count.

There could in principle be differently sorted and partitioned layouts
of a table, possibly containing different subsets of the columns. We
do not see this usage in Hive though but it is allowed by the
interface.

Our scope is not limited to Hive. It is therefore crucial to have a
differentiation between table and a particular materialization of it
to to cover a range of physical execution models including indexed
access paths.

The local implementation is initialized to point to a directory. The
HiveConnectorMetadata reads the contents of the directory and
interprets each directory as a table. If the directory contains a
.schema file, we read the partition and bucketing information from the
file. Otherwise we assume no bucketing or Hive partitioning and
interpret all the files as data files.


We offer a sample implementation of a write interface. The writes go
directly into the directory of the table and are not in any way
transactional. This may be completed in the future.

The local implementation samples the files at startup and after
writing. It also offers a sampling interface that reads some
percentage of rows and with a set of filters and produces selectivity
information and optional column by column statistics after filtering.

Implementations over disagg storage and metadata servers can sample
the actual data or return some estimate based on statistics kept by
the metadata server. Sampling is preferrable but at the discretion of
the implementation. Verax caches information from sampling and does
not run the same sample repeatedly.


For now the local connector metadata should be seen as a test-only
reference implementation. A more complete DDL support will be added in
time.
