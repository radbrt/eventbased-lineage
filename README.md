# Event-based lineage with Prefect

This library seeks to transparently add data lineage to Prefect Flows by providing a specialized Lineage Block type that can represent common connections (first off: databases) and write relevant information to prefect events.


#### ⚠️ This is all very young ⚠️
No code here is ready for anything, but is intended to provide some proof-of-concepts and ideas.

There hasn't been a lot of focus on lineage lately. dbt has some lineage, data catalogues are able to parse lineage from query history etc, and increasingly also able to do column level lineage. This is all within the SQL paradigm, where CTAS or insert-into-from-select type statements means that lineage can be provided by parsing a single (albeit often complex) SQL statement.

Providing lineage for arbitrary python code is a lot harder, and it is unrealistic to expect as fine-grained lineage there as with SQL. But surely, _something_ can be done.

In photography, there is an expression "the best camera is the one you actually use". Something similar goes for lineage - if lineage requires a lot of abstraction overhead, it is less likely to be used. And so, it is of limited use. Therefore, the purpose here is to provide almost transparent abstractions for data lineage - in keeping with the prefect philosophy of just letting pepole write python.

## Existing lineage concepts

While the project started out focusing on Marquez (check out the v0 release if you want to see the status right before we moved away from Marquez), we have pivoted to use Prefect Events. It is a goal though, that the sum of events posted can be assembled to OpenLineage records. Part of the reason we moved away from OpenLineage has to do with the difficulty in automatically marking runs as succeeded or failed, which comes free with Prefect.

The payload element of the event, together with other information in the events in the flow, should be sufficient to cobble together a full OpenLineage record similar to this example:

```json
{
    "eventType": "COMPLETE",
    "eventTime": "2023-04-07T11:32:46.042220Z",
    "job": {
        "namespace": "meltano",
        "name": "tap-mysql-to-target-snowflake"
    },
    "run": {
        "runId": "7725dfc77197dcc8ba3acb0c33ff1868a841881e9ecb3b411af406ede9bebc3d",
        "facets": {
            "metrics": {}
        }
    },
    "inputs": [
        {
            "namespace": "mysql://my-db.my-schema",
            "name": "numbers",
            "facets": {
                "schema": {
                    "fields": [
                        {
                            "name": "a",
                            "type": "string",
                            "description": null
                        },
                        {
                            "name": "b",
                            "type": "string",
                            "description": null
                        }
                    ],
                    "_producer": "meltano",
                    "_schemaURL": "https://example.com/schema"
                }
            }
        }
    ],
    "outputs": [
      {
      "namespace": "snowflake://zy12345.azure-northeurope/my-db.my-schema",
      "name": "numbers",
      "facets": {
          "schema": {
              "fields": [
                  {
                      "name": "a",
                      "type": "string",
                      "description": null
                  },
                  {
                      "name": "b",
                      "type": "string",
                      "description": null
                  }
              ],
              "_producer": "meltano",
              "_schemaURL": "https://example.com"
          }
      }
  }
],
    "producer": "https://meltano.com"
}
```

This makes sense, but creating a full lineage record is likely to impose some overhead in the form of abstractions.


## The general idea: Event-based lineage
Because Prefect doesn't have a DAG, and DAGs alone are not sufficient for lineage purposes anyways because table references may be provided as input, we want a different approach. I propose to focus on capturing "events" such as a table being written to or read from.

In order to make this transparent, we can hook into the functionality in Blocks. Blocks are useful for containing connection info like database usernames and passwords, but they are python classes and can contain a lot of logic that developers simply using the block does not need to contend with.

The example `SnowflakeLineage` block is a proof-of-concept for such a block. On it's face, it looks like a normal Snowflake Config block. You can set it up by specifying your snowflake account, username, password etc, and it has the trusty `execute()` method you might be used to if you have used any sqlalchemy-based DB connection. It also has a stand-in for pandas' `read_sql` method, so you can read from a database almost the same way you would do with normal pandas.

But under the hood, it does more than simply run and return a query. It analyzes the query, finds what tables are selected from or inserted to, finds the schema of these tables, and emits a metadata record showing the data sources the flow read from or wrote to.

When looking at all events eminating from your prefect flow, you will see the full picture of what the flow read, and what it wrote.


## Big-ticket items / vision
Hopefully the design choices will stabilize, and we can extend this example to other databases.


## Beyond the Snowflake example
The example with Snowflake is just an example - it should be simple to extend this to other SQL databases, but also more generally to other types of storage, APIs etc. The library is split in to a "core" block type named `LineageBlock` with some common functionality that can be inherited, and a `SnowflakeLineageBlock` particular to the Snowflake database.

The general philosophy is to create as thin wrappers as possible - adding this lineage should command as few code changes as possible. Therefore we use a pandas extension to write from pandas to Snowflake, and we reimplement the `execute` method from sqlalchemy.

When creating further extensions, the goal should probably be to model after existing popular libraries, such as paramiko or requests. Naturally, we can't reimplement all features, but we **can** implement the most common and important ones. And providing a method to surface the underlying object we are trying to mock might be a good backup.

Anyways: Design decisions are important. But while planning is everything, plans are useless.


## If you want to test this
Due to the raggedy shape of this repo, there are quite a few prerequisites.

1. First off, you need a snowflake account.
2. Secondly, you need Prefect.
3. You need a table in a schema to read from, and possibly to write back to.
4. You need to modify the flow code a little, since it reads a particular table name and does some aggregation.

First, register the block itself: `prefect block register --file snowflake_lineage_block.py`

Secondly, create an instance of the block either manually or by filling in below and running:

```py
from snowflake_lineage_block import SnowflakeLineageBlock

lb = SnowflakeLineageBlock(
    account="xy123.west-europe.azure",
    database="my_dwh",
    db_schema="my_schema",
    user="prefect_sys_user",
    password="Sup3r5ecret!",
    warehouse="COMPUTE_WH",
    role="DWH",
)

lb.save('ax', overwrite=True)
```

Then, it is time to edit the code in `flow.py`. If you are looking at this, you are probably more than capable to do that.

This has only been tested locally, I have no clue what happens if you run it remotely.

In any case, I'd actually be amazed if this works on the first attempt.