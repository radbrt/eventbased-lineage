# Event-based lineage with Prefect

#### ⚠️ This is a thinking-out-loud type of project ⚠️
no code here is ready for anything, but is intended to provide some proof-of-concepts and ideas.

There hasn't been a lot of focus on lineage lately. dbt has some lineage, data catalogues are able to parse lineage from query history etc, and increasingly also able to do column level lineage. This is all within the SQL paradigm, where CTAS or insert-into-from-select type statements means that lineage can be provided by parsing a single (albeit often complex) SQL statement.

Providing lineage for arbitrary python code is a lot harder, and it is unrealistic to expect as fine-grained lineage there as with SQL. But surely, _something_ can be done.

In photography, there is an expression "the best camera is the one you actually use". Something similar goes for lineage - if lineage requires a lot of abstraction overhead, it is less likely to be used. And so, it is of limited use. Therefore, the purpose here is to provide almost transparent abstractions for data lineage - in keeping with the prefect philosophy of just letting pepole write python.

## Existing lineage concepts
To my knowledge, the most mature (and available) lineage specifications are OpenLineage, OpenMetadata and DataHub lineage. All operate with "lineage records" that contains info about roughly 3 things:
- Input
- Output
- Job/run info

Because I am most familiar with OpenLineage, I will provide an example from there. Open Lineage actually provides a number of different record types, such as "start", "error", and here, "complete":

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


## Proposal: Event-based lineage
Because Prefect doesn't have a DAG, and DAGs alone are not sufficient for lineage purposes anyways because table references may be provided as input, we want a different approach. I propose to focus on capturing "events" such as a table being written to or read from.

In order to make this transparent, we can hook into the functionality in Blocks. Blocks are useful for containing connection info like database usernames and passwords, but they are python classes and can contain a lot of logic that developers simply using the block does not need to contend with.

The example `SnowflakeLineage` block is a proof-of-concept for such a block. On it's face, it looks like a normal Snowflake Config block. You can set it up by specifying your snowflake account, username, password etc, and it has the trusty `run_query()` method you might be used to if you have used the Snowflake block. Except not really, I haven't even checked but I'm sure it works differently. But it is there. For now that will have to be enough.

But under the hood, it does more than simply run and return a query. It analyzes the query, finds what tables are selected from or inserted to, finds the schema of these tables, records the flow run name and flow run ID, and emits a metadata record showing that this flow had lineage input/outputs.

Since one `run_query()` call emits (at most) one lineage event, these events have to be stitched together to provide a complete view. This is where we start talking about stuff that can't be fixed with a simple pull-request.

Fortunately, the Marquez API is fairly tolerant in what it accepts. We are able to send "partial" lineage to the API, and marquez will stitch it together. "Partial" lineage isn't really a term, but I use it to mean lineage that contains a job and run ID, and possibly some information on input/output. This is very close to the idea of event-based lineage.

Such a partial lineage record could look something like this:

```json
{
    "eventType": "RUNNING",
    "eventTime": "2023-04-10T13:44:25.546776+00:00",
    "job": {
        "namespace": "prefect",
        "name": "321c07ff-8295-4a0a-b916"
    },
    "run": {
        "runId": "1b1e18a8-4f4e-4737-9c70"
    },
    "inputs": [
        {
            "namespace": "prefect",
            "name": "snowflake://xy1234.west-europe.azure/dbthouse/landing/nkoder",
            "facets": {
                "schema": {
                    "fields": [
                        {
                            "name": "NAERK",
                            "type": "FLOAT"
                        },
                        {
                            "name": "NAERK_TEKST",
                            "type": "VARCHAR(16777216)"
                        }
                    ],
                    "_producer": "prefect",
                    "_schemaURL": "https://example.com"
                }
            }
        }
    ],
    "outputs": [],
    "producer": "https://prefect.io"
}
```

And, importantly, it can be followed by several other records that add information about "output" (tables written to), and run completion.

By default, the example block prints this data to the log, but you can provide a marquez endpoint in the config and it will post the lineage record there. There isn't an authentication option yet though, but that should be easy enough to add.

## Big-ticket items / vision

Marquez seems a little touchy with the lineage events that are produced, and if no "complete" record for the flow run is sent, the UI has seemed unstable (for whatever reason). Currently, I have added a method to the block that can be called to send a "completed" record, but this is not a good solution - if the flow fails it will remain open forever, and also even if everything works well it is one extra line of code that developers have to write, specific to lineage, and we want to avoid that.

The Prefect API probably contains enough information to help here, for instance on a flow fail event, an event will be created to reference that failure. We need to pick that up, and send a lineage record of type "error" to marquez.

As mentioned above, marquez is not the only lineage game in town. It just so happens to be the one I'm most familiar with, and it appears it is fairly compatible to the "lineage-event" way of thinking. Adapting something like this to other lineage systems is not something I have looked at.

Somewhat more immediately doable, there are a bunch of facets that can be populated. Things like row counts, quality metrics etc. Should be doable by adding some methods to the block - although we have to keep in mind that some of these operations can be very time-consuming.

## Beyond the Snowflake example
The example with Snowflake is just an example - it should be simple to extend this to other SQL databases, but also more generally to other types of storage, APIs etc. For some blocks/operations, it will probably hard to map the event to a specific event (read data, write data, etc), some events don't read or write at all (like running a `GRANT`), so we might want to add a "misc" type of reference that simply says "there is some connection here, but we can't map it properly to a table or a read/write operation. Also, events like "drop table" doesn't really map well onto current lineage concepts, and perhaps it shouldn't either. 

It might make sense to make an SDK from this, something like a `prefect-block-lineage` library with general classes that provide a lot of base functionality, and methods that can be overridden to suit particular sources.


## Issues with this current version of the code
There are plenty of bugs in the code - parsing the table references in the SQL only works if no DB or schema is specified. This is simply a bug. The code is there, but there is some issue.

Also, note that there are separate methods (wrappers) for writing pandas data frames to snowflake. The default pandas implementation simply passes an open connection to the `to_sql` method, which makes it hard to intercept. I have no clue if it is possible to improve this.

Because I haven't had time to argue with `SecreetStr` yet, the password to the snowflake DB is plaintext which it *really should not be*. Sorry not sorry. This is after all just proof-of-concept.


## If you want to test this
Due to the raggedy shape of this repo, there are quite a few prerequisites.

1. First off, you need a snowflake account.
2. You need a table in a schema to read from, and possibly to write back to.
3. If you want to post lineage to marquez, you need a marquez instance. I'm running one locally on my machine with the marquez quickstart - which means the flow needs to run locally too.
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
    marquez_endpoint="http://localhost:5000/api/v1/lineage",
)

lb.save('ax', overwrite=True)
```

If you don't have a marquez endpoint, just comment out the `marquez_endpoint` argument and you'll be fine.

Then, it is time to edit the code in `flow.py`. If you are looking at this, you are probably more than capable to do that.

This has only been tested locally, I have no clue what happens if you run it remotely.

In any case, I'd actually be amazed if this works on the first attempt.