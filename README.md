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

This makes sense, but doesn't work as well with transparent-ish lineage in Prefect.


## Proposal: Event-based lineage
Because Prefect doesn't have a DAG, and DAGs alone are not sufficient for lineage purposes anyways because table references may be provided as input, we want a different approach. I propose to focus on capturing "events" such as a table being written to or read from.

In order to make this transparent, we can hook into the functionality in Blocks. Blocks are useful for containing connection info like database usernames and passwords, but they are python classes and can contain a lot of logic that developers simply using the block does not need to contend with.

The example `SnowflakeLineage` block is a proof-of-concept for such a block. On it's face, it looks like a normal Snowflake Config block. You can set it up by specifying your snowflake account, username, password etc, and it has the trusty `run_query()` method you might be used to if you have used the Snowflake block.

But under the hood, it does more than simply run and return a query. It analyzes the query, finds what tables are selected from or inserted to, finds the schema of these tables, records the flow run name and flow run ID, and emits a metadata record showing that this flow had lineage input/outputs.

Since one `run_query()` call emits (at most) one lineage event, these events have to be stitched together to provide a complete view. This is where we start talking about stuff that can't be fixed with a simple pull-request.

A skeleton lineage event (of the type that gets produced today) looks something like this:

```json
{
    "flow_run_name": "extatic-capybara",
    "flow_run_id": "ce56d418-d32c-4602-965a-4194914068c5",
    "inputs": [
        {
            "table": "nkoder",
            "schema": [
                {
                    "name": "NAERK",
                    "type": "FLOAT"
                },
                {
                    "name": "NAERK_TEKST",
                    "type": "VARCHAR(16777216)"
                }
            ]
        }
    ],
    "outputs": []
}
```

It is, of course, simple to add info like account name, db name, schema etc so that the lineage becomes more complete.

## Big-ticket items / Vision
Currently, these "lineage events" are simply printed and appears in the log. But they should really be sent somewhere where they can be collected, assembled and used to populate a proper observability tool (such as Marquez). Alternatively, and more elegant, a lineage tool native to event-based lineage would be able to display that lineage events without any record of the flow ending are probably still running.

In order to truly assemble this information in a meaningful way, we need to add information about the flow run itself - if for no other reason then to make sure the flow is actually finished before we start assembling the lineage and feeding it into Marquez (or some other destination of your choice).

No worries, this information is available through the prefect API, both the Events endpoint and the Flows endpoint should contain sufficient information.


## Issues with this current version of the code
As mentioned above, the lineage event currently doesn't contain DB/Schema/account information. We want to create a table/namespace specification similar to that of the OpenLineage example above, where, essentially, we can uniquely identify a table with something like `snowflake://xy12345.azure-northeurope/db1/schema1/table1`.

Also, there are plenty of bugs in the code - parsing the table references in the SQL only works if no DB or schema is specified. This is simply a bug. The code is there, but there is some issue.

Also, note that there are separate methods (wrappers) for writing pandas data frames to snowflake. The default pandas implementation simply passes an open connection to the `to_sql` method, which makes it hard to intercept. I have no clue if it is possible to improve this.

Because I haven't had time to argue with `SecreetStr` yet, the password to the snowflake DB is plaintext which it *really should not be*. Sorry not sorry. This is after all just proof-of-concept.

The event example above includes the flow run name, but this is only included if the block is invoked in the flow. The name does not seem to be accessible within a task. It is still possible to stitch together via the flow ID so it is probably not a big problem.


## Beyond the Snowflake example
The example with Snowflake is just an example - it should be simple to extend this to other SQL databases, but also more generally to other types of storage, APIs etc. For some blocks/operations, it will probably hard to map the event to a specific event (read data, write data, etc), some events don't read or write at all (like running a `GRANT`), so we might want to add a "misc" type of reference that simply says "there is some connection here, but we can't map it properly to a table or a read/write operation. Also, events like "drop table" doesn't really map well onto current lineage concepts, and perhaps it shouldn't either. 

It might make sense to make an SDK from this, something like a `prefect-block-lineage` library with general classes that provide a lot of base functionality, and methods that can be overridden to suit particular sources.


