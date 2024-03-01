# Readme

This repository is a collection of python based examples that connect to and interact with Dremio. The examples support two main ways to connect to Dremio

1. [JDBC driver (legacy)](https://docs.dremio.com/cloud/sonar/client-apps/drivers/jdbc/)
2. [Apache ADBC driver](https://arrow.apache.org/adbc/main/faq.html#what-exactly-is-adbc)
  - this uses apache flight

## Prerequisites

1. **Python version**: Python version >= 3.0 is required to use these examples
2. **Libraries**: [jaydebeapi](https://pypi.org/project/JayDeBeApi/) and [apache adbc](https://arrow.apache.org/adbc/main/python/driver_manager.html#installation) are the two prerequisites. To install them run - 

```sh
python3 -m pip install -r requirements.txt
```

## Connecting to Dremio

### JDBC

1. Download the [JDBC driver (legacy)](https://docs.dremio.com/cloud/sonar/client-apps/drivers/jdbc/) jar. 
2. Connection example - 

Ensure that either [a PAT](https://docs.dremio.com/cloud/security/authentication/personal-access-token/#creating-a-token) has been generated or [a JWT](https://docs.dremio.com/cloud/security/app-authentication/external-token/) from an external authentication provider is available

```python
import connection
import pathlib

params = connection.JdbcConnectionParams(
    connection.Region.NA, # for North America, or EU for EMEA
    token = ...., token_type = connection.JdbcConnectionParams.TokenType.PAT, # for PAT
    project_id = ....,
    driver_jar = pathlib.Path(....) # path to the jdbc jar file
)

# optionally other parameters can be set like socks proxy etc.
params.set_socks_proxy(host, port, ...)

# connect and interact w/ Dremio
connection = params.connect()
with connection.cursor() as cursor:
    while row := cursor.execute(....):
        .... # use row
```