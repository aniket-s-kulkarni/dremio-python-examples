import jaydebeapi
import adbc_driver_flightsql as adbc
from adbc_driver_flightsql import dbapi
from pyarrow import flight
from http import cookies

import typing
import enum
import pathlib


class DriverType(enum.Enum):
    JDBC = 'jdbc:dremio:direct='
    ADBC = "grpc+tls://"


class Region(enum.Enum):
    NA = ""
    EU = "eu."


class ConnectionParams(dict):
    def __init__(self, uri: str, **kw):
        self.uri = uri
        super().__init__(**kw)

    def connect(self) -> typing.Union[jaydebeapi.Connection, dbapi.Connection]:
        pass


class JdbcConnectionParams(ConnectionParams):
    """
    Connect using dremio legacy JDBC driver
    """

    class TokenType(enum.Enum):
        PAT = "personal_access_token"
        JWT = "jwt"

    def __init__(self, region: Region, token: str, token_type: TokenType, project_id: str,
                 driver_jar: pathlib.Path, ssl=True, disableVerification=False):
        uri = f'{DriverType.JDBC.value}sql.{region.value}dremio.cloud:443'
        if not driver_jar.is_file():
            raise FileNotFoundError(driver_jar)
        self.jar = driver_jar
        super().__init__(uri, username='$token', password=token, ssl='true' if ssl else 'false',
                         disableCertificateVerification='true' if disableVerification else 'false',
                         token_type=token_type.value, PROJECT_ID=project_id)

    def classname(self):
        return 'com.dremio.jdbc.Driver'

    def connect(self) -> jaydebeapi.Connection:
        return jaydebeapi.connect(self.classname(), self.uri, self, jars=[str(self.jar)])

    def set_socks_proxy(self, host: str, port: int, username: str = None, password: str = None):
        self['socksProxyHost'] = host
        self['socksProxyPort'] = port
        if username is not None:
            self['socksProxyUsername'] = username
            if password is not None:
                self['socksProxyPassword'] = password

class AdbcConnectionParams(ConnectionParams):
    """
    Connect using apache ADBC
    """
    def __init__(self, region: Region, pat: str, ssl=True, disableVerification=False, project_id=None):
        uri = f'{DriverType.ADBC.value}'
        if not ssl:
            uri = uri.replace('+tls', '')
        uri = f'{uri}data.{region.value}dremio.cloud:443'
        kw = {
            adbc.DatabaseOptions.AUTHORIZATION_HEADER.value: f'Bearer {pat}',
            adbc.DatabaseOptions.TLS_SKIP_VERIFY.value: "false" if disableVerification else "true",
            f'{adbc.DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}useEncryption': "true" if ssl else "false",
        }
        if project_id:
            cookie = f'{adbc.DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}Cookie'
            kw[cookie] = f'project_id={project_id}'
            print(f'k = {cookie}, v = {kw[cookie]}')
        super().__init__(uri, **kw)

    def connect(self) -> dbapi.Connection:
        return dbapi.connect(self.uri, self)