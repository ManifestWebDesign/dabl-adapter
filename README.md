# DABL Adapter
Subclasses of PHP PDO for various SQL dialects

## Example

code:
```php
use Dabl\Adapter\DABLPDO;

// get connection (parameters usually come from .ini file)
$pdo = DABLPDO::connect(array(
    'driver' => 'mysql',
    'host' => 'localhost',
    'dbname' => 'test',
    'user' => 'root',
    'password' => ''
));

// Get database schema
$schema = $pdo->getDatabaseSchema();
$tables = $schema->getTables();
$columns = $table[0]->getColumns();
$type = $columns[0]->getType();
```


## Features

* Normalized constructor
* Read schema schemas (thanks to modified Propel libraries)
* Date and time helper methods (getTimestampFormatter, hourStart, dayStart, weekStart, monthStart)
* Helper methods for quoting arrays of parameters and identifier quoting
* Helper methods for applying LIMIT behavior

## Supported Dialects

* Microsoft Access
* Microsoft SQL Server
* MySQL
* Oracle
* Postgres
* Redshift
* SQLite