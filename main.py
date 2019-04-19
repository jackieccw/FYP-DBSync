from sync import Database, SyncUnit
from sync import create_bond, sync
from sqlalchemy import Table, column, insert, update, delete

# User Configurable Rule
SYNC_DELETE = True
HIGHER_PRIORITY_DELETE = True

# Initialize paramaters
table_name = 'student'
keyword = 'sync'
separator = '_'
# Initialize database
db1 = Database('sqlite:///1/test.db')
db2 = Database('sqlite:///2/test.db')
db3 = Database('sqlite:///3/test.db')
db4 = Database('sqlite:///4/test.db')
# Initialize sync unit
SU1 = SyncUnit(db1.engine, table_name, keyword, separator)
SU2 = SyncUnit(db2.engine, table_name, keyword, separator)
SU3 = SyncUnit(db3.engine, table_name, keyword, separator)
SU4 = SyncUnit(db4.engine, table_name, keyword, separator)
