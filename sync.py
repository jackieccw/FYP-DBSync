from random import randint
from sqlalchemy import create_engine, MetaData, Table, Column, Integer
from sqlalchemy import inspect
from sqlalchemy import table, column, text
from sqlalchemy import select, insert, update, delete
# from alembic.migration import MigrationContext
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations

### Object ###
# Database
# Consist of connection path, engine, connection, metadata
class Database:
    def __init__(self, path):
        self.path = path
        self.engine = create_engine(path)
        self.conn = self.engine.connect()
        self.meta = MetaData(bind = self.engine)

# Sync Unit
# Consist of code, name, id, priority, sync table name
# Each sync unit represent each table to be synchronized in a database
# A database can have multiple sync unit (table to be synchronized)
class SyncUnit:
    def __init__(self, engine, table_name, keyword, separator):
        self.reset(engine, table_name, keyword, separator)

    def reset(self, engine, table_name, keyword, separator):
        # Initialization
        self.table_name = self.code = self.name = self.id = self.priority = None

        # Search for sync table
        search_word = table_name + separator + keyword + separator
        for s in engine.table_names():
            index = s.find(search_word)
            # If sync table does not exist
            # Variables below remains null
            if index >= 0:
                self.table_name = s
                self.code = s[len(search_word):]
                words = self.code.split(separator)
                self.name = words[0]
                self.id = words[1]
                self.priority = int(words[2])
                break
        
    # Trigger PF to be its priority value
    # Use when data is modified
    def trigger_PF(self, db, pk_name, pk):
        sync_table = Table(self.table_name, db.meta, autoload= True)
        value = {}
        for column_name in sync_table.c.keys():
            value[column_name] = self.priority
        del value[pk_name]
        db.conn.execute(update(sync_table, values= value, whereclause= column(pk_name) == pk))


### Function ###
# Display error messages and exit
def error_exit(message):
    print('\n*** Error *** : ' + message + '\n')
    exit()

## Bonding ##
# Generate code for sync unit
def generate_code(path, table_name, separator, name = None, priority = None):
    print('\nDatabase Path: ' + path)
    print('Table Name: ' + table_name)
    # Prompt for a unique name for database
    if name == None:
        print('>> Please provide a unique name for each sync unit <<')
        name = input('Sync Unit Name: ')
    # Randomly generate a 4 digit code to reduce coincidence of name
    id_ = ''
    for _ in range(0, 4):
        id_ = id_ + str(randint(0,9))
    # Prompt for priority
    if priority == None:
        priority = input('Sync Unit Priority: ')
    # Combine name, id, priority to form a code
    code = name + separator + id_ + separator + str(priority)
    print('Sync Unit Code Generated: ' + code)
    return code

def get_pk_name(db, table_name):
    # Get all primary keys
    pks = inspect(db.engine).get_pk_constraint(table_name).get('constrained_columns')
    # Check existence of primary key
    if not pks:
        error_exit(table_name + ' table has no any primary key.')
    # Default primary key: id
    if 'id' in pks:
        pk_name = 'id'
    # Else, the first primary key in list
    else:
        pk_name = pks[0]
        print('"' + pk_name + '" is chosen as the primary key for synchronizing.\n')
    return pk_name

# Create sync table
def create_sync_table(db, SU, table_name, pk_name, keyword, separator, name = None, priority = None):
    print('Creating sync table...')
    ori_table = Table(table_name, db.meta, autoload= True)
    code = generate_code(db.path, table_name, separator, name, priority)
    sync_table_name = table_name + separator + keyword + separator + code
    # Copy the definition of column of the chosen primary key
    sync_table = Table(sync_table_name, db.meta, ori_table.c[pk_name].copy())
    sync_table.metadata.create_all(db.engine)
    # Reset the sync unit
    print('Resetting sync unit...')
    SU.reset(db.engine, table_name, keyword, separator)

    # Copy the chosen column of primary key to the sync table
    print('Replicating primary key...')
    pks = db.conn.execute(select(from_obj= table(table_name), columns= [column(pk_name)]))
    sync_table = Table(sync_table_name, db.meta, autoload= True)
    for pk in pks:
        db.conn.execute(insert(sync_table, values= dict(pk)))
    print('Creation completed.\n')

# Create operation for Alembic
def create_op(connection):
    context = MigrationContext.configure(connection)
    operation = Operations(context)
    return operation

# Add new column to sync table
def add_bond(db, sync_table_name, code):
    print('Creating bond with ' + code + ' in ' + sync_table_name + '...')
    op = create_op(db.conn)
    op.add_column(sync_table_name, Column(code, Integer, server_default= text('0')))
    db.meta = MetaData(bind= db.engine)
    print('Bonding completed.')

# Add bond and duplicate PF of referral
def bond_referral(dest_db, dest_sync_table, dest_sync_table_name, source_SU_code, new_SU_code, pk_name):
    print('Referring bond of ' + new_SU_code + ' from ' + source_SU_code + ' in ' + dest_sync_table_name + '...')
    add_bond(dest_db, dest_sync_table_name, new_SU_code)
    statement = select(from_obj= table(dest_sync_table_name), columns= [column(pk_name), column(source_SU_code)], whereclause= column(source_SU_code) > 0)
    query = dest_db.conn.execute(statement)
    for row in query:
        pk = row[0]
        pf = row[1]
        statement = update(dest_sync_table, values= {new_SU_code: pf}, whereclause= column(pk_name) == pk)

# Check mutual existence of bond
def check_mutual_bond(init_SU, init_sync_table, supp_SU, supp_sync_table):
    init_cols_name = init_sync_table.c.keys()
    if supp_SU.code in init_cols_name:
        del init_cols_name[init_cols_name.index(supp_SU.code)]
    else:
        error_exit('Bond with Supporter SU does not exist in Initiator SU.\nPlease sync with a mutual SU or create a new bond.')
    supp_cols_name = supp_sync_table.c.keys()
    if init_SU.code in supp_cols_name:
        del supp_cols_name[supp_cols_name.index(init_SU.code)]
    else:
        error_exit('Bond with Initiator SU does not exist in Supporter SU.\nPlease sync with a mutual SU or create a new bond.')
    
    return init_cols_name, supp_cols_name

# Check for existence of new SU for bond referral
def check_bond_referral(init_db, init_SU, init_cols_name, supp_db, supp_SU, supp_cols_name, pk_name):
    init_sync_table = Table(init_SU.table_name, init_db.meta, autoload= True)
    supp_sync_table = Table(supp_SU.table_name, supp_db.meta, autoload= True)
    for _ in range(len(init_cols_name)):
        SU_code = init_cols_name.pop(0)
        if SU_code in supp_cols_name:
            del supp_cols_name[supp_cols_name.index(SU_code)]
        else:
            bond_referral(supp_db, supp_sync_table, supp_SU.table_name, init_SU.code, SU_code, pk_name)
    for _ in range(len(supp_cols_name)):
        SU_code = supp_cols_name.pop(0)
        bond_referral(init_db, init_sync_table, init_SU.table_name, supp_SU.code, SU_code, pk_name)

# Create bond between sync unit
def create_bond(source_db, source_SU, dest_db, dest_SU, table_name, keyword, separator, source_SU_name = None, source_SU_priority = None, dest_SU_name = None, dest_SU_priority = None):
    # Check existence of table to be synchronized in source database
    if not source_db.engine.dialect.has_table(source_db.conn, table_name):
        error_exit('Table to be synchronized not existed in source database.')
    # Get primary key
    pk_name = get_pk_name(source_db, table_name)
    
    # Check existence of table to be synchronized in destination database
    if dest_db.engine.dialect.has_table(dest_db.conn, table_name):
        # Ask if the user wants to drop the table
        delete_dest_table = input('Table of same name existed in destination database.\nDo you want to delete it? (Y/N) ')
        if delete_dest_table == 'Y':
            delete_dest_table = input('This action will reset the sync unit.\nDo you confirm? (Y/N) ')
        if delete_dest_table == 'Y':
            # Drop table with the same name as table to be synchronized
            dest_table = Table(table_name, dest_db.meta, autoload= True)
            dest_table.drop()
        else:
            error_exit('Table of same name existed in destination database.')

    while True:
        # Drop sync table
        if not dest_SU.table_name == None:
            print(dest_SU.table_name)
            dest_sync_table = Table(dest_SU.table_name, dest_db.meta, autoload= True)
            dest_sync_table.drop()
            print('************DROP LIAO**********************')
            dest_SU.reset(dest_db.engine, table_name, keyword, separator)
        else:
            break
    
    print('Replicating table...')
    # Obtain table information
    source_table = Table(table_name, source_db.meta, autoload=True)
    # Create a same table in destination database
    source_table.metadata.create_all(dest_db.engine)
    dest_table = Table(table_name, dest_db.meta, autoload=True)
    # Copy all data from source database to destination database
    data = source_db.conn.execute(select([source_table]))
    for entry in data:
        dest_db.conn.execute(insert(dest_table, values= dict(entry)))
    print('Replication completed.\n')

    # Check existence of sync table in source database
    if source_SU.table_name == None:
        # Create if not existed
        create_sync_table(source_db, source_SU, table_name, pk_name, keyword, separator, source_SU_name, source_SU_priority)
    # Create sync table in destination database
    create_sync_table(dest_db, dest_SU, table_name, pk_name, keyword, separator, dest_SU_name, dest_SU_priority)

    # Create bond
    add_bond(source_db, source_SU.table_name, dest_SU.code)
    add_bond(dest_db, dest_SU.table_name, source_SU.code)

    source_sync_table = Table(source_SU.table_name, source_db.meta, autoload= True)
    dest_sync_table = Table(dest_SU.table_name, dest_db.meta, autoload= True)

    # Check mutual bond
    source_cols_name, dest_cols_name = check_mutual_bond(source_SU, source_sync_table, dest_SU, dest_sync_table)
    # Check for newly added SU for bond referral
    check_bond_referral(source_db, source_SU, source_cols_name, dest_db, dest_SU, dest_cols_name, pk_name)


## Sync ##
# Propagation completion flag
# Set PF = -1 as an intermediate value
# Only set to 0 after synchronization is done
def set_propag_flag(first_db, first_sync_table, first_SU_code, second_db, second_sync_table, second_SU_code, pk_name, pk):
    statement = update(first_sync_table, values= {second_SU_code: -1}, whereclause= column(pk_name) == pk)
    first_db.conn.execute(statement)
    statement = update(second_sync_table, values= {first_SU_code: -1}, whereclause= column(pk_name) == pk)
    second_db.conn.execute(statement)

# Synchronization completion
# Set PF = 0
def clear_propag_flag(first_db, first_sync_table, first_SU_code, second_db, second_sync_table, second_SU_code):
    first_statement = update(first_sync_table, values= {second_SU_code: 0}, whereclause= column(second_SU_code) == -1)
    first_db.conn.execute(first_statement)
    second_statement = update(second_sync_table, values= {first_SU_code: 0}, whereclause= column(first_SU_code) == -1)
    second_db.conn.execute(second_statement)

# Determine whether to delete
# Based on user configurable rule
def to_delete(content_priority, empty_priority, SYNC_DELETE, HIGHER_PRIORITY_DELETE):
    if SYNC_DELETE:
        if HIGHER_PRIORITY_DELETE:
            if content_priority > empty_priority:
                delete_it = False
            else:
                delete_it = True
        else:
            delete_it = True
    else:
        delete_it = False
    return delete_it

# Delete row
def delete_entry(db, ori_table, sync_table, pk_name, pk):
    # Original table
    statement = delete(ori_table, whereclause= column(pk_name) == pk)
    db.conn.execute(statement)
    # Sync table
    statement = delete(sync_table, whereclause= column(pk_name) == pk)
    db.conn.execute(statement)

# Copy newly added entry or restore deleted row from other SU
def copy_entry(source_db, source_table, source_sync_table, source_SU_code, dest_db, dest_table, dest_sync_table, dest_SU_code, pk_name, pk):
    # Original table
    source_statement = select([source_table], whereclause= column(pk_name) == pk)
    value = dict(source_db.conn.execute(source_statement).fetchone())
    dest_statement = insert(dest_table, values= value)
    dest_db.conn.execute(dest_statement)
    # Sync table
    source_statement = select([source_sync_table], whereclause= column(pk_name) == pk)
    value = dict(source_db.conn.execute(source_statement).fetchone())
    del value[dest_SU_code]
    dest_statement = insert(dest_sync_table, values= value)
    dest_db.conn.execute(dest_statement)
    # Set PF = -1
    set_propag_flag(source_db, source_sync_table, source_SU_code, dest_db, dest_sync_table, dest_SU_code, pk_name, pk)

# Check spread
# Calculate number of columns with value of zero along the row
def check_spread(db, check_table, pk_name, pk):
    spread = 0
    statement = select([check_table], whereclause= column(pk_name) == pk)
    query = db.conn.execute(statement).fetchone()
    for propag_factor in query:
        if not propag_factor:
            spread = spread + 1
    return spread

# Propagate data from source to destination
# Update PF accordingly
def propag(source_db, source_SU, source_table, source_sync_table, dest_db, dest_SU, dest_table, dest_sync_table, pk_name, pk):
    # Original Table (Data)
    # Select from source and update to destination
    source_statement = select([source_table], whereclause= column(pk_name) == pk)
    value = dict(source_db.conn.execute(source_statement).fetchone())
    dest_statement = update(dest_table, values= value, whereclause= column(pk_name) == pk)
    dest_db.conn.execute(dest_statement)

    # Sync Table (PF)
    # Select from source and update to destination
    source_statement = select([source_sync_table], whereclause= column(pk_name) == pk)
    value = dict(source_db.conn.execute(source_statement).fetchone())
    # except PF of itself
    del value[dest_SU.code]
    dest_statement = update(dest_sync_table, values= value, whereclause= column(pk_name) == pk)
    dest_db.conn.execute(dest_statement)

    # PF = -1
    set_propag_flag(source_db, source_sync_table, source_SU.code, dest_db, dest_sync_table, dest_SU.code, pk_name, pk)

# Synchronization
def sync(init_db, init_SU, supp_db, supp_SU, table_name, SYNC_DELETE, HIGHER_PRIORITY_DELETE):
    # Initialize table object
    init_table = Table(table_name, init_db.meta, autoload= True)
    supp_table = Table(table_name, supp_db.meta, autoload= True)
    init_sync_table = Table(init_SU.table_name, init_db.meta, autoload= True)
    supp_sync_table = Table(supp_SU.table_name, supp_db.meta, autoload= True)
    
    # Check mutual bond
    init_cols_name, supp_cols_name = check_mutual_bond(init_SU, init_sync_table, supp_SU, supp_sync_table)
    
    # Check consistency of primary key
    pk_name = get_pk_name(init_db, table_name)
    if not pk_name == get_pk_name(supp_db, supp_SU.table_name):
        error_exit('Inconsistent primary key existed in initiator and supporter sync unit.')
    
    # Check for newly added SU for bond referral
    check_bond_referral(init_db, init_SU, init_cols_name, supp_db, supp_SU, supp_cols_name, pk_name)

    # Reset -1 to 0 in case of unexpected termination of synchronization from previous session
    clear_propag_flag(init_db, init_sync_table, init_SU.code, supp_db, supp_sync_table, supp_SU.code)

    # Initiator: PF > 0
    # Select all entries with PF > 0
    print('\nSynchronizing...')
    print('Initiator: Modified Data')
    init_statement = select(from_obj= table(init_SU.table_name), columns= [column(pk_name), column(supp_SU.code)], whereclause= column(supp_SU.code) > 0)
    init_query = init_db.conn.execute(init_statement)
    for row in init_query:
        pk = row[0]
        init_priority = row[1]
        supp_statement = select(from_obj= table(supp_SU.table_name), columns= [column(init_SU.code)], whereclause= column(pk_name) == pk)
        supp_priority = supp_db.conn.execute(supp_statement).fetchone()
        # For newly added entry
        if supp_priority == None:
            copy_entry(init_db, init_table, init_sync_table, init_SU.code, supp_db, supp_table, supp_sync_table, supp_SU.code, pk_name, pk)
        # For existing entry
        else:
            supp_priority = supp_priority[0]
            # Priority
            if init_priority > supp_priority:  
                init_win = True
            elif supp_priority > init_priority:
                init_win = False
            else:
                # Spread
                init_spread = check_spread(init_db, init_sync_table, pk_name, pk)
                supp_spread = check_spread(supp_db, supp_sync_table, pk_name, pk)
                if init_spread > supp_spread:
                    init_win = False
                # Initiator wins unconditionally
                else:
                    init_win = True
            # Conflict resolved
            # Copy data from winner to loser
            if init_win:
                propag(init_db, init_SU, init_table, init_sync_table, supp_db, supp_SU, supp_table, supp_sync_table, pk_name, pk)
            else:
                propag(supp_db, supp_SU, supp_table, supp_sync_table, init_db, init_SU, init_table, init_sync_table, pk_name, pk)
    
    # Supporter: PF > 0
    # Select all entries with PF > 0
    print('Supporter: Modified Data')
    supp_statement = select(from_obj= table(supp_SU.table_name), columns= [column(pk_name), column(init_SU.code)], whereclause= column(init_SU.code) > 0)
    supp_query = supp_db.conn.execute(supp_statement)
    for row in supp_query:
        pk = row[0]
        # Select dummy data
        init_statement = select(from_obj= table(init_SU.table_name), columns= [column(supp_SU.code)], whereclause= column(pk_name) == pk)
        # Just to check whether it is newly added entry
        if init_db.conn.execute(init_statement).fetchone() == None:
            copy_entry(supp_db, supp_table, supp_sync_table, supp_SU.code, init_db, init_table, init_sync_table, init_SU.code, pk_name, pk)
        # PF must be zero if not newly added entry
        else:
            propag(supp_db, supp_SU, supp_table, supp_sync_table, init_db, init_SU, init_table, init_sync_table, pk_name, pk)

    # Initiator: PF = 0
    # Select all entries with PF = 0
    print('Initiator: Unchanged Data')
    init_statement = select(from_obj= table(init_SU.table_name), columns= [column(pk_name)], whereclause= column(supp_SU.code) == 0)
    init_query = init_db.conn.execute(init_statement)
    for row in init_query:
        pk = row[0]
        # Select dummy data
        supp_statement = select(from_obj= table(supp_SU.table_name), columns= [column(init_SU.code)], whereclause= column(pk_name) == pk)
        # Just to check whether it was deleted
        if supp_db.conn.execute(supp_statement).fetchone() == None:
            if to_delete(init_SU.priority, supp_SU.priority, SYNC_DELETE, HIGHER_PRIORITY_DELETE):
                delete_entry(init_db, init_table, init_sync_table, pk_name, pk)
            else:
                copy_entry(init_db, init_table, init_sync_table, init_SU.code, supp_db, supp_table, supp_sync_table, supp_SU.code, pk_name, pk)
        # Exsitence checked
        else:
            set_propag_flag(init_db, init_sync_table, init_SU.code, supp_db, supp_sync_table, supp_SU.code, pk_name, pk)
    
    # Supporter: PF = 0
    # Select all entries with PF = 0
    print('Supporter: Unchanged Data')
    supp_statement = select(from_obj= table(supp_SU.table_name), columns= [column(pk_name)], whereclause= column(init_SU.code) == 0)
    supp_query = supp_db.conn.execute(supp_statement)
    for row in supp_query:
        pk = row[0]
        # Select dummy data
        init_statement = select(from_obj= table(init_SU.table_name), columns= [column(supp_SU.code)], whereclause= column(pk_name) == pk)
        # Just to check whether it was deleted
        if init_db.conn.execute(init_statement).fetchone() == None:
            if to_delete(supp_SU.priority, init_SU.priority, SYNC_DELETE, HIGHER_PRIORITY_DELETE):
                delete_entry(supp_db, supp_table, supp_sync_table, pk_name, pk)
            else:
                copy_entry(supp_db, supp_table, supp_sync_table, supp_SU.code, init_db, init_table, init_sync_table, init_SU.code, pk_name, pk)
        # Exsitence checked
        else:
            set_propag_flag(init_db, init_sync_table, init_SU.code, supp_db, supp_sync_table, supp_SU.code, pk_name, pk)
    
    # Synchronization completed
    # Reset PF = 0
    clear_propag_flag(init_db, init_sync_table, init_SU.code, supp_db, supp_sync_table, supp_SU.code)
    print('Synchronization completed.\n')