# from sync import Database, SyncUnit
# from sync import create_bond, sync
# from sqlalchemy import Table, update, column

# # Initiator
# init_db = db1
# init_SU = SU1
# # Supporter
# supp_db = db2
# supp_SU = SU2

# # Setup
# create_bond(init_db, init_SU, supp_db, supp_SU, table_name, keyword, separator)

# # Sync
# sync(init_db, init_SU, supp_db, supp_SU, table_name, SYNC_DELETE, HIGHER_PRIORITY_DELETE)

# # Modification
# # Target
# db = db1
# SU = SU1
# student = Table(table_name, db.meta, autoload= True)
# # Parameters
# pk_name = 'id'
# id_num = 1
# value = {'name': 'Jackie'}
# db.conn.execute(update(student, whereclause= column(pk_name) == id_num, values= value))
# SU.trigger_PF(db, pk_name, id_num)

# # Insert
# # Target
# db = db1
# SU = SU1
# student = Table(table_name, db.meta, autoload= True)
# sync_table = Table(SU.table_name, db.meta, autoload= True)
# # Parameters
# pk_name = 'id'
# id_num = 11
# value = {pk_name: id_num, 'name': 'Tom', 'gender': 'M'}
# db.conn.execute(insert(student, values= value))
# value = {pk_name: id_num}
# db.conn.execute(insert(sync_table, values= value))
# SU.trigger_PF(db, pk_name, id_num)

# # Delete
# # Target
# db = db1
# SU = SU1
# student = Table(table_name, db.meta, autoload= True)
# sync_table = Table(SU.table_name, db.meta, autoload= True)
# # Parameters
# pk_name = 'id'
# id_num = 11
# db.conn.execute(delete(student, whereclause= column(pk_name) == id_num))
# db.conn.execute(delete(sync_table, whereclause= column(pk_name) == id_num))

# # Initiator
# init_db = db1
# init_SU = SU1
# # Supporter
# supp_db = db2
# supp_SU = SU2
# # Sync
# sync(init_db, init_SU, supp_db, supp_SU, table_name, SYNC_DELETE, HIGHER_PRIORITY_DELETE)

# # Supporter
# supp_db = db3
# supp_SU = SU3
# # Sync
# sync(init_db, init_SU, supp_db, supp_SU, table_name, SYNC_DELETE, HIGHER_PRIORITY_DELETE)

# # Supporter
# supp_db = db4
# supp_SU = SU4
# # Sync
# sync(init_db, init_SU, supp_db, supp_SU, table_name, SYNC_DELETE, HIGHER_PRIORITY_DELETE)

# # Initiator
# init_db = db2
# init_SU = SU2
# # Sync
# sync(init_db, init_SU, supp_db, supp_SU, table_name, SYNC_DELETE, HIGHER_PRIORITY_DELETE)

# # Supporter
# supp_db = db3
# supp_SU = SU3
# # Sync
# sync(init_db, init_SU, supp_db, supp_SU, table_name, SYNC_DELETE, HIGHER_PRIORITY_DELETE)

# # Initiator
# init_db = db4
# init_SU = SU4
# # Sync
# sync(init_db, init_SU, supp_db, supp_SU, table_name, SYNC_DELETE, HIGHER_PRIORITY_DELETE)

