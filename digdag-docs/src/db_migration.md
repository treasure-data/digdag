# Upgrade with database migration

## 1. Automatic migration

Digdag supports automatic migration when newer version requires database schema modification.
When a digdag server start, it check `schema_migrations` table and execute each migration sequentially and automatically.
This is default enable, but you can enable/disable with system parameter `database.migrate`.
This parameter is documented from 0.9.36

## 2. Change of migration behavior from 0.9.36
Each migration is executed with transaction. So if migrations fail, you can fix the cause and then retry it. 
From 0.9.36, we added non-transactional migration. This is because some DDL cannot run in transaction.
In such migration, if the migration fail, there is a possibility to be required fix by hand before you retry. 
So we explain how to recover when a non-transactional migration fails in section 4.

## 3. How to upgrade Digdag safely
If you are running a Digdag cluster in production, we recommend the following way to avoid trouble.

1. Disable auto migration:  `database.migrate: false`
1. Stop all Digdag servers.
1. Upgrade Digdag binary.
1. Check migrations by `digdag migrate check` and confirm which migrations will be applied.
1. Run cli `digdag migrate run` in a server.
1. Start Digdag servers.

## 4. List of non-transactional migrations
### ver. 0.9.36
#### 20190318175338
This migration add an index to session_attempts to avoid performance degrade with large session_attempts.
If you want to retry this migration because of unexpected errors, please run following sql.

    drop index session_attempts_on_site_id_and_state_flags_partial_2;
    delete from schema_migrations where name like '20190318175338';

