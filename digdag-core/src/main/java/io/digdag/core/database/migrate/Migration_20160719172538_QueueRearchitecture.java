package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160719172538_QueueRearchitecture
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        // queued_task_locks
        handle.update("drop table queued_task_locks");
        handle.update("drop table queued_shared_task_locks");

        handle.update(
                context.newCreateTableBuilder("queued_task_locks")
                .addLongId("id")  // references queued_tasks.id
                .addInt("site_id", "")
                .addInt("queue_id", "")
                .addInt("priority", "not null")
                .addInt("retry_count", "not null default 0")
                .addLong("lock_expire_time", "")
                .addString("lock_agent_id", "")
                .build());

        handle.update("insert into queued_task_locks" +
                " (id, site_id, queue_id, priority)" +
                " select id, site_id, NULL, priority" +
                " from queued_tasks");

        // queues
        handle.update("alter table queues" +
                " add column shared_site_id int");

        // resource_types
        handle.update("drop table resource_types");

        // queued_tasks
        handle.update("alter table queued_tasks" +
                " alter column task_id drop not null");
        handle.update("alter table queued_tasks" +
                " alter column queue_id drop not null");
        handle.update("alter table queued_tasks" +
                " alter column data drop not null");
        handle.update("alter table queued_tasks" +
                " drop column priority");
        handle.update("alter table queued_tasks" +
                " drop column resource_type_id");
        handle.update("create unique index queued_tasks_on_site_id_task_id on queued_tasks (site_id, task_id)");

        if (context.isPostgres()) {
            handle.update(
                "CREATE FUNCTION lock_shared_tasks(target_site_id int, target_site_max_concurrency bigint, limit_count int, lock_expire_seconds int, agent_id text) returns setof bigint as $$\n" +
                "BEGIN\n" +
                "  IF pg_try_advisory_xact_lock(23300, target_site_id) THEN\n" +
                "    RETURN QUERY\n" +
                "      with updated as (\n" +
                "        update queued_task_locks\n" +
                "        set lock_expire_time = cast(extract(epoch from statement_timestamp()) as bigint) + lock_expire_seconds,\n" +
                "            lock_agent_id = agent_id\n" +
                "        where id = any(\n" +
                "          select queued_task_locks.id\n" +
                "          from queued_task_locks\n" +
                "          where lock_expire_time is null\n" +
                "          and site_id = target_site_id\n" +
                "          and not exists (\n" +
                "            select * from (\n" +
                "              select queue_id, count(*) as count\n" +
                "              from queued_task_locks\n" +
                "              where lock_expire_time is not null\n" +
                "                and site_id = target_site_id\n" +
                "              group by queue_id\n" +
                "            ) runnings\n" +
                "            join queues on queues.id = runnings.queue_id\n" +
                "            where runnings.count >= queues.max_concurrency\n" +
                "              and runnings.queue_id = queued_task_locks.queue_id\n" +
                "          )\n" +
                "          and not exists (\n" +
                "            select count(*)\n" +
                "            from queued_task_locks\n" +
                "            where lock_expire_time is not null\n" +
                "              and site_id = target_site_id\n" +
                "            having count(*) >= target_site_max_concurrency\n" +
                "          )\n" +
                "          order by queue_id, priority desc, id\n" +
                "          limit limit_count\n" +
                "        )\n" +
                "        returning queue_id, priority, id\n" +
                "      )\n" +
                "      select id from updated\n" +
                "      order by queue_id, priority desc, id;\n" +
                "  END IF;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql VOLATILE\n" +
            "");

            handle.update("create index queued_tasks_shared_grouping on queued_task_locks (site_id, queue_id) where site_id is not null and lock_expire_time is not null");
            handle.update("create index queued_tasks_ordering on queued_task_locks (site_id, queue_id, priority desc, id) where lock_expire_time is null");
            handle.update("create index queued_tasks_expiration on queued_task_locks (lock_expire_time) where lock_expire_time is not null");
        }
        else {
            handle.update("create index queued_tasks_shared_grouping on queued_task_locks (lock_expire_time, site_id, queue_id)");
            handle.update("create index queued_tasks_ordering on queued_task_locks (site_id, queue_id, lock_expire_time, priority desc, id)");
        }
    }
}
