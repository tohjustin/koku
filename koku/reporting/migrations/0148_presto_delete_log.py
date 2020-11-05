# Generated by Django 3.1.2 on 2020-10-30 19:22
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [("reporting", "0147_auto_20201028_1305")]

    operations = [
        migrations.RunSQL(
            """
create table if not exists presto_delete_wrapper_log (
    id uuid primary key default public.uuid_generate_v4(),
    action_ts timestamptz not null default now(),
    table_name text not null,
    where_clause text not null,
    result_rows bigint
);
"""
        ),
        migrations.RunSQL(
            """
drop trigger if exists tr_presto_before_insert on presto_delete_wrapper_log ;
create trigger tr_presto_before_insert
before insert on presto_delete_wrapper_log
   for each row execute function public.tr_presto_delete_wrapper_log_action();
"""
        ),
    ]