# Generated by Django 2.2.12 on 2020-07-14 18:16
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [("reporting_common", "0024_remove_costusagereportmanifest_num_processed_files")]

    operations = [migrations.RemoveField(model_name="costusagereportmanifest", name="task")]
