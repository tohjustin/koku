# Generated by Django 2.1.2 on 2018-10-12 17:24

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('reporting', '0007_awscostentrybill_provider_id'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='awscostentrybill',
            unique_together={('bill_type', 'payer_account_id', 'billing_period_start')},
        ),
    ]