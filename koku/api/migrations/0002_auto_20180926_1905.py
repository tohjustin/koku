# Generated by Django 2.1 on 2018-09-26 19:05

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='provider',
            name='type',
            field=models.CharField(choices=[('AWS', 'AWS'), ('OCP', 'OCP'), ('AWS-local', 'AWS-local'), ('OCP-local', 'OCP-local')], default='AWS', max_length=50),
        ),
    ]