# Generated by Django 2.1.5 on 2019-03-01 18:50

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('rates', '0003_auto_20190213_2040'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='rate',
            options={'ordering': ['-id']},
        ),
    ]
