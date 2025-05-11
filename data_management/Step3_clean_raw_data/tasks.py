# tasks.py
from invoke import task

@task
def stage1(c):
    c.run("python3 clean_customer_data.py")

@task
def stage2(c):
    c.run("python3 clean_item_data.py")

@task
def stage3(c):
    c.run("python3 clean_vendor_data.py")

@task
def stage4(c):
    c.run("python3 clean_transaction_data.py")

@task(pre=[stage1, stage2, stage3, stage4])
def all(c):
    """Run scripts in sequence: 1→2→3"""
    c.run("python3 clean_purchase_order_data.py")