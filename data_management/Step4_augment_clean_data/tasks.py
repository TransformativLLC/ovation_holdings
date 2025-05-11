# tasks.py
from invoke import task

@task
def stage1(c):
    c.run("python3 augment_item_data.py")

@task
def stage2(c):
    c.run("python3 augment_purchase_order_data.py")

@task(pre=[stage1, stage2])
def all(c):
    """Run scripts in sequence: 1→2→3"""
    c.run("python3 augment_transaction_data.py")