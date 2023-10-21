"namebench.py: perf test nameservers."

VERSION='0.01b'

import os
import sys
import time
import queue
import random
import pathlib
import threading

from concurrent.futures import ThreadPoolExecutor

from loguru import logger

import dns.message
import dns.rdataclass
import dns.rdatatype
import dns.query
import click
from rich import print
from rich.pretty import pprint
from rich.console import Console
from rich.table import Table

logger.remove()
logger.add(sys.stderr, level="INFO")

class Domains:

    def __init__(self, filename):
        try:
            with open(filename) as src:
                self.domains = [l.strip() for l in src.readlines()]
        except Exception as e:
            print(f'{filename=} not found', e)

    def __len__(self):
        return len(self.domains)

    def __iter__(self):
        yield from self.domains

    def __repr__(self):
        return f'<Domains {len(self.domains)}>'

class Nameservers:

    def __init__(self, filename):
        try:
            with open(filename) as src:
                self.nameservers = [l.strip() for l in src.readlines()]
        except Exception as e:
            print(f'{filename=} not found', e)

    def __len__(self):
        return len(self.nameservers)

    def __iter__(self):
        yield from self.nameservers

    def __repr__(self):
        return f'<Nameservers {len(self.nameservers)}>'

def delayed(d=1):
    def a(f):
        def b(*a, **kw):
            time.sleep(random.randint(0,d))
            return f(*a, **kw)
        return b
    return a

def send(q, d, n, ch, tick=None):
    "Sends a NS query over UDP and enqueue it into `ch`."
    beg = time.time()
    r = dns.query.udp(q, n)
    rtt = time.time() - beg
    item = (d, n, r, rtt)
    ch.put(item)
    if tick:
        time.sleep(random.random())
        tick()
    logger.debug('queud {item}'.format(item=item))

#@delayed()
def query(domain):
    "Creates a NS query."
    time.sleep(random.random()/3)
    qname = dns.name.from_text(domain)
    return dns.message.make_query(qname, dns.rdatatype.NS)

def shuffled(seq):
    copy = list(seq)[:]
    random.shuffle(copy)
    return copy

def update_stats(history, stat):
    lo = None
    hi = None
    total = None
    for _, rtt in history:
        lo = min(lo, rtt) if lo else rtt
        hi = max(hi, rtt) if hi else rtt
        total = (total + rtt) if total else rtt
    avg = total / len(history)
    return {'min': lo, 'max': hi, 'avg': avg, 'count': stat['count']+1}

@click.command
@click.argument('domains', )
@click.argument('nameservers', )
@click.option('--debug', default=False)
def main(domains, nameservers, debug):

    print()
    print('[bold white]namebench[/bold white]', VERSION)
    print()

    ds = Domains(domains)
    ns = Nameservers(nameservers)

    print(len(ds), 'domains')
    print(len(ns), 'nameservers')

    print()

    if debug:
        logger.debug('{ds}'.format(ds=ds))
        logger.debug('{ns}'.format(ns=ns))

    # Queue((d,n,msg,rtt))
    answers = queue.Queue()

    from rich.progress import Progress

    with Progress() as progress:
        total = len(ds) * len(ns)
        qtask = progress.add_task("[cyan]Querying...", total=total)
        etask = progress.add_task("[green]Queuing...", total=total)
        stask = progress.add_task("[bold green]Summing...", total=total)

        tick = lambda: progress.update(etask, advance=1)
        with ThreadPoolExecutor(5) as executor:
            for n in ns:
                for d in shuffled(ds):
                    logger.debug('submit {d} to {n}'.format(d=d, n=n))
                    executor.submit(send, query(d), d, n, answers, tick)
                    progress.update(qtask, advance=1)

        lock = threading.Lock()
        store = {
            n:{
                'answers': [],  # [(msg, rtt)]
                'stats': {'min':None, 'max':None, 'avg':None, 'count':0}
            } for n in ns
        }

        while answers.not_empty:
            try:
                ans = answers.get(timeout=1)
                logger.debug('> {ans}'.format(ans=ans))
                d, n, msg, rtt = ans
                with lock:
                    bucket = store[n]
                    bucket['answers'].append((msg, rtt))
                    bucket['stats'] = update_stats(
                        bucket['answers'],
                        bucket['stats']
                    )
                progress.update(stask, advance=1)
                answers.task_done()
            except queue.Empty:
                logger.debug('empty')
                break

        answers.join()

    print()

    table = Table(title="namebench")

    table.add_column("ns", justify="right", style="magenta", no_wrap=True)
    table.add_column("min", style="cyan")
    table.add_column("max", style="purple")
    table.add_column("avg", style="yellow")
    table.add_column("count", justify="right", style="green")

    for n in store:
        s = store[n]["stats"]
        table.add_row(n, str(s['min']), str(s['max']), str(s['avg']), str(s['count']))

    console = Console()
    console.print(table)

    print()
    print('[bold]bye[/bold]')
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('user quit.')
