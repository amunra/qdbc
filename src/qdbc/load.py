import typer
import questdb.ingress as qi
import numpy as np
import pandas as pd
import random
import time
import sys
from concurrent.futures import ThreadPoolExecutor, Future
from numba import vectorize, float64
import requests


class CpuTable:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def _request(self, sql):
        response = requests.get(
            f'http://{self.host}:{self.port}/exec',
            params={'query': sql}).json()
        return response

    def drop(self):
        response = self._request('drop table cpu')
        if response.get('ddl') == 'OK':
            print(f'Dropped table cpu')
            return True
        elif response.get('error', '').startswith('table does not exist'):
            print(f'Table cpu does not exist')
            return False
        else:
            raise RuntimeError(f'Failed to drop table cpu: {response}')

    def create(self):
        symbol_cols = [
            'hostname', 'region', 'datacenter', 'rack', 'os', 'arch',
            'team', 'service', 'service_version', 'service_environment']
        double_cols = [
            'usage_user', 'usage_system', 'usage_idle', 'usage_nice',
            'usage_iowait', 'usage_irq', 'usage_softirq', 'usage_steal',
            'usage_guest', 'usage_guest_nice']
        sql = f'''
            create table cpu (
                {', '.join(f'{col} symbol' for col in symbol_cols)},
                {', '.join(f'{col} double' for col in double_cols)},
                timestamp timestamp)
                    timestamp(timestamp)
                    partition by day
            '''
        response = self._request(sql)
        if response.get('ddl') == 'OK':
            print(f'Created table cpu')
        else:
            raise RuntimeError(f'Failed to create table cpu: {response}')

    def get_row_count(self):
        response = self._request('select count(*) from cpu')
        return response['dataset'][0][0]

    def block_until_rowcount(self, target_count, timeout=30.0):
        t0 = time.monotonic()
        while True:
            row_count = self.get_row_count()
            if row_count == target_count:
                return
            elif row_count > target_count:
                raise RuntimeError(
                    f'Row count {row_count} exceeds target {target_count}')
            if time.monotonic() - t0 > timeout:
                raise RuntimeError(
                    f'Timed out waiting for row count to reach {target_count}')
            time.sleep(0.1)


@vectorize([float64(float64, float64)])
def _clip_add(x, y):
    z = x + y
    # Clip to the 0 and 100 boundaries
    if z < 0.0:
        z = 0.0
    elif z > 100.0:
        z = 100.0
    return z


_REGIONS = {
    "us-east-1": [
        "us-east-1a",
        "us-east-1b",
        "us-east-1c",
        "us-east-1e"],
    "us-west-1": [
        "us-west-1a",
        "us-west-1b"],
    "us-west-2": [
        "us-west-2a",
        "us-west-2b",
        "us-west-2c"],
    "eu-west-1": [
        "eu-west-1a",
        "eu-west-1b",
        "eu-west-1c"],
    "eu-central-1": [
        "eu-central-1a",
        "eu-central-1b"],
    "ap-southeast-1": [
        "ap-southeast-1a",
        "ap-southeast-1b"],
    "ap-southeast-2": [
        "ap-southeast-2a",
        "ap-southeast-2b"],
    "ap-northeast-1": [
        "ap-northeast-1a",
        "ap-northeast-1c"],
    "sa-east-1": [
        "sa-east-1a",
        "sa-east-1b",
        "sa-east-1c"],
}


_REGION_KEYS = list(_REGIONS.keys())


_MACHINE_RACK_CHOICES = [
    str(n)
    for n in range(100)]


_MACHINE_OS_CHOICES = [
    "Ubuntu16.10",
    "Ubuntu16.04LTS",
    "Ubuntu15.10"]


_MACHINE_ARCH_CHOICES = [
    "x64",
    "x86"]


_MACHINE_TEAM_CHOICES = [
    "SF",
    "NYC",
    "LON",
    "CHI"]


_MACHINE_SERVICE_CHOICES = [
    str(n)
    for n in range(20)]


_MACHINE_SERVICE_VERSION_CHOICES = [
    str(n)
    for n in range(2)]


_MACHINE_SERVICE_ENVIRONMENT_CHOICES = [
    "production",
    "staging",
    "test"]


def gen_dataframe(seed, row_count, scale):
    rand, np_rand = random.Random(seed), np.random.default_rng(seed)

    def mk_symbols_series(strings):
        return pd.Series(strings, dtype='string[pyarrow]')

    def mk_hostname():
        repeated = [f'host_{n}' for n in range(scale)]
        repeat_count = row_count // scale + 1
        values = (repeated * repeat_count)[:row_count]
        return mk_symbols_series(values)

    def rep_choice(choices):
        return rand.choices(choices, k=row_count)

    def mk_cpu_series():
        values = np_rand.normal(0, 1, row_count + 1)
        _clip_add.accumulate(values, out=values)
        return pd.Series(values[1:], dtype='float64')

    region = []
    datacenter = []
    for _ in range(row_count):
        reg = random.choice(_REGION_KEYS)
        region.append(reg)
        datacenter.append(rand.choice(_REGIONS[reg]))

    df = pd.DataFrame({
        'hostname': mk_hostname(),
        'region': mk_symbols_series(region),
        'datacenter': mk_symbols_series(datacenter),
        'rack': mk_symbols_series(rep_choice(_MACHINE_RACK_CHOICES)),
        'os': mk_symbols_series(rep_choice(_MACHINE_OS_CHOICES)),
        'arch': mk_symbols_series(rep_choice(_MACHINE_ARCH_CHOICES)),
        'team': mk_symbols_series(rep_choice(_MACHINE_TEAM_CHOICES)),
        'service': mk_symbols_series(rep_choice(_MACHINE_SERVICE_CHOICES)),
        'service_version': mk_symbols_series(
            rep_choice(_MACHINE_SERVICE_VERSION_CHOICES)),
        'service_environment': mk_symbols_series(
            rep_choice(_MACHINE_SERVICE_ENVIRONMENT_CHOICES)),
        'usage_user': mk_cpu_series(),
        'usage_system': mk_cpu_series(),
        'usage_idle': mk_cpu_series(),
        'usage_nice': mk_cpu_series(),
        'usage_iowait': mk_cpu_series(),
        'usage_irq': mk_cpu_series(),
        'usage_softirq': mk_cpu_series(),
        'usage_steal': mk_cpu_series(),
        'usage_guest': mk_cpu_series(),
        'usage_guest_nice': mk_cpu_series(),
        'timestamp': pd.date_range('2016-01-01', periods=row_count, freq='10s'),
    })

    df.index.name = 'cpu'
    return df


def chunk_up_dataframe(df, chunk_row_count):
    dfs = []
    for i in range(0, len(df), chunk_row_count):
        dfs.append(df.iloc[i:i + chunk_row_count])
    return dfs


def assign_dfs_to_workers(dfs, workers):
    dfs_by_worker = [[] for _ in range(workers)]
    for i, df in enumerate(dfs):
        dfs_by_worker[i % workers].append(df)
    return dfs_by_worker


def sanity_check_split(df, dfs):
    df2 = pd.concat(dfs)
    assert len(df) == len(df2)
    assert df.equals(df2)


def sanity_check_split2(df, dfs_by_worker):
    df2 = pd.concat([
        df
        for dfs in dfs_by_worker
            for df in dfs])
    df2.sort_values(by='timestamp', inplace=True)
    assert len(df) == len(df2)
    assert df.equals(df2)


def chunk_up_by_worker(df, workers, chunk_row_count):
    dfs = chunk_up_dataframe(df, chunk_row_count)
    sanity_check_split(df, dfs)
    dfs_by_worker = assign_dfs_to_workers(dfs, workers)
    sanity_check_split2(df, dfs_by_worker)
    return dfs_by_worker


def dataframe(obj, df):
    obj.dataframe(df, symbols=True, at='timestamp')


def serialize_one(df, write_ilp, row_count):
    buf = qi.Buffer()
    op = dataframe
    t0 = time.monotonic()
    op(buf, df)    
    t1 = time.monotonic()
    elapsed = t1 - t0
    if write_ilp:
        if write_ilp == '-':
            print(buf)
        else:
            with open(write_ilp, 'w') as f:
                f.write(str(buf))
    row_speed = row_count / elapsed / 1_000_000.0
    print('Serialized:')
    print(
        f'  {row_count} rows in {elapsed:.2f}s: '
        f'{row_speed:.2f} mil rows/sec.')
    size_mb = len(buf) / 1024.0 / 1024.0
    throughput_mb = size_mb / elapsed
    print(
        f'  ILP Buffer size: {size_mb:.2f} MiB: '
        f'{throughput_mb:.2f} MiB/sec.')
    return len(buf)


def serialize_workers(df, row_count, workers, worker_chunk_row_count, debug):
    dfs_by_worker = chunk_up_by_worker(
        df, workers, worker_chunk_row_count)
    bufs = [qi.Buffer() for _ in range(workers)]
    tpe = ThreadPoolExecutor(max_workers=workers)

    # Warm up the thread pool.
    tpe.map(lambda e: None, [None] * workers)

    op = dataframe

    if debug:
        repld = [False]
        import threading
        lock = threading.Lock()

        def serialize_dfs(buf, dfs):
            size = 0
            for df in dfs:
                try:
                    op(buf, df)
                except Exception as e:
                    with lock:
                        if not repld[0]:
                            import code
                            code.interact(local=locals())
                            repld[0] = True
                    raise e
                size += len(buf)
                buf.clear()
            return size
    else:
        def serialize_dfs(buf, dfs):
            size = 0
            for df in dfs:
                op(buf, df)
                size += len(buf)
                buf.clear()
            return size

    t0 = time.monotonic()
    futures = [
        tpe.submit(serialize_dfs, buf, dfs)
        for buf, dfs in zip(bufs, dfs_by_worker)]
    sizes = [fut.result() for fut in futures]
    t1 = time.monotonic()
    size = sum(sizes)
    elapsed = t1 - t0
    row_speed = row_count / elapsed / 1_000_000.0
    print('Serialized:')
    print(
        f'  {row_count} rows in {elapsed:.2f}s: '
        f'{row_speed:.2f} mil rows/sec.')
    throughput_mb = size / elapsed / 1024.0 / 1024.0
    size_mb = size / 1024.0 / 1024.0
    print(
        f'  ILP Buffer size: {size_mb:.2f} MiB: '
        f'{throughput_mb:.2f} MiB/sec.')
    return size


def send_one(df, size, row_count, host, ilp_port):
    op = dataframe
    with qi.Sender.from_conf(f'tcp::addr={host}:{ilp_port};') as sender:
        t0 = time.monotonic()
        op(sender, df)
        sender.flush()
        t1 = time.monotonic()
    elapsed = t1 - t0
    row_speed = row_count / elapsed / 1_000_000.0
    print('Sent:')
    print(
        f'  {row_count} rows in {elapsed:.2f}s: '
        f'{row_speed:.2f} mil rows/sec.')
    throughput_mb = size / elapsed / 1024.0 / 1024.0
    size_mb = size / 1024.0 / 1024.0
    print(
        f'  ILP Buffer size: {size_mb:.2f} MiB: '
        f'{throughput_mb:.2f} MiB/sec.')


def send_workers(df, size, row_count, host, ilp_port, workers, worker_chunk_row_count):
    dfs_by_worker = chunk_up_by_worker(
        df, workers, worker_chunk_row_count)

    tpe = ThreadPoolExecutor(max_workers=workers)

    def connected_sender():
        sender = qi.Sender.from_conf(f'tcp::addr={host}:{ilp_port};')
        sender.establish()
        return sender

    senders = [
        tpe.submit(connected_sender)
        for _ in range(workers)]
    senders: list[qi.Sender] = [f.result() for f in senders]

    def worker_job(op, sender, worker_dfs):
        try:
            for df in worker_dfs:
                op(sender, df)
            sender.flush()
        finally:
            sender.close()

    op = dataframe

    t0 = time.monotonic()
    futures: list[Future] = [
        tpe.submit(worker_job, op, sender, dfs)
        for sender, dfs in zip(senders, dfs_by_worker)]
    for f in futures:
        f.result()
    t1 = time.monotonic()

    elapsed = t1 - t0
    row_speed = row_count / elapsed / 1_000_000.0
    print('Sent:')
    print(
        f'  {row_count} rows in {elapsed:.2f}s: '
        f'{row_speed:.2f} mil rows/sec.')
    throughput_mb = size / elapsed / 1024.0 / 1024.0
    size_mb = size / 1024.0 / 1024.0
    print(
        f'  ILP Buffer size: {size_mb:.2f} MiB: '
        f'{throughput_mb:.2f} MiB/sec.')


def load(
    row_count: int = typer.Option(10_000_000, help="Number of rows to load"),
    scale: int = typer.Option(4000, help="Scale factor"),
    seed: int = typer.Option(random.randrange(sys.maxsize), help="Random seed"),
    write_ilp: str = typer.Option(None, help="Write ILP output file"),
    shell: bool = typer.Option(False, help="Enable shell mode"),
    send: bool = typer.Option(False, help="Send data to server"),
    host: str = typer.Option("localhost", help="Host address"),
    ilp_port: int = typer.Option(9009, help="ILP port"),
    http_port: int = typer.Option(9000, help="HTTP port"),
    workers: int = typer.Option(None, help="Number of workers"),
    worker_chunk_row_count: int = typer.Option(10_000, help="Rows per worker chunk"),
    validation_query_timeout: float = typer.Option(120.0, help="Validation query timeout in seconds"),
    debug: bool = typer.Option(False, help="Enable debug mode")
):
    """Load test data into QuestDB."""
    cpu_table = CpuTable(host, http_port)

    if send:
        cpu_table.drop()
        cpu_table.create()

    df = gen_dataframe(seed, row_count, scale)

    if not workers:
        size = serialize_one(df, write_ilp, row_count)
    else:
        if workers < 1:
            raise ValueError('workers must be >= 1')
        size = serialize_workers(df, row_count, workers, worker_chunk_row_count, debug)

    if shell:
        import code
        code.interact(local=locals())

    if send:
        if not workers:
            send_one(df, size, row_count, host, ilp_port)
        else:
            send_workers(df, size, row_count, host, ilp_port, workers, worker_chunk_row_count)

        cpu_table.block_until_rowcount(
            row_count, timeout=validation_query_timeout)
    else:
        print('Not sending. Use --send to send to server.')

