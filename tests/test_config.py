import os

from bamboo.config import Config

HOSTS = ['host1', 'host2', 'host3']
ENV_HOSTS = ' '.join(HOSTS)  # "host1 host2 host3"
DUMMY_HOSTS = ['not_the_host']


def test_kwargs():
    config = Config(hosts=HOSTS)
    assert config['hosts'] == HOSTS


def test_call_with_kwargs():
    config = Config(hosts=DUMMY_HOSTS)
    config(hosts=HOSTS)
    assert config['hosts'] == HOSTS


def test_env_var(monkeypatch):
    monkeypatch.setenv('BAMBOO_HOSTS', ENV_HOSTS)
    config = Config()
    assert config['hosts'] == HOSTS


def test_kwargs_override_env(monkeypatch):
    monkeypatch.setenv('BAMBOO_HOSTS', DUMMY_HOSTS[0])
    config = Config(hosts=HOSTS)
    assert config['hosts'] == HOSTS
