from setuptools import setup

__version__ = '0.0.1'  # This is ovewritten by the execfile below
exec (open('rollastic/_version.py').read())

conf = dict(
    name='rollastic',
    description='Automated ElasticSearch cluster rolling restarts/upgrades via SaltStack',
    url='http://github.com/vertical-knowledge/rollastic',
    author='VK',
    author_email='vk@vertical-knowledge.com',
    license='GPL',
    keywords=['elasticsearch', 'elastic', 'cluster', 'salt', 'saltstack', 'rolling', 'upgrade', 'restart'],
    classifiers=[],

    version=__version__,
    packages=['rollastic'],
    entry_points={
        'console_scripts': [
            'rollastic = rollastic.__main__:cli',
        ]
    },

    install_requires=[
        'elasticsearch',
        'click',
    ],
    extras_require={
        # 'salt': [
        #    'salt',
        # ],
    },
)

conf.update(dict(
    download_url='{}/tarball/{}'.format(conf['url'], conf['version']),
))

setup(**conf)
