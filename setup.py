from setuptools import setup

__version__ = '0.0.1'  # This is ovewritten by the execfile below
exec (open('rollastic/_version.py').read())

setup(
    name='rollastic',
    version=__version__,
    packages=['rollastic'],
    url='',
    license='GPL',
    author='VK',
    author_email='vk@vertical-knowledge.com',
    description='ElasticSearch cluster management via SaltStack -- rolling restarts/upgrades',
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
