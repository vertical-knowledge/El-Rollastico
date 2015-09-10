from setuptools import setup

setup(
    name='rollastic',
    version='0.0.1',
    modules=['rollastic'],
    url='',
    license='GPL',
    author='VK',
    author_email='vk@vertical-knowledge.com',
    description='ElasticSearch cluster management -- rolling restart/upgrade',
    entry_points={
        'console_scripts': [
            'rollastic = rollastic:cli',
        ]
    },

    install_requires=[
        'elasticsearch',
        'click',
    ],
    extras_require={
        #'salt': [
        #    'salt',
        #],
    },
)
