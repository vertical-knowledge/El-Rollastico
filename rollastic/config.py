LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s| %(name)s/%(processName)s[%(process)d]-%(threadName)s: %(message)s @%(funcName)s:%(lineno)d #%(levelname)s',
        }
    },
    'handlers': {
        'console': {
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        # 'logfile': {
        #     'formatter': 'standard',
        #     'class': 'logging.FileHandler',
        #     'filename': 'rollastic.log',
        # },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',
    },
    'loggers': {
        'rollastic': dict(level='DEBUG'),

        # These are super noisy
        'elasticsearch': dict(level='WARNING'),
        'requests': dict(level='WARNING'),
        'urllib3': dict(level='WARNING'),
    }
}